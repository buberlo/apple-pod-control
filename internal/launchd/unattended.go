package launchd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	unattendedPlistMode      = 0o644
	rootUserID               = 0
	wheelGroupID             = 0
	unattendedLaunchctlPath  = "/bin/launchctl"
	unattendedSudoPath       = "/usr/bin/sudo"
	unattendedEnvPath        = "/usr/bin/env"
	unattendedNullDevice     = "/dev/null"
	unattendedChmodPath      = "/bin/chmod"
	unattendedLSPath         = "/bin/ls"
	unattendedSupervisorPath = "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
	unattendedBootoutTimeout = 5 * time.Second
	unattendedBootoutPoll    = 25 * time.Millisecond
)

var safeAccountName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9._-]{0,63}$`)

type accountRecord struct {
	username string
	uid      int
	gid      int
	home     string
}

type targetIdentity struct {
	accountRecord
	groupName string
}

type accountLookup func(string) (accountRecord, error)
type groupLookup func(int) (string, error)
type fileChown func(*os.File, int, int) error
type ownershipLookup func(string, os.FileInfo) (int, int, error)

type unattendedPlan struct {
	config    Config
	target    targetIdentity
	plistPath string
	logDir    string
	logPath   string
	plist     []byte
}

// InstallUnattended installs a root-owned LaunchDaemon that always executes
// APC as the explicitly selected non-root account.
func (m *Manager) InstallUnattended(ctx context.Context, config Config, targetUsername string) (string, error) {
	if err := m.requireRootMutation(); err != nil {
		return "", err
	}
	plan, err := m.unattendedPlan(config, targetUsername)
	if err != nil {
		return "", err
	}
	if err := m.refusePerUserSupervisor(ctx, plan); err != nil {
		return "", err
	}
	if err := m.validateExistingPlistOrAbsent(plan.plistPath, plan.plist); err != nil {
		return "", err
	}
	created, err := m.writeUnattendedPlist(plan.plistPath, plan.plist)
	if err != nil {
		return "", err
	}

	serviceTarget := systemServiceTarget(plan.config)
	_, loaded, err := m.systemServiceStatus(ctx, plan)
	if err != nil {
		return "", err
	}
	// launchd retains the submitted job definition independently of the plist.
	// If this install had to recreate an absent plist while the label was still
	// loaded, kickstart alone would restart the stale definition. Remove that
	// exact label first so bootstrap is guaranteed to consume the plist we just
	// validated and published.
	if created && loaded {
		if _, stderr, bootoutErr := m.runner.Run(ctx, unattendedLaunchctlPath, "bootout", serviceTarget); bootoutErr != nil && !serviceNotFound(stderr) {
			return "", commandError("replace stale unattended LaunchDaemon", stderr, bootoutErr)
		}
		if err := m.waitSystemServiceUnloaded(ctx, serviceTarget); err != nil {
			return "", err
		}
		loaded = false
	}
	if !loaded {
		if _, stderr, err := m.runner.Run(ctx, unattendedLaunchctlPath, "bootstrap", "system", plan.plistPath); err != nil {
			return "", commandError("bootstrap unattended LaunchDaemon", stderr, err)
		}
	}
	if _, stderr, err := m.runner.Run(ctx, unattendedLaunchctlPath, "enable", serviceTarget); err != nil {
		return "", commandError("enable unattended LaunchDaemon", stderr, err)
	}
	if _, stderr, err := m.runner.Run(ctx, unattendedLaunchctlPath, "kickstart", "-k", serviceTarget); err != nil {
		return "", commandError("start unattended LaunchDaemon", stderr, err)
	}
	if _, loaded, err := m.systemServiceStatus(ctx, plan); err != nil {
		return "", err
	} else if !loaded {
		return "", fmt.Errorf("unattended LaunchDaemon did not load in the system domain")
	}
	return plan.plistPath, nil
}

// StatusUnattended validates both the protected plist on disk and the loaded
// system-domain service. It is read-only and therefore does not require root.
func (m *Manager) StatusUnattended(ctx context.Context, config Config, targetUsername string) ([]byte, error) {
	plan, err := m.unattendedPlan(config, targetUsername)
	if err != nil {
		return nil, err
	}
	if err := m.refusePerUserSupervisor(ctx, plan); err != nil {
		return nil, err
	}
	if err := m.validateManagedPlist(plan.plistPath, plan.plist); err != nil {
		return nil, err
	}
	output, loaded, err := m.systemServiceStatus(ctx, plan)
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, fmt.Errorf("unattended LaunchDaemon is not loaded in the system domain")
	}
	return output, nil
}

// UninstallUnattended removes only an exact APC-owned LaunchDaemon. A caller
// must explicitly confirm the mutation; target-user logs and state are retained.
func (m *Manager) UninstallUnattended(ctx context.Context, config Config, targetUsername string, confirmed bool) error {
	if err := m.requireRootMutation(); err != nil {
		return err
	}
	if !confirmed {
		return fmt.Errorf("refusing unattended LaunchDaemon removal without explicit confirmation")
	}
	plan, err := m.unattendedPlan(config, targetUsername)
	if err != nil {
		return err
	}
	err = m.validateManagedPlist(plan.plistPath, plan.plist)
	if errors.Is(err, os.ErrNotExist) {
		_, loaded, statusErr := m.rawServiceStatus(ctx, systemServiceTarget(plan.config))
		if statusErr != nil {
			return statusErr
		}
		if loaded {
			return fmt.Errorf("refusing to remove a loaded system service without its exact owned plist")
		}
		return nil
	}
	if err != nil {
		return err
	}

	serviceTarget := systemServiceTarget(plan.config)
	if _, stderr, err := m.runner.Run(ctx, unattendedLaunchctlPath, "bootout", serviceTarget); err != nil && !serviceNotFound(stderr) {
		return commandError("stop unattended LaunchDaemon", stderr, err)
	}
	if err := m.waitSystemServiceUnloaded(ctx, serviceTarget); err != nil {
		return err
	}
	if err := os.Remove(plan.plistPath); err != nil {
		return fmt.Errorf("remove unattended LaunchDaemon plist: %w", err)
	}
	if _, loaded, err := m.rawServiceStatus(ctx, serviceTarget); err != nil {
		return err
	} else if loaded {
		return fmt.Errorf("unattended LaunchDaemon remained loaded after removal")
	}
	return nil
}

func (m *Manager) unattendedPlan(config Config, targetUsername string) (unattendedPlan, error) {
	config, err := normalizeUnattendedConfig(config)
	if err != nil {
		return unattendedPlan{}, err
	}
	target, err := m.resolveTargetIdentity(targetUsername)
	if err != nil {
		return unattendedPlan{}, err
	}
	if err := m.validateLaunchDaemonsDirectory(); err != nil {
		return unattendedPlan{}, err
	}
	logDirectory, logPath := unattendedUserLogPaths(target, config)
	plistPath := filepath.Join(m.launchDaemonsDirectory, Label(config)+".plist")
	if filepath.Dir(plistPath) != filepath.Clean(m.launchDaemonsDirectory) {
		return unattendedPlan{}, fmt.Errorf("unattended plist escaped the LaunchDaemons directory")
	}
	return unattendedPlan{
		config: config, target: target, plistPath: plistPath, logDir: logDirectory, logPath: logPath,
		plist: renderUnattendedPlist(config, target, logPath),
	}, nil
}

func unattendedUserLogPaths(target targetIdentity, config Config) (string, string) {
	logDirectory := filepath.Join(target.home, "Library", "Logs", "APC")
	return logDirectory, filepath.Join(logDirectory, config.Role+"-"+config.Cluster+"-unattended.log")
}

func normalizeUnattendedConfig(config Config) (Config, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return Config{}, err
	}
	info, err := inspectPathNoFollow(config.Executable)
	if err != nil {
		return Config{}, fmt.Errorf("inspect unattended APC executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return Config{}, fmt.Errorf("unattended APC executable must be an executable regular file")
	}
	return config, nil
}

func (m *Manager) resolveTargetIdentity(username string) (targetIdentity, error) {
	if !safeAccountName.MatchString(username) {
		return targetIdentity{}, fmt.Errorf("target username is invalid")
	}
	if m.lookupAccount == nil || m.lookupGroup == nil || m.ownership == nil {
		return targetIdentity{}, fmt.Errorf("unattended identity resolution is unavailable")
	}
	account, err := m.lookupAccount(username)
	if err != nil {
		return targetIdentity{}, fmt.Errorf("resolve target account: %w", err)
	}
	if account.username != username || strings.EqualFold(account.username, "root") || account.uid <= 0 || account.gid <= 0 {
		return targetIdentity{}, fmt.Errorf("target account must resolve exactly to a non-root user and group")
	}
	groupName, err := m.lookupGroup(account.gid)
	if err != nil {
		return targetIdentity{}, fmt.Errorf("resolve target group: %w", err)
	}
	if !safeAccountName.MatchString(groupName) {
		return targetIdentity{}, fmt.Errorf("target group name is invalid")
	}
	home := filepath.Clean(account.home)
	if !filepath.IsAbs(home) || home == string(filepath.Separator) || home != account.home || strings.ContainsAny(home, "\r\n") {
		return targetIdentity{}, fmt.Errorf("target home must be an exact absolute non-root path")
	}
	homeInfo, err := inspectPathNoFollow(home)
	if err != nil {
		return targetIdentity{}, fmt.Errorf("inspect target home: %w", err)
	}
	if !homeInfo.IsDir() || homeInfo.Mode().Perm()&0o022 != 0 {
		return targetIdentity{}, fmt.Errorf("target home must be a protected real directory")
	}
	uid, gid, err := m.ownership(home, homeInfo)
	if err != nil {
		return targetIdentity{}, fmt.Errorf("inspect target home ownership: %w", err)
	}
	if uid != account.uid || gid != account.gid {
		return targetIdentity{}, fmt.Errorf("target home ownership does not match the target account")
	}
	account.home = home
	return targetIdentity{accountRecord: account, groupName: groupName}, nil
}

func (m *Manager) validateLaunchDaemonsDirectory() error {
	directory := filepath.Clean(m.launchDaemonsDirectory)
	if !filepath.IsAbs(directory) || directory == string(filepath.Separator) || directory != m.launchDaemonsDirectory || strings.ContainsAny(directory, "\r\n") {
		return fmt.Errorf("LaunchDaemons directory must be an explicit absolute path")
	}
	validationRoot := filepath.Clean(m.launchDaemonsValidationRoot)
	if m.launchDaemonsValidationRoot == "" {
		validationRoot = string(filepath.Separator)
	}
	if !filepath.IsAbs(validationRoot) || strings.ContainsAny(validationRoot, "\r\n") {
		return fmt.Errorf("LaunchDaemons validation root must be an explicit absolute path")
	}
	relative, err := filepath.Rel(validationRoot, directory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("LaunchDaemons directory must remain beneath its protected validation root")
	}
	chain := absoluteDirectoryChain(directory)
	for len(chain) > 0 && chain[0] != validationRoot {
		chain = chain[1:]
	}
	if len(chain) == 0 {
		return fmt.Errorf("LaunchDaemons validation root is not an ancestor of its directory")
	}
	for _, ancestor := range chain {
		info, err := inspectPathNoFollow(ancestor)
		if err != nil {
			return fmt.Errorf("inspect LaunchDaemons directory chain at %s: %w", ancestor, err)
		}
		if !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("LaunchDaemons directory chain must contain only protected real directories")
		}
		uid, gid, err := m.ownership(ancestor, info)
		if err != nil {
			return fmt.Errorf("inspect LaunchDaemons directory chain ownership at %s: %w", ancestor, err)
		}
		if uid != rootUserID {
			return fmt.Errorf("LaunchDaemons directory chain must be owned by root")
		}
		if ancestor == directory {
			if gid != wheelGroupID {
				return fmt.Errorf("LaunchDaemons directory must be owned by root:wheel")
			}
			if err := validateNoExtendedACL(ancestor); err != nil {
				return fmt.Errorf("validate LaunchDaemons directory ACL: %w", err)
			}
			continue
		}
		if err := validateNoACLGrants(ancestor); err != nil {
			return fmt.Errorf("validate LaunchDaemons ancestor ACL at %s: %w", ancestor, err)
		}
	}
	return nil
}

func absoluteDirectoryChain(path string) []string {
	if path == string(filepath.Separator) {
		return []string{path}
	}
	components := strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator))
	chain := make([]string, 0, len(components)+1)
	current := string(filepath.Separator)
	chain = append(chain, current)
	for _, component := range components {
		current = filepath.Join(current, component)
		chain = append(chain, current)
	}
	return chain
}

func (m *Manager) validateExistingPlistOrAbsent(path string, expected []byte) error {
	err := m.validateManagedPlist(path, expected)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (m *Manager) validateManagedPlist(path string, expected []byte) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != unattendedPlistMode {
		return fmt.Errorf("unattended plist must be a regular non-symlink file with mode 0644")
	}
	uid, gid, err := m.ownership(path, info)
	if err != nil {
		return fmt.Errorf("inspect unattended plist ownership: %w", err)
	}
	if uid != rootUserID || gid != wheelGroupID {
		return fmt.Errorf("unattended plist must be owned by root:wheel")
	}
	if err := validateNoExtendedACL(path); err != nil {
		return fmt.Errorf("validate unattended plist ACL: %w", err)
	}
	data, err := readFileNoFollow(path, 64<<10)
	if err != nil {
		return fmt.Errorf("read unattended plist: %w", err)
	}
	if !bytes.Equal(data, expected) {
		return fmt.Errorf("refusing mismatched existing unattended plist")
	}
	return nil
}

func (m *Manager) writeUnattendedPlist(path string, data []byte) (bool, error) {
	if err := m.validateExistingPlistOrAbsent(path, data); err != nil {
		return false, err
	} else if _, err := os.Lstat(path); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect unattended plist: %w", err)
	}

	temporary, err := os.CreateTemp(filepath.Dir(path), ".apc-launchdaemon-*.tmp")
	if err != nil {
		return false, fmt.Errorf("create temporary unattended plist: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("write unattended plist: %w", err)
	}
	if err := removeExtendedACL(temporaryPath); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("remove inherited unattended plist ACL: %w", err)
	}
	if err := temporary.Chmod(unattendedPlistMode); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("protect unattended plist: %w", err)
	}
	if err := m.chown(temporary, rootUserID, wheelGroupID); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("own unattended plist: %w", err)
	}
	if err := validateNoExtendedACL(temporaryPath); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("validate temporary unattended plist ACL: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("sync unattended plist: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("close unattended plist: %w", err)
	}

	if err := os.Link(temporaryPath, path); errors.Is(err, os.ErrExist) {
		if validationErr := m.validateManagedPlist(path, data); validationErr != nil {
			return false, validationErr
		}
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("publish unattended plist: %w", err)
	}
	if err := m.securePublishedPlist(path); err != nil {
		_ = os.Remove(path)
		return false, err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return false, err
	}
	if err := m.validateManagedPlist(path, data); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Manager) securePublishedPlist(path string) error {
	if err := removeExtendedACL(path); err != nil {
		return fmt.Errorf("remove published unattended plist ACL: %w", err)
	}
	file, info, err := openVerifiedNoFollow(path)
	if err != nil {
		return fmt.Errorf("verify published unattended plist: %w", err)
	}
	defer file.Close()
	if !info.Mode().IsRegular() {
		return fmt.Errorf("published unattended plist is not a regular file")
	}
	if err := file.Chmod(unattendedPlistMode); err != nil {
		return fmt.Errorf("protect published unattended plist: %w", err)
	}
	if err := m.chown(file, rootUserID, wheelGroupID); err != nil {
		return fmt.Errorf("own published unattended plist: %w", err)
	}
	if err := validateNoExtendedACL(path); err != nil {
		return fmt.Errorf("validate published unattended plist ACL: %w", err)
	}
	return nil
}

func (m *Manager) rawServiceStatus(ctx context.Context, serviceTarget string) ([]byte, bool, error) {
	stdout, stderr, err := m.runner.Run(ctx, unattendedLaunchctlPath, "print", serviceTarget)
	if err == nil {
		return stdout, true, nil
	}
	if serviceNotFound(stderr) {
		return nil, false, nil
	}
	return nil, false, commandError("read launchd service status", stderr, err)
}

func (m *Manager) waitSystemServiceUnloaded(ctx context.Context, serviceTarget string) error {
	waitCtx, cancel := context.WithTimeout(ctx, unattendedBootoutTimeout)
	defer cancel()
	ticker := time.NewTicker(unattendedBootoutPoll)
	defer ticker.Stop()
	for {
		if _, loaded, err := m.rawServiceStatus(waitCtx, serviceTarget); err != nil {
			return err
		} else if !loaded {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return fmt.Errorf("wait for unattended LaunchDaemon to unload: %w", ctx.Err())
			}
			return fmt.Errorf("timed out waiting for unattended LaunchDaemon %s to unload", serviceTarget)
		case <-ticker.C:
		}
	}
}

func (m *Manager) systemServiceStatus(ctx context.Context, plan unattendedPlan) ([]byte, bool, error) {
	serviceTarget := systemServiceTarget(plan.config)
	output, loaded, err := m.rawServiceStatus(ctx, serviceTarget)
	if err != nil || !loaded {
		return output, loaded, err
	}
	arguments := unattendedProgramArguments(plan.config, plan.target, plan.logPath)
	if err := validateRunningLaunchdService(output, serviceTarget, arguments); err != nil {
		return nil, true, fmt.Errorf("unattended LaunchDaemon is loaded but unhealthy: %w", err)
	}
	return output, true, nil
}

type launchdPrintRecord struct {
	state     string
	program   string
	pid       int
	lastExit  string
	arguments []string
}

func validateRunningLaunchdService(output []byte, serviceTarget string, expectedArguments []string) error {
	record, err := parseLaunchdPrint(output, serviceTarget)
	if err != nil {
		return err
	}
	if record.state != "running" || record.pid <= 0 {
		return fmt.Errorf("launchd state must be running with a positive pid")
	}
	if len(expectedArguments) == 0 || record.program != expectedArguments[0] || len(record.arguments) != len(expectedArguments) {
		return fmt.Errorf("launchd program or wrapper arguments do not match the protected definition")
	}
	for index := range expectedArguments {
		if record.arguments[index] != expectedArguments[index] {
			return fmt.Errorf("launchd wrapper argument %d does not match the protected definition", index)
		}
	}
	return nil
}

// parseLaunchdPrint consumes only launchctl's observed top-level key/value and
// argument-block structure. `launchctl print` is not a stable API, so changes
// in macOS output intentionally fail closed instead of being guessed around.
func parseLaunchdPrint(output []byte, serviceTarget string) (launchdPrintRecord, error) {
	lines := strings.Split(string(output), "\n")
	record := launchdPrintRecord{pid: -1}
	header := serviceTarget + " = {"
	depth := 0
	seenHeader := false
	inArguments := false
	seen := make(map[string]bool)
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if !seenHeader {
			if line != header {
				return launchdPrintRecord{}, fmt.Errorf("launchctl output does not identify %s", serviceTarget)
			}
			seenHeader = true
			depth = 1
			continue
		}
		if inArguments {
			if line == "}" {
				inArguments = false
				depth--
				continue
			}
			record.arguments = append(record.arguments, line)
			continue
		}
		if strings.HasSuffix(line, " = {") {
			key := strings.TrimSuffix(line, " = {")
			if depth == 1 && key == "arguments" {
				if seen[key] {
					return launchdPrintRecord{}, fmt.Errorf("launchctl output contains duplicate arguments")
				}
				seen[key] = true
				inArguments = true
			}
			depth++
			continue
		}
		if line == "}" {
			depth--
			continue
		}
		if depth != 1 {
			continue
		}
		key, value, found := strings.Cut(line, " = ")
		if !found || (key != "state" && key != "program" && key != "pid" && key != "last exit code") {
			continue
		}
		if seen[key] {
			return launchdPrintRecord{}, fmt.Errorf("launchctl output contains duplicate %s", key)
		}
		seen[key] = true
		switch key {
		case "state":
			record.state = value
		case "program":
			record.program = value
		case "pid":
			pid, err := strconv.Atoi(value)
			if err != nil {
				return launchdPrintRecord{}, fmt.Errorf("launchctl pid is invalid")
			}
			record.pid = pid
		case "last exit code":
			record.lastExit = value
		}
	}
	if !seenHeader || depth != 0 || inArguments || !seen["state"] || !seen["program"] || !seen["pid"] || !seen["arguments"] {
		return launchdPrintRecord{}, fmt.Errorf("launchctl output is incomplete or malformed")
	}
	return record, nil
}

func (m *Manager) refusePerUserSupervisor(ctx context.Context, plan unattendedPlan) error {
	perUserPlist := filepath.Join(plan.target.home, "Library", "LaunchAgents", Label(plan.config)+".plist")
	if _, err := os.Lstat(perUserPlist); err == nil {
		return perUserConflictError("per-user LaunchAgent plist exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect per-user LaunchAgent conflict: %w", err)
	}

	for _, serviceTarget := range []string{
		"user/" + strconv.Itoa(plan.target.uid) + "/" + Label(plan.config),
		"gui/" + strconv.Itoa(plan.target.uid) + "/" + Label(plan.config),
	} {
		if _, loaded, err := m.rawServiceStatus(ctx, serviceTarget); err != nil {
			return err
		} else if loaded {
			return perUserConflictError("per-user supervisor is loaded in " + strings.SplitN(serviceTarget, "/", 2)[0] + " domain")
		}
	}
	return nil
}

func perUserConflictError(detail string) error {
	return fmt.Errorf("%s; run the existing per-user uninstall first before using unattended mode", detail)
}

func (m *Manager) requireRootMutation() error {
	if m.euid != rootUserID {
		return fmt.Errorf("unattended LaunchDaemon mutations require effective uid 0")
	}
	return nil
}

func systemServiceTarget(config Config) string {
	return "system/" + Label(config)
}

func renderUnattendedPlist(config Config, target targetIdentity, logPath string) []byte {
	arguments := unattendedProgramArguments(config, target, logPath)
	var output strings.Builder
	output.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + escape(Label(config)) + `</string>
  <key>ProgramArguments</key>
  <array>
`)
	for _, argument := range arguments {
		output.WriteString("    <string>" + escape(argument) + "</string>\n")
	}
	output.WriteString(`  </array>
  <key>WorkingDirectory</key>
  <string>` + escape(target.home) + `</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>` + escape(target.home) + `</string>
    <key>PATH</key>
    <string>` + unattendedSupervisorPath + `</string>
  </dict>
  <key>SessionCreate</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>ExitTimeOut</key>
  <integer>30</integer>
  <key>ProcessType</key>
  <string>Background</string>
  <key>Umask</key>
  <integer>63</integer>
  <key>StandardOutPath</key>
  <string>` + unattendedNullDevice + `</string>
  <key>StandardErrorPath</key>
  <string>` + unattendedNullDevice + `</string>
</dict>
</plist>
`)
	return []byte(output.String())
}

func unattendedProgramArguments(config Config, target targetIdentity, logPath string) []string {
	return []string{
		unattendedLaunchctlPath,
		"asuser", strconv.Itoa(target.uid),
		unattendedSudoPath,
		"-n", "-H", "-u", target.username, "--",
		unattendedEnvPath,
		"-i",
		"HOME=" + target.home,
		"PATH=" + unattendedSupervisorPath,
		config.Executable,
		"system", "supervise",
		"--role", config.Role,
		"--cluster", config.Cluster,
		"--interval", config.Interval.String(),
		"--log-file", logPath,
	}
}

func inspectPathNoFollow(path string) (os.FileInfo, error) {
	file, info, err := openVerifiedNoFollow(path)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return info, nil
}

func openVerifiedNoFollow(path string) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return nil, nil, fmt.Errorf("path must not be a symbolic link")
	}
	fileDescriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fileDescriptor), path)
	if file == nil {
		_ = syscall.Close(fileDescriptor)
		return nil, nil, fmt.Errorf("open path without following links")
	}
	after, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !os.SameFile(before, after) {
		_ = file.Close()
		return nil, nil, fmt.Errorf("path changed while it was being opened")
	}
	return file, after, nil
}

func readFileNoFollow(path string, maximum int64) ([]byte, error) {
	file, _, err := openVerifiedNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, fmt.Errorf("file exceeds size limit")
	}
	return data, nil
}

func syncDirectory(path string) error {
	directory, info, err := openVerifiedNoFollow(path)
	if err != nil {
		return fmt.Errorf("open LaunchDaemons directory for sync: %w", err)
	}
	defer directory.Close()
	if !info.IsDir() {
		return fmt.Errorf("LaunchDaemons path is not a directory")
	}
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync LaunchDaemons directory: %w", err)
	}
	return nil
}

func removeExtendedACL(path string) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	output, err := exec.Command(unattendedChmodPath, "-N", path).CombinedOutput()
	if err != nil {
		return commandError("remove extended ACL", bytes.TrimSpace(output), err)
	}
	return validateNoExtendedACL(path)
}

func validateNoExtendedACL(path string) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	output, err := exec.Command(unattendedLSPath, "-lde", path).CombinedOutput()
	if err != nil {
		return commandError("inspect extended ACL", bytes.TrimSpace(output), err)
	}
	firstLine, aclEntries, _ := bytes.Cut(output, []byte{'\n'})
	fields := bytes.Fields(firstLine)
	if len(fields) == 0 {
		return fmt.Errorf("inspect extended ACL: ls returned malformed output")
	}
	if bytes.ContainsRune(fields[0], '+') || len(bytes.TrimSpace(aclEntries)) != 0 {
		return fmt.Errorf("path must not have an extended ACL")
	}
	return nil
}

func validateNoACLGrants(path string) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	output, err := exec.Command(unattendedLSPath, "-lde", path).CombinedOutput()
	if err != nil {
		return commandError("inspect extended ACL grants", bytes.TrimSpace(output), err)
	}
	lines := bytes.Split(output, []byte{'\n'})
	if len(lines) == 0 || len(bytes.Fields(lines[0])) == 0 {
		return fmt.Errorf("inspect extended ACL grants: ls returned malformed output")
	}
	for _, rawLine := range lines[1:] {
		line := strings.TrimSpace(string(rawLine))
		if line == "" {
			continue
		}
		entry, _, found := strings.Cut(line, ":")
		if !found {
			return fmt.Errorf("inspect extended ACL grants: ls returned a malformed ACL entry")
		}
		if _, err := strconv.Atoi(entry); err != nil {
			return fmt.Errorf("inspect extended ACL grants: ls returned a malformed ACL index")
		}
		switch {
		case strings.Contains(line, " allow "):
			return fmt.Errorf("path must not have an extended ACL grant")
		case strings.Contains(line, " deny "):
			// macOS commonly protects HOME ancestors with deny-only entries.
		default:
			return fmt.Errorf("inspect extended ACL grants: ls returned an unknown ACL entry")
		}
	}
	return nil
}

func defaultAccountLookup(username string) (accountRecord, error) {
	resolved, err := user.Lookup(username)
	if err != nil {
		return accountRecord{}, err
	}
	uid, err := strconv.Atoi(resolved.Uid)
	if err != nil {
		return accountRecord{}, fmt.Errorf("parse account uid: %w", err)
	}
	gid, err := strconv.Atoi(resolved.Gid)
	if err != nil {
		return accountRecord{}, fmt.Errorf("parse account gid: %w", err)
	}
	return accountRecord{username: resolved.Username, uid: uid, gid: gid, home: resolved.HomeDir}, nil
}

func defaultGroupLookup(gid int) (string, error) {
	resolved, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		return "", err
	}
	return resolved.Name, nil
}

func defaultFileChown(file *os.File, uid, gid int) error {
	return file.Chown(uid, gid)
}

func defaultOwnershipLookup(_ string, info os.FileInfo) (int, int, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("file ownership metadata is unavailable")
	}
	return int(stat.Uid), int(stat.Gid), nil
}
