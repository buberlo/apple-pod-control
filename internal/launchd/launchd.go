package launchd

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var safeName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

type Config struct {
	Role       string
	Cluster    string
	Executable string
	Interval   time.Duration
}

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, []byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, binary string, arguments ...string) ([]byte, []byte, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

type Manager struct {
	runner                 commandRunner
	uid                    int
	euid                   int
	home                   string
	launchDaemonsDirectory string
	// launchDaemonsValidationRoot is always "/" in production. Tests may set
	// a narrower private sandbox root so validation does not depend on the
	// operating system's shared temporary-directory ancestors.
	launchDaemonsValidationRoot string
	lookupAccount               accountLookup
	lookupGroup                 groupLookup
	chown                       fileChown
	ownership                   ownershipLookup
	directoryOpenedHook         func(string)
}

func NewManager() (*Manager, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory: %w", err)
	}
	return &Manager{
		runner: execRunner{}, uid: os.Getuid(), euid: os.Geteuid(), home: home,
		launchDaemonsDirectory:      "/Library/LaunchDaemons",
		launchDaemonsValidationRoot: string(filepath.Separator),
		lookupAccount:               defaultAccountLookup, lookupGroup: defaultGroupLookup,
		chown: defaultFileChown, ownership: defaultOwnershipLookup,
	}, nil
}

func (m *Manager) Install(ctx context.Context, config Config) (string, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return "", err
	}
	plistPath := m.plistPath(config)
	logDirectory := filepath.Join(m.home, "Library", "Logs", "APC")
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o700); err != nil {
		return "", fmt.Errorf("create LaunchAgents directory: %w", err)
	}
	if err := os.MkdirAll(logDirectory, 0o700); err != nil {
		return "", fmt.Errorf("create APC log directory: %w", err)
	}
	if _, err := os.Lstat(plistPath); err == nil {
		_, _, _ = m.runner.Run(ctx, "launchctl", "bootout", m.serviceTarget(config))
		_, _, _ = m.runner.Run(ctx, "launchctl", "bootout", m.guiServiceTarget(config))
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect existing LaunchAgent: %w", err)
	}
	data := RenderPlist(config, filepath.Join(logDirectory, config.Role+"-"+config.Cluster+".log"))
	if err := writeAtomicFile(plistPath, data, 0o644); err != nil {
		return "", err
	}
	domain := "user/" + strconv.Itoa(m.uid)
	if _, stderr, err := m.runner.Run(ctx, "launchctl", "bootstrap", domain, plistPath); err != nil {
		return "", commandError("bootstrap LaunchAgent", stderr, err)
	}
	if _, stderr, err := m.runner.Run(ctx, "launchctl", "enable", m.serviceTarget(config)); err != nil {
		return "", commandError("enable LaunchAgent", stderr, err)
	}
	if _, stderr, err := m.runner.Run(ctx, "launchctl", "kickstart", "-k", m.serviceTarget(config)); err != nil {
		return "", commandError("start LaunchAgent", stderr, err)
	}
	return plistPath, nil
}

func (m *Manager) Uninstall(ctx context.Context, config Config) error {
	config, err := normalizeConfig(config)
	if err != nil {
		return err
	}
	_, stderr, bootoutErr := m.runner.Run(ctx, "launchctl", "bootout", m.serviceTarget(config))
	if bootoutErr != nil && !serviceNotFound(stderr) {
		return commandError("stop LaunchAgent", stderr, bootoutErr)
	}
	_, stderr, guiBootoutErr := m.runner.Run(ctx, "launchctl", "bootout", m.guiServiceTarget(config))
	if guiBootoutErr != nil && !serviceNotFound(stderr) {
		return commandError("stop GUI LaunchAgent", stderr, guiBootoutErr)
	}
	if err := os.Remove(m.plistPath(config)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove LaunchAgent plist: %w", err)
	}
	return nil
}

func (m *Manager) Status(ctx context.Context, config Config) ([]byte, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	stdout, stderr, err := m.runner.Run(ctx, "launchctl", "print", m.serviceTarget(config))
	if err != nil {
		return nil, commandError("read LaunchAgent status", stderr, err)
	}
	return stdout, nil
}

func RenderPlist(config Config, logPath string) []byte {
	label := Label(config)
	arguments := []string{config.Executable, "system", "supervise", "--role", config.Role, "--cluster", config.Cluster, "--interval", config.Interval.String()}
	var output strings.Builder
	output.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + escape(label) + `</string>
  <key>ProgramArguments</key>
  <array>
`)
	for _, argument := range arguments {
		output.WriteString("    <string>" + escape(argument) + "</string>\n")
	}
	output.WriteString(`  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>ProcessType</key>
  <string>Background</string>
  <key>LimitLoadToSessionType</key>
  <string>Background</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key>
  <string>` + escape(logPath) + `</string>
  <key>StandardErrorPath</key>
  <string>` + escape(logPath) + `</string>
</dict>
</plist>
`)
	return []byte(output.String())
}

func Label(config Config) string {
	return "dev.apc." + config.Role + "." + config.Cluster
}

func normalizeConfig(config Config) (Config, error) {
	if config.Role != "server" && config.Role != "agent" && config.Role != "ha" {
		return Config{}, fmt.Errorf("role must be server, agent, or ha")
	}
	if !safeName.MatchString(config.Cluster) {
		return Config{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if config.Executable == "" {
		return Config{}, fmt.Errorf("APC executable path is required")
	}
	absolute, err := filepath.Abs(config.Executable)
	if err != nil {
		return Config{}, fmt.Errorf("resolve APC executable: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return Config{}, fmt.Errorf("inspect APC executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return Config{}, fmt.Errorf("APC executable must be an executable regular file")
	}
	config.Executable = absolute
	if config.Interval == 0 {
		config.Interval = 15 * time.Second
	}
	if config.Interval < 5*time.Second {
		return Config{}, fmt.Errorf("supervisor interval must be at least 5s")
	}
	return config, nil
}

func (m *Manager) plistPath(config Config) string {
	return filepath.Join(m.home, "Library", "LaunchAgents", Label(config)+".plist")
}

func (m *Manager) serviceTarget(config Config) string {
	return "user/" + strconv.Itoa(m.uid) + "/" + Label(config)
}

func (m *Manager) guiServiceTarget(config Config) string {
	return "gui/" + strconv.Itoa(m.uid) + "/" + Label(config)
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".apc-launchagent-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary LaunchAgent plist: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write LaunchAgent plist: %w", err)
	}
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set LaunchAgent plist mode: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close LaunchAgent plist: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish LaunchAgent plist: %w", err)
	}
	return nil
}

func escape(value string) string {
	var output bytes.Buffer
	_ = xml.EscapeText(&output, []byte(value))
	return output.String()
}

func serviceNotFound(stderr []byte) bool {
	value := strings.ToLower(string(stderr))
	return strings.Contains(value, "could not find service") || strings.Contains(value, "no such process") || strings.Contains(value, "domain does not support specified action")
}

func commandError(operation string, stderr []byte, err error) error {
	detail := strings.TrimSpace(string(stderr))
	if detail == "" {
		detail = err.Error()
	}
	return fmt.Errorf("%s: %s", operation, detail)
}
