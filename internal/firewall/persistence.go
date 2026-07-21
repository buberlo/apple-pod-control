package firewall

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	helperPath      = "/Library/PrivilegedHelperTools/dev.apc.firewall"
	launchDaemonDir = "/Library/LaunchDaemons"
)

type InstallationStatus struct {
	Cluster      string `json:"cluster" yaml:"cluster"`
	Anchor       string `json:"anchor" yaml:"anchor"`
	DaemonLabel  string `json:"daemonLabel" yaml:"daemonLabel"`
	PlistPath    string `json:"plistPath" yaml:"plistPath"`
	HelperPath   string `json:"helperPath" yaml:"helperPath"`
	RuleCount    int    `json:"ruleCount" yaml:"ruleCount"`
	ReferenceSet bool   `json:"referenceSet" yaml:"referenceSet"`
}

func Install(ctx context.Context, config Config, executable string) (string, error) {
	config, err := normalize(config)
	if err != nil {
		return "", err
	}
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("installing the PF LaunchDaemon requires root; rerun this exact command with sudo")
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil {
		return "", fmt.Errorf("resolve APC executable: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect APC executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("APC executable must be an executable regular file")
	}
	if err := installHelper(resolved); err != nil {
		return "", err
	}
	plistPath := daemonPath(config.Cluster)
	previous, readErr := os.ReadFile(plistPath)
	hadPrevious := readErr == nil
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return "", fmt.Errorf("read existing PF LaunchDaemon: %w", readErr)
	}
	if err := writeAtomic(plistPath, RenderLaunchDaemon(config), 0o644); err != nil {
		return "", fmt.Errorf("install PF LaunchDaemon: %w", err)
	}
	rollback := func(cause error) error {
		_, _ = runLaunchctl(ctx, "bootout", serviceTarget(config.Cluster))
		var rollbackErr error
		if hadPrevious {
			if err := writeAtomic(plistPath, previous, 0o644); err != nil {
				rollbackErr = err
			} else if _, err := runLaunchctl(ctx, "bootstrap", "system", plistPath); err != nil {
				rollbackErr = err
			} else if _, err := runLaunchctl(ctx, "kickstart", "-k", serviceTarget(config.Cluster)); err != nil {
				rollbackErr = err
			}
		} else if err := os.Remove(plistPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			rollbackErr = err
		}
		if rollbackErr != nil {
			return errors.Join(cause, fmt.Errorf("restore previous PF LaunchDaemon: %w", rollbackErr))
		}
		return cause
	}
	if _, err := runLaunchctl(ctx, "bootout", serviceTarget(config.Cluster)); err != nil && !serviceNotFound(err) {
		return "", rollback(err)
	}
	if _, err := runLaunchctl(ctx, "bootstrap", "system", plistPath); err != nil {
		return "", rollback(err)
	}
	if _, err := runLaunchctl(ctx, "enable", serviceTarget(config.Cluster)); err != nil {
		return "", rollback(err)
	}
	if _, err := runLaunchctl(ctx, "kickstart", "-k", serviceTarget(config.Cluster)); err != nil {
		return "", rollback(err)
	}
	if err := Apply(ctx, config); err != nil {
		return "", rollback(err)
	}
	if _, err := Verify(ctx, config.Cluster); err != nil {
		_ = Remove(ctx, config.Cluster)
		return "", rollback(err)
	}
	return plistPath, nil
}

func Verify(ctx context.Context, cluster string) (InstallationStatus, error) {
	status := InstallationStatus{
		Cluster: cluster, Anchor: anchorName(cluster), DaemonLabel: label(cluster),
		PlistPath: daemonPath(cluster), HelperPath: helperPath,
	}
	if !safeCluster.MatchString(cluster) {
		return status, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if os.Geteuid() != 0 {
		return status, fmt.Errorf("verifying the PF installation requires root; rerun this exact command with sudo")
	}
	if err := verifyRootFile(helperPath, 0o755); err != nil {
		return status, fmt.Errorf("verify privileged helper: %w", err)
	}
	if err := verifyRootFile(status.PlistPath, 0o644); err != nil {
		return status, fmt.Errorf("verify PF LaunchDaemon: %w", err)
	}
	plist, err := os.ReadFile(status.PlistPath)
	if err != nil {
		return status, fmt.Errorf("read PF LaunchDaemon: %w", err)
	}
	for _, required := range []string{helperPath, label(cluster), "<string>firewall</string>", "<string>apply</string>", "<string>--yes</string>"} {
		if !bytes.Contains(plist, []byte(required)) {
			return status, fmt.Errorf("PF LaunchDaemon is missing required value %q", required)
		}
	}
	if _, err := runCommand(ctx, "/usr/bin/plutil", "validate PF LaunchDaemon", "-lint", status.PlistPath); err != nil {
		return status, err
	}
	if _, err := runLaunchctl(ctx, "print", serviceTarget(cluster)); err != nil {
		return status, err
	}
	rules, err := runCommand(ctx, "/sbin/pfctl", "read APC PF anchor", "-a", status.Anchor, "-sr")
	if err != nil {
		return status, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(rules)), "\n") {
		if strings.TrimSpace(line) != "" {
			status.RuleCount++
		}
	}
	if status.RuleCount < 4 || !bytes.Contains(rules, []byte("pass in quick")) || !bytes.Contains(rules, []byte("block")) {
		return status, fmt.Errorf("PF anchor %q does not contain APC's complete allow/block rules", status.Anchor)
	}
	if err := verifyRootFile(tokenPath(cluster), 0o600); err != nil {
		return status, fmt.Errorf("verify PF reference token: %w", err)
	}
	token, err := os.ReadFile(tokenPath(cluster))
	if err != nil {
		return status, fmt.Errorf("read PF reference token: %w", err)
	}
	if !safeToken(string(token)) {
		return status, fmt.Errorf("PF reference token is invalid")
	}
	references, err := runCommand(ctx, "/sbin/pfctl", "read PF references", "-s", "References")
	if err != nil {
		return status, err
	}
	if !referenceContains(string(references), strings.TrimSpace(string(token))) {
		return status, fmt.Errorf("PF reference token is not held by the running packet filter")
	}
	status.ReferenceSet = true
	return status, nil
}

func Uninstall(ctx context.Context, cluster string) error {
	if !safeCluster.MatchString(cluster) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("uninstalling the PF LaunchDaemon requires root; rerun this exact command with sudo")
	}
	if _, err := runLaunchctl(ctx, "bootout", serviceTarget(cluster)); err != nil && !serviceNotFound(err) {
		return err
	}
	plistPath := daemonPath(cluster)
	if err := Remove(ctx, cluster); err != nil {
		if _, restoreErr := os.Stat(plistPath); restoreErr == nil {
			_, _ = runLaunchctl(ctx, "bootstrap", "system", plistPath)
			_, _ = runLaunchctl(ctx, "kickstart", "-k", serviceTarget(cluster))
		}
		return err
	}
	if err := os.Remove(plistPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove PF LaunchDaemon: %w", err)
	}
	return nil
}

func RenderLaunchDaemon(config Config) []byte {
	arguments := []string{
		helperPath, "--cluster", config.Cluster, "system", "firewall", "apply",
		"--role", config.Role, "--interface", config.Interface, "--local-ip", config.LocalIP,
		"--api-port", fmt.Sprint(config.APIPort), "--vxlan-port", fmt.Sprint(config.VXLANPort),
		"--kubelet-port", fmt.Sprint(config.KubeletPort), "--yes",
	}
	for _, peer := range config.Peers {
		arguments = append(arguments, "--peer", peer)
	}
	var output strings.Builder
	output.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + xmlEscape(label(config.Cluster)) + `</string>
  <key>ProgramArguments</key>
  <array>
`)
	for _, argument := range arguments {
		output.WriteString("    <string>" + xmlEscape(argument) + "</string>\n")
	}
	output.WriteString(`  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>30</integer>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
`)
	return []byte(output.String())
}

func installHelper(source string) error {
	if err := ensurePrivilegedHelperDirectory(); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open APC executable: %w", err)
	}
	defer input.Close()
	temporary, err := os.CreateTemp(filepath.Dir(helperPath), ".apc-firewall-*.tmp")
	if err != nil {
		return fmt.Errorf("create privileged helper: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := io.Copy(temporary, input); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("copy privileged helper: %w", err)
	}
	if err := temporary.Chmod(0o755); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect privileged helper: %w", err)
	}
	if err := temporary.Chown(0, 0); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("own privileged helper: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync privileged helper: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close privileged helper: %w", err)
	}
	if err := os.Rename(temporaryPath, helperPath); err != nil {
		return fmt.Errorf("publish privileged helper: %w", err)
	}
	return nil
}

func ensurePrivilegedHelperDirectory() error {
	directory := filepath.Dir(helperPath)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(directory, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create privileged helper directory: %w", err)
		}
		if err := os.Chown(directory, 0, 0); err != nil {
			return fmt.Errorf("own privileged helper directory: %w", err)
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			return fmt.Errorf("protect privileged helper directory: %w", err)
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return fmt.Errorf("inspect privileged helper directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("privileged helper directory %s is not a real directory", directory)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Gid != 0 {
		return fmt.Errorf("privileged helper directory %s must be owned by root:wheel", directory)
	}
	if info.Mode().Perm() != 0o755 {
		return fmt.Errorf("privileged helper directory %s has mode %04o, expected 0755", directory, info.Mode().Perm())
	}
	return nil
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".apc-firewall-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chown(0, 0); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func runLaunchctl(ctx context.Context, arguments ...string) ([]byte, error) {
	return runCommand(ctx, "/bin/launchctl", "configure PF LaunchDaemon", arguments...)
}

func runCommand(ctx context.Context, binary, operation string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		return output.Bytes(), commandError(operation, output.Bytes(), err)
	}
	return output.Bytes(), nil
}

func verifyRootFile(path string, expectedMode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	if info.Mode().Perm() != expectedMode {
		return fmt.Errorf("%s has mode %04o, expected %04o", path, info.Mode().Perm(), expectedMode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Gid != 0 {
		return fmt.Errorf("%s must be owned by root:wheel", path)
	}
	return nil
}

func daemonPath(cluster string) string {
	return filepath.Join(launchDaemonDir, label(cluster)+".plist")
}

func label(cluster string) string {
	return "dev.apc.firewall." + cluster
}

func serviceTarget(cluster string) string {
	return "system/" + label(cluster)
}

func serviceNotFound(err error) bool {
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "could not find service") || strings.Contains(value, "no such process") || strings.Contains(value, "service cannot load in requested session")
}

func xmlEscape(value string) string {
	var output bytes.Buffer
	_ = xml.EscapeText(&output, []byte(value))
	return output.String()
}
