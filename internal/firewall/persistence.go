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
)

const (
	helperPath      = "/Library/PrivilegedHelperTools/dev.apc.firewall"
	launchDaemonDir = "/Library/LaunchDaemons"
)

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
	return plistPath, nil
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
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
`)
	return []byte(output.String())
}

func installHelper(source string) error {
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
	command := exec.CommandContext(ctx, "/bin/launchctl", arguments...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		return output.Bytes(), commandError("configure PF LaunchDaemon", output.Bytes(), err)
	}
	return output.Bytes(), nil
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
