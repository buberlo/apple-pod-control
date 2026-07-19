package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type SuperviseOptions struct {
	Role     string
	Name     string
	Interval time.Duration
	Output   io.Writer
}

// Supervise continuously keeps the Apple container service and one APC node
// running. launchd restarts this loop if the process itself fails.
func (m *Manager) Supervise(ctx context.Context, options SuperviseOptions) error {
	if options.Role != "server" && options.Role != "agent" {
		return fmt.Errorf("role must be server or agent")
	}
	if !dnsLabel.MatchString(options.Name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if options.Interval == 0 {
		options.Interval = 15 * time.Second
	}
	if options.Interval < 5*time.Second {
		return fmt.Errorf("supervisor interval must be at least 5s")
	}
	if options.Output == nil {
		options.Output = io.Discard
	}
	if err := m.reconcileSupervisedNode(ctx, options); err != nil {
		fmt.Fprintf(options.Output, "%s reconcile failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
	}
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.reconcileSupervisedNode(ctx, options); err != nil {
				fmt.Fprintf(options.Output, "%s reconcile failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
			}
		}
	}
}

func (m *Manager) reconcileSupervisedNode(ctx context.Context, options SuperviseOptions) error {
	if _, stderr, err := m.runner.Run(ctx, m.binary, "system", "status"); err != nil {
		if _, startStderr, startErr := m.runner.Run(ctx, m.binary, "system", "start"); startErr != nil {
			return errors.Join(commandError("read Apple container service status", stderr, err), commandError("start Apple container service", startStderr, startErr))
		}
	}
	if options.Role == "agent" {
		state, err := m.AgentStatus(ctx, options.Name)
		if err == nil && strings.EqualFold(state.RuntimeState, "running") {
			return nil
		}
		_, startErr := m.StartAgent(ctx, options.Name, 45*time.Second)
		return startErr
	}
	state, err := m.Status(ctx, options.Name)
	if err == nil && strings.EqualFold(state.RuntimeState, "running") && state.NodeReady {
		return nil
	}
	_, startErr := m.Start(ctx, options.Name, 2*time.Minute)
	return startErr
}

// DeleteServer removes the local server VM envelope. When keepData is false,
// it also removes the APC-owned data volume and local server configuration.
func (m *Manager) DeleteServer(ctx context.Context, name string, keepData bool) error {
	if !dnsLabel.MatchString(name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	files, err := serverConfigurationFiles(name)
	if err != nil {
		return err
	}
	if err := m.deleteOwnedContainer(ctx, ContainerName(name), name, "server"); err != nil {
		return err
	}
	if keepData {
		return nil
	}
	if err := m.deleteOwnedVolume(ctx, ServerVolumeName(name), name, "server"); err != nil {
		return err
	}
	if err := removeExactFiles(files); err != nil {
		return err
	}
	if err := clearCurrentCluster(name); err != nil {
		return err
	}
	return removeEmptyClusterDirectory(name)
}

// DeleteAgent removes the local agent VM envelope. When keepData is false, it
// also removes the APC-owned data volume and the saved local agent config.
func (m *Manager) DeleteAgent(ctx context.Context, name string, keepData bool) error {
	if !dnsLabel.MatchString(name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if err := m.deleteOwnedContainer(ctx, AgentContainerName(name), name, "agent"); err != nil {
		return err
	}
	if keepData {
		return nil
	}
	if err := m.deleteOwnedVolume(ctx, AgentVolumeName(name), name, "agent"); err != nil {
		return err
	}
	path, err := agentConfigPath(name)
	if err != nil {
		return err
	}
	if err := removeExactFiles([]string{path}); err != nil {
		return err
	}
	return removeEmptyClusterDirectory(name)
}

func (m *Manager) deleteOwnedContainer(ctx context.Context, containerName, clusterName, role string) error {
	record, err := m.inspect(ctx, containerName)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateOwnedContainer(record, clusterName, role); err != nil {
		return err
	}
	if !strings.EqualFold(record.Status.State, "stopped") {
		if _, stderr, stopErr := m.runner.Run(ctx, m.binary, "stop", containerName); stopErr != nil {
			return commandError("stop APC "+role+" node before deletion", stderr, stopErr)
		}
	}
	if _, stderr, deleteErr := m.runner.Run(ctx, m.binary, "delete", containerName); deleteErr != nil {
		return commandError("delete APC "+role+" node", stderr, deleteErr)
	}
	return nil
}

func (m *Manager) inspectVolume(ctx context.Context, name string) (volumeRecord, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "volume", "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return volumeRecord{}, ErrNotFound
		}
		return volumeRecord{}, commandError("inspect K3s data volume", stderr, err)
	}
	var records []volumeRecord
	if err := json.Unmarshal(stdout, &records); err != nil {
		return volumeRecord{}, fmt.Errorf("decode volume inspect output: %w", err)
	}
	if len(records) != 1 {
		return volumeRecord{}, fmt.Errorf("volume inspect returned %d records", len(records))
	}
	return records[0], nil
}

func (m *Manager) deleteOwnedVolume(ctx context.Context, volumeName, clusterName, role string) error {
	record, err := m.inspectVolume(ctx, volumeName)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateOwnedVolume(record, volumeName, clusterName, role); err != nil {
		return err
	}
	if _, stderr, deleteErr := m.runner.Run(ctx, m.binary, "volume", "delete", volumeName); deleteErr != nil {
		return commandError("delete APC "+role+" data volume", stderr, deleteErr)
	}
	return nil
}

func validateOwnedVolume(record volumeRecord, volumeName, clusterName, role string) error {
	labels := record.Configuration.Labels
	if record.Configuration.Name != "" && record.Configuration.Name != volumeName {
		return fmt.Errorf("volume inspect returned unexpected volume %q", record.Configuration.Name)
	}
	if labels["apc.dev/managed"] != "true" || labels["apc.dev/cluster"] != clusterName || labels["apc.dev/role"] != role {
		return fmt.Errorf("volume %q exists but is not the expected APC %s volume", volumeName, role)
	}
	return nil
}

func serverConfigurationFiles(name string) ([]string, error) {
	configPath, err := clusterConfigPath(name)
	if err != nil {
		return nil, err
	}
	kubeconfigPath, err := KubeconfigPath(name)
	if err != nil {
		return nil, err
	}
	config, loadErr := loadClusterConfig(name)
	switch {
	case loadErr == nil:
		kubeconfigPath = config.KubeconfigPath
	case errors.Is(loadErr, os.ErrNotExist):
	default:
		return nil, loadErr
	}
	return []string{configPath, kubeconfigPath}, nil
}

func removeExactFiles(paths []string) error {
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		clean := filepath.Clean(path)
		if _, duplicate := seen[clean]; duplicate {
			continue
		}
		seen[clean] = struct{}{}
		if err := os.Remove(clean); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove APC configuration file %q: %w", clean, err)
		}
	}
	return nil
}

func clearCurrentCluster(name string) error {
	current, err := CurrentCluster()
	if err != nil {
		return nil
	}
	if current != name {
		return nil
	}
	path, err := currentClusterPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear current cluster: %w", err)
	}
	return nil
}

func removeEmptyClusterDirectory(name string) error {
	configPath, err := clusterConfigPath(name)
	if err != nil {
		return err
	}
	err = os.Remove(filepath.Dir(configPath))
	if err == nil || errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTEMPTY) {
		return nil
	}
	return fmt.Errorf("remove empty APC cluster directory: %w", err)
}
