package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ImageImportTargets returns the local K3s VM envelopes that may receive an
// image archive. A protected HA configuration is authoritative: every declared
// network, volume and server envelope must exist, match its exact APC identity
// and be running before any target is returned. Legacy clusters retain their
// single-server target resolution.
func (m *Manager) ImageImportTargets(ctx context.Context, name string) ([]string, error) {
	config, err := loadHAConfig(name)
	if errors.Is(err, os.ErrNotExist) {
		return []string{ContainerName(name)}, nil
	}
	if err != nil {
		return nil, err
	}

	preflight, err := m.preflightHA(ctx, config, false)
	if err != nil {
		return nil, fmt.Errorf("validate HA image import targets: %w", err)
	}
	if !preflight.networkExists {
		return nil, fmt.Errorf("HA cluster %q image import requires its exact APC-owned network %q", config.Name, config.NetworkName)
	}
	if len(preflight.volumeExists) != len(config.Members) {
		return nil, fmt.Errorf("HA cluster %q image import requires all %d exact APC-owned member volumes; found %d", config.Name, len(config.Members), len(preflight.volumeExists))
	}
	if len(preflight.containerRecord) != len(config.Members) {
		return nil, fmt.Errorf("HA cluster %q image import requires all %d exact APC-owned server envelopes; found %d", config.Name, len(config.Members), len(preflight.containerRecord))
	}

	targets := make([]string, 0, len(config.Members))
	for _, member := range config.Members {
		containerName := HAContainerName(config.Name, member.ID)
		record, exists := preflight.containerRecord[member.ID]
		if !exists {
			return nil, fmt.Errorf("HA member %d container %q was not validated", member.ID, containerName)
		}
		if !strings.EqualFold(record.Status.State, "running") {
			return nil, fmt.Errorf("HA member %d container %q is %s; all HA image import targets must be running", member.ID, containerName, record.Status.State)
		}
		targets = append(targets, containerName)
	}
	return targets, nil
}
