package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var immutableImageReference = regexp.MustCompile(`^[^\s]+@sha256:[0-9a-fA-F]{64}$`)

type UpgradeResult struct {
	State      State
	BackupPath string
	FromImage  string
	ToImage    string
	Changed    bool
}

// UpgradeServer replaces the server VM image while retaining its volume. A
// full backup is mandatory and is automatically restored if the new node does
// not become Ready.
func (m *Manager) UpgradeServer(ctx context.Context, name, image, backupPath string) (UpgradeResult, error) {
	if !dnsLabel.MatchString(name) {
		return UpgradeResult{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if !immutableImageReference.MatchString(image) {
		return UpgradeResult{}, fmt.Errorf("upgrade image must be an immutable OCI sha256 digest reference")
	}
	config, err := loadClusterConfig(name)
	if err != nil {
		return UpgradeResult{}, err
	}
	result := UpgradeResult{FromImage: config.Image, ToImage: image}
	if config.Image == image {
		result.State, err = m.Status(ctx, name)
		return result, err
	}
	if backupPath == "" {
		backupPath, err = DefaultUpgradeBackupPath(name)
		if err != nil {
			return UpgradeResult{}, err
		}
	}
	backup, err := m.BackupServer(ctx, name, backupPath)
	if err != nil {
		return UpgradeResult{}, fmt.Errorf("create pre-upgrade backup: %w", err)
	}
	result.BackupPath = backup.Path
	if err := m.DeleteServer(ctx, name, true); err != nil {
		return result, fmt.Errorf("remove old server envelope: %w", err)
	}
	config.Image = image
	state, upgradeErr := m.Create(ctx, config)
	if upgradeErr == nil {
		result.State = state
		result.Changed = true
		return result, nil
	}
	_, restoreErr := m.RestoreServer(ctx, name, backup.Path)
	if restoreErr != nil {
		return result, errors.Join(fmt.Errorf("start upgraded server: %w", upgradeErr), fmt.Errorf("automatic rollback failed: %w", restoreErr))
	}
	return result, fmt.Errorf("start upgraded server: %w; previous server data was restored from %s", upgradeErr, backup.Path)
}

func DefaultUpgradeBackupPath(name string) (string, error) {
	if !dnsLabel.MatchString(name) {
		return "", fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	return filepath.Join(configDirectory, "apc", "backups", name, timestamp+".apcbackup"), nil
}
