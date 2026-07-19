package cluster

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"time"
)

const (
	backupAPIVersion = "apc.dev/v1alpha1"
	backupKind       = "ClusterBackup"
	backupDataFile   = "volume.tar"
	backupImage      = "docker.io/library/busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
)

type BackupManifest struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Cluster    string    `json:"cluster"`
	CreatedAt  time.Time `json:"createdAt"`
	DataFile   string    `json:"dataFile"`
	DataSHA256 string    `json:"dataSHA256"`
	Config     Config    `json:"config"`
}

type BackupResult struct {
	Path       string
	Bytes      int64
	DataSHA256 string
	CreatedAt  time.Time
}

func BackupContainerName(clusterName string) string {
	return "apc-k3s-" + clusterName + "-backup"
}

// BackupServer writes a consistent offline copy of the APC server volume.
// A running server is stopped for the copy and returned to Ready afterwards.
func (m *Manager) BackupServer(ctx context.Context, name, output string) (result BackupResult, err error) {
	if !dnsLabel.MatchString(name) {
		return BackupResult{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if strings.TrimSpace(output) == "" {
		return BackupResult{}, fmt.Errorf("backup output directory is required")
	}
	config, err := loadClusterConfig(name)
	if err != nil {
		return BackupResult{}, err
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return BackupResult{}, fmt.Errorf("resolve backup output path: %w", err)
	}
	if _, statErr := os.Lstat(output); statErr == nil {
		return BackupResult{}, fmt.Errorf("backup output %q already exists", output)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return BackupResult{}, fmt.Errorf("inspect backup output: %w", statErr)
	}

	containerRecord, inspectErr := m.inspect(ctx, ContainerName(name))
	if inspectErr != nil && !errors.Is(inspectErr, ErrNotFound) {
		return BackupResult{}, inspectErr
	}
	wasRunning := false
	if inspectErr == nil {
		if err := validateOwnedContainer(containerRecord, name, "server"); err != nil {
			return BackupResult{}, err
		}
		wasRunning = !strings.EqualFold(containerRecord.Status.State, "stopped")
		if wasRunning {
			if err := m.Stop(ctx, name); err != nil {
				return BackupResult{}, err
			}
		}
	}
	defer func() {
		if wasRunning {
			_, startErr := m.Start(ctx, name, config.StartupTimeout)
			if startErr != nil {
				startErr = fmt.Errorf("restart cluster after backup attempt at %s: %w", output, startErr)
			}
			err = errors.Join(err, startErr)
		}
	}()

	volumeRecord, err := m.inspectVolume(ctx, ServerVolumeName(name))
	if err != nil {
		return BackupResult{}, err
	}
	if err := validateOwnedVolume(volumeRecord, ServerVolumeName(name), name, "server"); err != nil {
		return BackupResult{}, err
	}
	if err := m.startBackupHelper(ctx, name); err != nil {
		return BackupResult{}, err
	}
	defer func() {
		err = errors.Join(err, m.deleteOwnedContainer(ctx, BackupContainerName(name), name, "backup"))
	}()

	parent := filepath.Dir(output)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return BackupResult{}, fmt.Errorf("create backup parent directory: %w", err)
	}
	temporary, err := os.MkdirTemp(parent, "."+filepath.Base(output)+".partial-")
	if err != nil {
		return BackupResult{}, fmt.Errorf("create temporary backup directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		return BackupResult{}, fmt.Errorf("secure temporary backup directory: %w", err)
	}
	defer os.RemoveAll(temporary)

	archivePath := filepath.Join(temporary, backupDataFile)
	archive, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return BackupResult{}, fmt.Errorf("create backup data archive: %w", err)
	}
	hash := sha256.New()
	counter := &countingWriter{writer: io.MultiWriter(archive, hash)}
	var stderr bytes.Buffer
	streamErr := m.stream.RunIO(ctx, m.binary, []string{"exec", BackupContainerName(name), "tar", "-C", "/data", "-cf", "-", "."}, nil, counter, &stderr)
	closeErr := archive.Close()
	if streamErr != nil {
		return BackupResult{}, commandError("archive APC server data", stderr.Bytes(), streamErr)
	}
	if closeErr != nil {
		return BackupResult{}, fmt.Errorf("close backup data archive: %w", closeErr)
	}
	manifest := BackupManifest{
		APIVersion: backupAPIVersion,
		Kind:       backupKind,
		Cluster:    name,
		CreatedAt:  time.Now().UTC(),
		DataFile:   backupDataFile,
		DataSHA256: hex.EncodeToString(hash.Sum(nil)),
		Config:     config,
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return BackupResult{}, fmt.Errorf("encode backup manifest: %w", err)
	}
	manifestData = append(manifestData, '\n')
	if err := writePrivateFile(filepath.Join(temporary, "manifest.json"), manifestData); err != nil {
		return BackupResult{}, fmt.Errorf("write backup manifest: %w", err)
	}
	if err := os.Rename(temporary, output); err != nil {
		return BackupResult{}, fmt.Errorf("publish backup: %w", err)
	}
	result = BackupResult{Path: output, Bytes: counter.bytes, DataSHA256: manifest.DataSHA256, CreatedAt: manifest.CreatedAt}
	return result, nil
}

// RestoreServer replaces the named APC server volume with a validated backup
// and recreates the server from the configuration stored in the manifest.
func (m *Manager) RestoreServer(ctx context.Context, name, input string) (State, error) {
	manifest, archivePath, err := validateBackup(name, input)
	if err != nil {
		return State{}, err
	}
	containerRecord, inspectErr := m.inspect(ctx, ContainerName(name))
	if inspectErr != nil && !errors.Is(inspectErr, ErrNotFound) {
		return State{}, inspectErr
	}
	if inspectErr == nil {
		if err := validateOwnedContainer(containerRecord, name, "server"); err != nil {
			return State{}, err
		}
		if !strings.EqualFold(containerRecord.Status.State, "stopped") {
			if err := m.Stop(ctx, name); err != nil {
				return State{}, err
			}
		}
	}
	if err := m.ensureVolume(ctx, ServerVolumeName(name), name, "server"); err != nil {
		return State{}, err
	}
	if err := m.startBackupHelper(ctx, name); err != nil {
		return State{}, err
	}
	cleanup := func() error {
		return m.deleteOwnedContainer(ctx, BackupContainerName(name), name, "backup")
	}
	var stderr bytes.Buffer
	if err := m.stream.RunIO(ctx, m.binary, []string{"exec", BackupContainerName(name), "sh", "-c", "rm -rf /data/* /data/.[!.]* /data/..?*"}, nil, io.Discard, &stderr); err != nil {
		return State{}, errors.Join(commandError("clear APC server data before restore", stderr.Bytes(), err), cleanup())
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		return State{}, errors.Join(fmt.Errorf("open backup data archive: %w", err), cleanup())
	}
	stderr.Reset()
	extractErr := m.stream.RunIO(ctx, m.binary, []string{"exec", "-i", BackupContainerName(name), "tar", "-C", "/data", "-xf", "-"}, archive, io.Discard, &stderr)
	closeErr := archive.Close()
	if extractErr != nil {
		return State{}, errors.Join(commandError("restore APC server data", stderr.Bytes(), extractErr), closeErr, cleanup())
	}
	if closeErr != nil {
		return State{}, errors.Join(fmt.Errorf("close backup data archive: %w", closeErr), cleanup())
	}
	if err := cleanup(); err != nil {
		return State{}, err
	}
	if err := saveClusterConfig(manifest.Config); err != nil {
		return State{}, err
	}
	return m.Start(ctx, name, manifest.Config.StartupTimeout)
}

func (m *Manager) startBackupHelper(ctx context.Context, name string) error {
	record, err := m.inspect(ctx, BackupContainerName(name))
	if err == nil {
		if ownershipErr := validateOwnedContainer(record, name, "backup"); ownershipErr != nil {
			return ownershipErr
		}
		return fmt.Errorf("backup helper %q already exists", BackupContainerName(name))
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	arguments := []string{
		"run", "--detach", "--name", BackupContainerName(name),
		"--arch", "arm64", "--cpus", "1", "--memory", "512M",
		"--volume", ServerVolumeName(name) + ":/data",
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster=" + name,
		"--label", "apc.dev/role=backup",
		"--progress", "plain",
		backupImage, "sleep", "300",
	}
	if _, stderr, runErr := m.runner.Run(ctx, m.binary, arguments...); runErr != nil {
		return commandError("start APC backup helper", stderr, runErr)
	}
	return nil
}

func validateBackup(name, input string) (BackupManifest, string, error) {
	if !dnsLabel.MatchString(name) {
		return BackupManifest{}, "", fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	input, err := filepath.Abs(input)
	if err != nil {
		return BackupManifest{}, "", fmt.Errorf("resolve backup path: %w", err)
	}
	info, err := os.Stat(input)
	if err != nil {
		return BackupManifest{}, "", fmt.Errorf("read backup directory: %w", err)
	}
	if !info.IsDir() {
		return BackupManifest{}, "", fmt.Errorf("backup path must be a directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return BackupManifest{}, "", fmt.Errorf("backup directory permissions must be 0700 or stricter")
	}
	manifestData, err := os.ReadFile(filepath.Join(input, "manifest.json"))
	if err != nil {
		return BackupManifest{}, "", fmt.Errorf("read backup manifest: %w", err)
	}
	var manifest BackupManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return BackupManifest{}, "", fmt.Errorf("decode backup manifest: %w", err)
	}
	if manifest.APIVersion != backupAPIVersion || manifest.Kind != backupKind {
		return BackupManifest{}, "", fmt.Errorf("unsupported APC backup format")
	}
	if manifest.Cluster != name || manifest.Config.Name != name {
		return BackupManifest{}, "", fmt.Errorf("backup belongs to cluster %q, not %q", manifest.Cluster, name)
	}
	if manifest.DataFile != backupDataFile {
		return BackupManifest{}, "", fmt.Errorf("backup manifest contains an invalid data file")
	}
	manifest.Config, err = normalizeConfig(manifest.Config)
	if err != nil {
		return BackupManifest{}, "", fmt.Errorf("validate backup cluster configuration: %w", err)
	}
	archivePath := filepath.Join(input, manifest.DataFile)
	archive, err := os.Open(archivePath)
	if err != nil {
		return BackupManifest{}, "", fmt.Errorf("open backup data archive: %w", err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, archive); err != nil {
		_ = archive.Close()
		return BackupManifest{}, "", fmt.Errorf("checksum backup data archive: %w", err)
	}
	actualHash := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actualHash, manifest.DataSHA256) {
		_ = archive.Close()
		return BackupManifest{}, "", fmt.Errorf("backup checksum mismatch: got %s", actualHash)
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		_ = archive.Close()
		return BackupManifest{}, "", fmt.Errorf("rewind backup data archive: %w", err)
	}
	tarReader := tar.NewReader(archive)
	for {
		header, nextErr := tarReader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			_ = archive.Close()
			return BackupManifest{}, "", fmt.Errorf("validate backup tar stream: %w", nextErr)
		}
		if err := validateTarPath(header.Name); err != nil {
			_ = archive.Close()
			return BackupManifest{}, "", err
		}
		if header.Typeflag == tar.TypeLink {
			if err := validateTarPath(header.Linkname); err != nil {
				_ = archive.Close()
				return BackupManifest{}, "", fmt.Errorf("invalid hard link in backup: %w", err)
			}
		}
		if header.Typeflag == tar.TypeSymlink {
			linkTarget := header.Linkname
			resolved := ""
			if pathpkg.IsAbs(linkTarget) {
				const k3sDataRoot = "/var/lib/rancher/k3s/"
				if !strings.HasPrefix(linkTarget, k3sDataRoot) {
					_ = archive.Close()
					return BackupManifest{}, "", fmt.Errorf("backup contains an escaping symbolic link %q", header.Name)
				}
				resolved = pathpkg.Clean(strings.TrimPrefix(linkTarget, k3sDataRoot))
			} else {
				resolved = pathpkg.Clean(pathpkg.Join(pathpkg.Dir(header.Name), linkTarget))
			}
			if resolved == ".." || strings.HasPrefix(resolved, "../") {
				_ = archive.Close()
				return BackupManifest{}, "", fmt.Errorf("backup contains an escaping symbolic link %q", header.Name)
			}
		}
	}
	if err := archive.Close(); err != nil {
		return BackupManifest{}, "", fmt.Errorf("close backup data archive: %w", err)
	}
	return manifest, archivePath, nil
}

func validateTarPath(value string) error {
	clean := pathpkg.Clean(value)
	if pathpkg.IsAbs(value) || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("backup contains an unsafe path %q", value)
	}
	return nil
}

type countingWriter struct {
	writer io.Writer
	bytes  int64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	written, err := w.writer.Write(data)
	w.bytes += int64(written)
	return written, err
}
