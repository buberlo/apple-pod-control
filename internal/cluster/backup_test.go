package cluster

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateBackupChecksFormatChecksumAndCluster(t *testing.T) {
	directory := writeTestBackup(t, "home", "state/value", []byte("preserved"))
	manifest, archive, err := validateBackup("home", directory)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Cluster != "home" || filepath.Base(archive) != backupDataFile {
		t.Fatalf("unexpected backup: %#v, %q", manifest, archive)
	}
	if _, _, err := validateBackup("other", directory); err == nil || !strings.Contains(err.Error(), "belongs to cluster") {
		t.Fatalf("cluster mismatch was accepted: %v", err)
	}
	archiveFile, err := os.OpenFile(filepath.Join(directory, backupDataFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archiveFile.Write([]byte("tampered")); err != nil {
		t.Fatal(err)
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateBackup("home", directory); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum mismatch was accepted: %v", err)
	}
}

func TestValidateBackupRejectsTarPathTraversal(t *testing.T) {
	directory := writeTestBackup(t, "home", "../escape", []byte("unsafe"))
	if _, _, err := validateBackup("home", directory); err == nil || !strings.Contains(err.Error(), "unsafe path") {
		t.Fatalf("unsafe path was accepted: %v", err)
	}
}

func TestValidateBackupAllowsK3sInternalAbsoluteSymlinksOnly(t *testing.T) {
	directory := writeTestSymlinkBackup(t, "/var/lib/rancher/k3s/server/token")
	if _, _, err := validateBackup("home", directory); err != nil {
		t.Fatalf("K3s data-root symlink rejected: %v", err)
	}
	directory = writeTestSymlinkBackup(t, "/etc/passwd")
	if _, _, err := validateBackup("home", directory); err == nil || !strings.Contains(err.Error(), "escaping symbolic link") {
		t.Fatalf("external absolute symlink was accepted: %v", err)
	}
}

func writeTestBackup(t *testing.T, clusterName, entryName string, data []byte) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(directory, backupDataFile)
	archive, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(archive)
	if err := tw.WriteHeader(&tar.Header{Name: entryName, Mode: 0o600, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(archiveData)
	config, err := normalizeConfig(Config{Name: clusterName})
	if err != nil {
		t.Fatal(err)
	}
	manifest := BackupManifest{
		APIVersion: backupAPIVersion,
		Kind:       backupKind,
		Cluster:    clusterName,
		CreatedAt:  time.Now().UTC(),
		DataFile:   backupDataFile,
		DataSHA256: hex.EncodeToString(digest[:]),
		Config:     config,
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), manifestData, 0o600); err != nil {
		t.Fatal(err)
	}
	return directory
}

func writeTestSymlinkBackup(t *testing.T, linkTarget string) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(directory, backupDataFile)
	archive, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(archive)
	if err := tw.WriteHeader(&tar.Header{Name: "server/node-token", Linkname: linkTarget, Typeflag: tar.TypeSymlink, Mode: 0o777}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(archiveData)
	config, err := normalizeConfig(Config{Name: "home"})
	if err != nil {
		t.Fatal(err)
	}
	manifest := BackupManifest{APIVersion: backupAPIVersion, Kind: backupKind, Cluster: "home", DataFile: backupDataFile, DataSHA256: hex.EncodeToString(digest[:]), Config: config}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), manifestData, 0o600); err != nil {
		t.Fatal(err)
	}
	return directory
}
