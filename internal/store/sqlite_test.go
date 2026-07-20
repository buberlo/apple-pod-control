package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buberlo/apple-pod-control/internal/model"
)

func TestOpenCreatesPrivateRegularDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := Open(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	assertPrivateRegularFile(t, path)
	assertPrivateSQLiteSidecars(t, path)
	if err := database.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}
	assertPrivateRegularFile(t, path)
}

func TestOpenEscapesSQLiteURICharactersAndPragmaTextInFilename(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state?_pragma=journal_mode(OFF)&vfs=foreign#literal.db")
	database, err := Open(path)
	if err != nil {
		t.Fatalf("open database with URI characters: %v", err)
	}
	defer database.Close()

	assertPrivateRegularFile(t, path)
	var sequence int
	var name, openedPath string
	if err := database.db.QueryRowContext(context.Background(), `PRAGMA database_list`).Scan(&sequence, &name, &openedPath); err != nil {
		t.Fatalf("read database_list: %v", err)
	}
	openedInfo, err := os.Lstat(openedPath)
	if err != nil {
		t.Fatalf("inspect opened database: %v", err)
	}
	expectedInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("inspect expected database: %v", err)
	}
	if name != "main" || !os.SameFile(openedInfo, expectedInfo) {
		t.Fatalf("sqlite opened %q (%s), want exact path %q", openedPath, name, path)
	}
	var journalMode string
	if err := database.db.QueryRowContext(context.Background(), `PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("filename injected journal mode %q, want WAL", journalMode)
	}
	if _, err := os.Lstat(filepath.Join(directory, "state")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SQLite opened URI-truncated decoy path: %v", err)
	}
}

func TestSQLiteDSNPreservesMemoryDatabaseAndEscapesFilePath(t *testing.T) {
	if dsn := sqliteDSN(":memory:"); !strings.HasPrefix(dsn, "file::memory:?") {
		t.Fatalf("memory DSN = %q", dsn)
	}
	dsn := sqliteDSN("/tmp/apc?literal#state.db")
	if strings.Contains(dsn, "?literal") || strings.Contains(dsn, "#state") || !strings.Contains(dsn, "%3F") || !strings.Contains(dsn, "%23") {
		t.Fatalf("file DSN did not escape URI delimiters: %q", dsn)
	}
}

func TestOpenSecuresExistingSQLiteSidecarsAndRejectsSidecarSymlink(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create database file: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.WriteFile(path+suffix, nil, 0o644); err != nil {
			t.Fatalf("create %s sidecar: %v", suffix, err)
		}
	}
	if err := secureExistingSQLiteSidecars(path); err != nil {
		t.Fatalf("secure sidecars: %v", err)
	}
	assertPrivateSQLiteSidecars(t, path)

	target := filepath.Join(directory, "outside")
	if err := os.WriteFile(target, []byte("unchanged"), 0o644); err != nil {
		t.Fatalf("create symlink target: %v", err)
	}
	if err := os.Remove(path + "-wal"); err != nil {
		t.Fatalf("remove regular WAL: %v", err)
	}
	if err := os.Symlink(target, path+"-wal"); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	database, err := Open(path)
	if database != nil {
		_ = database.Close()
		t.Fatal("Open accepted a symbolic-link SQLite sidecar")
	}
	if err == nil || !strings.Contains(err.Error(), "sidecar") {
		t.Fatalf("Open error = %v", err)
	}
	contents, readErr := os.ReadFile(target)
	if readErr != nil || string(contents) != "unchanged" {
		t.Fatalf("sidecar symlink target changed: contents=%q err=%v", contents, readErr)
	}
}

func TestOpenMigratesExistingRegularDatabasePermissionsWithoutDataLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := Open(path)
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	stored, _, err := database.UpsertDeployment(context.Background(), storeTestDeployment())
	if err != nil {
		t.Fatalf("seed database: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close seeded database: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("broaden database permissions: %v", err)
	}

	database, err = Open(path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer database.Close()
	assertPrivateRegularFile(t, path)
	read, err := database.GetDeployment(context.Background(), stored.Metadata.Namespace, stored.Metadata.Name)
	if err != nil {
		t.Fatalf("read preserved deployment: %v", err)
	}
	if read.Metadata.UID != stored.Metadata.UID || read.Spec.Replicas != stored.Spec.Replicas {
		t.Fatalf("database contents changed: got %#v, want %#v", read, stored)
	}
}

func TestOpenRefusesSymlinkWithoutChangingTarget(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.db")
	contents := []byte("not a database")
	if err := os.WriteFile(target, contents, 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(directory, "state.db")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	database, err := Open(link)
	if database != nil {
		_ = database.Close()
		t.Fatal("Open returned a database for a symbolic link")
	}
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("Open error = %v", err)
	}
	targetInfo, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("inspect target: %v", err)
	}
	if targetInfo.Mode().Perm() != 0o644 {
		t.Fatalf("target permissions changed to %04o", targetInfo.Mode().Perm())
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(contents) {
		t.Fatalf("target contents changed to %q", got)
	}
}

func TestOpenRefusesNonRegularPathWithoutChangingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("create directory path: %v", err)
	}

	database, err := Open(path)
	if database != nil {
		_ = database.Close()
		t.Fatal("Open returned a database for a directory")
	}
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("Open error = %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("inspect directory path: %v", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o755 {
		t.Fatalf("directory path changed: mode %v", info.Mode())
	}
}

func assertPrivateRegularFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("inspect database file: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("database mode = %v, want regular file", info.Mode())
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("database permissions = %04o, want 0600", info.Mode().Perm())
	}
}

func assertPrivateSQLiteSidecars(t *testing.T, databasePath string) {
	t.Helper()
	for _, suffix := range []string{"-wal", "-shm"} {
		path := databasePath + suffix
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			t.Fatalf("inspect sqlite sidecar %s: %v", suffix, err)
		}
		assertPrivateRegularFile(t, path)
	}
}

func TestDeploymentRoundTripAndGeneration(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment := storeTestDeployment()
	stored, created, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || !created || stored.Metadata.Generation != 1 || stored.Metadata.UID == "" {
		t.Fatalf("first upsert: stored=%#v created=%t err=%v", stored, created, err)
	}
	storedAgain, created, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || created || storedAgain.Metadata.Generation != 1 {
		t.Fatalf("idempotent upsert: stored=%#v created=%t err=%v", storedAgain, created, err)
	}
	deployment.Spec.Replicas = 3
	updated, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || updated.Metadata.Generation != 2 {
		t.Fatalf("updated generation: stored=%#v err=%v", updated, err)
	}
	read, err := database.GetDeployment(ctx, "default", "web")
	if err != nil || read.Spec.Replicas != 3 {
		t.Fatalf("get: %#v, %v", read, err)
	}
}

func storeTestDeployment() model.Deployment {
	return model.Deployment{
		APIVersion: model.APIVersion, Kind: model.Kind, Metadata: model.ObjectMeta{Name: "web"},
		Spec: model.DeploymentSpec{Replicas: 2, Selector: model.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: model.PodTemplateSpec{Metadata: model.TemplateMeta{Labels: map[string]string{"app": "web"}},
				Spec: model.PodSpec{Containers: []model.Container{{Name: "web", Image: "nginx:alpine"}}}}},
	}
}
