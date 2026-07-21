package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const haLockSubprocessEnvironment = "APC_HA_LOCK_SUBPROCESS"

func TestHAOperationLockSerializesAcrossProcesses(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	configPath, err := HAConfigPath("ha-lab")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}

	lock, err := acquireHAOperationLock(context.Background(), "ha-lab")
	if err != nil {
		t.Fatal(err)
	}
	blockedOutput := runHALockSubprocess(t, 150*time.Millisecond)
	if !strings.Contains(blockedOutput, "deadline exceeded") {
		t.Fatalf("contending process was not bounded by its context: %s", blockedOutput)
	}
	if err := lock.release(); err != nil {
		t.Fatal(err)
	}

	acquiredOutput := runHALockSubprocess(t, time.Second)
	if !strings.Contains(acquiredOutput, "acquired") {
		t.Fatalf("released HA lock remained unavailable to another process: %s", acquiredOutput)
	}
}

func TestHAOperationLockCreatesFreshPrivateClusterDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	configPath, err := HAConfigPath("fresh-ha")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Dir(configPath)); !os.IsNotExist(err) {
		t.Fatalf("fresh cluster directory unexpectedly exists: %v", err)
	}
	lock, err := acquireHAOperationLock(context.Background(), "fresh-ha")
	if err != nil {
		t.Fatal(err)
	}
	defer lock.release() //nolint:errcheck -- asserted through the explicit lock test
	info, err := os.Lstat(filepath.Dir(configPath))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("fresh lock directory mode = %v, want private 0700 directory", info.Mode())
	}
	lockInfo, err := os.Lstat(filepath.Join(filepath.Dir(configPath), haOperationLockFilename))
	if err != nil {
		t.Fatal(err)
	}
	if !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0o600 {
		t.Fatalf("fresh operation lock mode = %v, want private 0600 file", lockInfo.Mode())
	}
}

func TestHAOperationLockSubprocessHelper(t *testing.T) {
	if os.Getenv(haLockSubprocessEnvironment) != "1" {
		return
	}
	timeout, err := time.ParseDuration(os.Getenv("APC_HA_LOCK_TIMEOUT"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	lock, err := acquireHAOperationLock(ctx, "ha-lab")
	if err != nil {
		fmt.Printf("lock-error: %v\n", err)
		return
	}
	defer lock.release() //nolint:errcheck -- subprocess exits immediately after the assertion marker
	fmt.Println("acquired")
}

func runHALockSubprocess(t *testing.T, timeout time.Duration) string {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestHAOperationLockSubprocessHelper$")
	command.Env = append(os.Environ(),
		haLockSubprocessEnvironment+"=1",
		"APC_HA_LOCK_TIMEOUT="+timeout.String(),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("HA lock subprocess failed: %v\n%s", err, output)
	}
	return string(output)
}
