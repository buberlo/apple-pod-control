package cluster

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBoundedSupervisorLogCapsRepeatedFailuresAndOversizedLines(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "Library", "Logs", "APC", "server-home-unattended.log")
	runtime := testSupervisorLogRuntime(home, 128)
	log, err := openSupervisorLog(path, "server", "home", runtime)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		line := []byte("persistent reconcile failure that must remain bounded\n")
		if written, err := log.Write(line); err != nil || written != len(line) {
			t.Fatalf("write %d = %d, %v", index, written, err)
		}
		assertSupervisorLogBound(t, path, runtime.maximum)
	}
	oversized := []byte(strings.Repeat("x", 512) + "\n")
	if written, err := log.Write(oversized); err != nil || written != len(oversized) {
		t.Fatalf("oversized write = %d, %v", written, err)
	}
	assertSupervisorLogBound(t, path, runtime.maximum)
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != int(runtime.maximum) || string(data) != string(oversized[len(oversized)-int(runtime.maximum):]) {
		t.Fatalf("oversized line bypassed bounded suffix policy: size=%d", len(data))
	}
	if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != supervisorLogFileMode {
		t.Fatalf("log mode = %v, %v", infoModeForSupervisorLog(info), err)
	}
	if info, err := os.Lstat(filepath.Dir(path)); err != nil || info.Mode().Perm() != supervisorLogDirectoryMode {
		t.Fatalf("log directory mode = %v, %v", infoModeForSupervisorLog(info), err)
	}
}

func TestSupervisorKeepsDurationFailuresInsideBoundedSink(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "Library", "Logs", "APC", "server-home-unattended.log")
	runtime := testSupervisorLogRuntime(home, 256)
	log, err := openSupervisorLog(path, "server", "home", runtime)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager("container")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err = manager.supervise(ctx, SuperviseOptions{Role: "server", Name: "home", Interval: time.Millisecond, Output: log}, supervisorRuntime{
		reconcile: func(context.Context, SuperviseOptions) error {
			return errors.New(strings.Repeat("continuous failure ", 8))
		},
	})
	if err != nil {
		t.Fatalf("bounded duration supervisor: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	assertSupervisorLogBound(t, path, runtime.maximum)
}

func TestSupervisorLogRejectsSymlinkForeignModeAndForeignOwner(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string, *supervisorLogRuntime)
	}{
		{name: "symlinked directory", mutate: func(t *testing.T, path string, _ *supervisorLogRuntime) {
			logs := filepath.Dir(filepath.Dir(path))
			if err := os.MkdirAll(logs, supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			foreign := filepath.Join(t.TempDir(), "foreign-logs")
			if err := os.Mkdir(foreign, supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(foreign, filepath.Dir(path)); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
		}},
		{name: "symlink", mutate: func(t *testing.T, path string, _ *supervisorLogRuntime) {
			if err := os.MkdirAll(filepath.Dir(path), supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			foreign := filepath.Join(t.TempDir(), "foreign.log")
			if err := os.WriteFile(foreign, []byte("foreign"), supervisorLogFileMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(foreign, path); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
		}},
		{name: "foreign mode", mutate: func(t *testing.T, path string, _ *supervisorLogRuntime) {
			if err := os.MkdirAll(filepath.Dir(path), supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("foreign"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "foreign owner", mutate: func(t *testing.T, path string, runtime *supervisorLogRuntime) {
			if err := os.MkdirAll(filepath.Dir(path), supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("foreign"), supervisorLogFileMode); err != nil {
				t.Fatal(err)
			}
			baseOwnership := runtime.ownership
			runtime.ownership = func(candidate string, info os.FileInfo) (int, int, error) {
				uid, gid, err := baseOwnership(candidate, info)
				if candidate == path {
					return uid + 1, gid, err
				}
				return uid, gid, err
			}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, "Library", "Logs", "APC", "server-home-unattended.log")
			runtime := testSupervisorLogRuntime(home, 128)
			test.mutate(t, path, &runtime)
			if log, err := openSupervisorLog(path, "server", "home", runtime); err == nil {
				_ = log.Close()
				t.Fatal("unsafe existing supervisor log was accepted")
			}
		})
	}
}

func TestSupervisorLogRejectsPathOutsideExactHomeSink(t *testing.T) {
	home := t.TempDir()
	runtime := testSupervisorLogRuntime(home, 128)
	foreign := filepath.Join(t.TempDir(), "server-home-unattended.log")
	if log, err := openSupervisorLog(foreign, "server", "home", runtime); err == nil {
		_ = log.Close()
		t.Fatal("supervisor log outside exact HOME sink was accepted")
	}
}

func TestSupervisorLogRejectsDarwinACLAllowGrants(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS extended ACL semantics")
	}
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "home ancestor", mutate: func(t *testing.T, path string) {
			addSupervisorTestACL(t, filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(path)))), "everyone allow read,write,delete")
		}},
		{name: "APC directory", mutate: func(t *testing.T, path string) {
			if err := os.MkdirAll(filepath.Dir(path), supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			addSupervisorTestACL(t, filepath.Dir(path), "everyone allow read,write,delete")
		}},
		{name: "log file", mutate: func(t *testing.T, path string) {
			if err := os.MkdirAll(filepath.Dir(path), supervisorLogDirectoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, nil, supervisorLogFileMode); err != nil {
				t.Fatal(err)
			}
			addSupervisorTestACL(t, path, "everyone allow read,write,delete")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, "Library", "Logs", "APC", "server-home-unattended.log")
			test.mutate(t, path)
			if log, err := openSupervisorLog(path, "server", "home", testSupervisorLogRuntime(home, 128)); err == nil {
				_ = log.Close()
				t.Fatal("allow ACL grant was accepted")
			} else if !strings.Contains(err.Error(), "unsafe extended ACL") {
				t.Fatalf("ACL error = %v", err)
			}
		})
	}
}

func TestSupervisorLogClearsInheritedDenyACLFromNewArtifacts(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS extended ACL semantics")
	}
	home := t.TempDir()
	logs := filepath.Join(home, "Library", "Logs")
	if err := os.MkdirAll(logs, supervisorLogDirectoryMode); err != nil {
		t.Fatal(err)
	}
	addSupervisorTestACL(t, logs, "everyone deny delete,file_inherit,directory_inherit")
	path := filepath.Join(logs, "APC", "server-home-unattended.log")
	log, err := openSupervisorLog(path, "server", "home", testSupervisorLogRuntime(home, 128))
	if err != nil {
		t.Fatalf("deny-only ancestor ACL should be tolerated: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	assertSupervisorACLFree(t, filepath.Dir(path))
	assertSupervisorACLFree(t, path)
}

func addSupervisorTestACL(t *testing.T, path, entry string) {
	t.Helper()
	output, err := exec.Command(supervisorChmodPath, "+a", entry, path).CombinedOutput()
	if err != nil {
		t.Fatalf("add supervisor test ACL: %v: %s", err, output)
	}
	t.Cleanup(func() {
		_, _ = exec.Command(supervisorChmodPath, "-N", path).CombinedOutput()
	})
}

func assertSupervisorACLFree(t *testing.T, path string) {
	t.Helper()
	output, err := exec.Command(supervisorLSPath, "-lde", path).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect supervisor ACL: %v: %s", err, output)
	}
	lines := bytesSplitNonemptySupervisorACL(output)
	if len(lines) != 1 {
		t.Fatalf("supervisor artifact retained an ACL: %s", output)
	}
}

func bytesSplitNonemptySupervisorACL(output []byte) []string {
	var result []string
	for _, line := range strings.Split(string(output), "\n") {
		if strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return result
}

func testSupervisorLogRuntime(home string, maximum int64) supervisorLogRuntime {
	return supervisorLogRuntime{
		home: home, uid: os.Geteuid(), gid: os.Getegid(), maximum: maximum,
		ownership: supervisorLogFileOwnership,
	}
}

func assertSupervisorLogBound(t *testing.T, path string, maximum int64) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > maximum {
		t.Fatalf("supervisor log size = %d, cap = %d", info.Size(), maximum)
	}
}

func infoModeForSupervisorLog(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode()
}
