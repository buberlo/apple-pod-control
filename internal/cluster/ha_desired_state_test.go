package cluster

import (
	"os"
	"strings"
	"testing"
)

func TestHADesiredStateIsPrivateStrictAndRoundTrips(t *testing.T) {
	setHAConfigHome(t)
	state := defaultHADesiredState("ha-lab")
	state.ClusterState = haDesiredStopped
	if err := saveHADesiredState(state); err != nil {
		t.Fatal(err)
	}
	path, err := haDesiredStatePath(state.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("desired-state mode = %v, want private regular 0600", info.Mode())
	}
	loaded, err := loadHADesiredState(state.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ClusterState != haDesiredStopped || loaded.Cluster != state.Cluster || loaded.UpdatedAt.IsZero() {
		t.Fatalf("loaded desired state = %+v", loaded)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "\n}", ",\n  \"unknown\": true\n}", 1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHADesiredState(state.Cluster); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("strict decode error = %v", err)
	}
}

func TestHADesiredStateRejectsPublicFileAndSymlink(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "public mode",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				target := path + ".target"
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			state := defaultHADesiredState("ha-lab")
			if err := saveHADesiredState(state); err != nil {
				t.Fatal(err)
			}
			path, err := haDesiredStatePath(state.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, path)
			if _, err := loadHADesiredState(state.Cluster); err == nil || !strings.Contains(err.Error(), "private regular file") {
				t.Fatalf("private-file validation error = %v", err)
			}
		})
	}
}

func TestReadExactHADesiredStateRejectsPathSwap(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
		open func(string) (*os.File, error)
	}{
		{
			name: "before open",
			want: "changed while it was being opened",
			open: func(path string) (*os.File, error) {
				if err := os.Rename(path, path+".original"); err != nil {
					return nil, err
				}
				if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
					return nil, err
				}
				return openHADesiredStateFile(path)
			},
		},
		{
			name: "after open",
			want: "changed while it was being read",
			open: func(path string) (*os.File, error) {
				file, err := openHADesiredStateFile(path)
				if err != nil {
					return nil, err
				}
				if err := os.Rename(path, path+".original"); err != nil {
					file.Close()
					return nil, err
				}
				if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
					file.Close()
					return nil, err
				}
				return file, nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			state := defaultHADesiredState("ha-lab")
			if err := saveHADesiredState(state); err != nil {
				t.Fatal(err)
			}
			path, err := haDesiredStatePath(state.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := readExactHADesiredStateFile(path, test.open); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("path-swap error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadHADesiredStateRejectsOversizeFile(t *testing.T) {
	setHAConfigHome(t)
	state := defaultHADesiredState("ha-lab")
	if err := saveHADesiredState(state); err != nil {
		t.Fatal(err)
	}
	path, err := haDesiredStatePath(state.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", int(haDesiredStateMaximum)+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHADesiredState(state.Cluster); err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("oversize desired-state error = %v", err)
	}
}

func TestHADesiredStateMemberAndClusterTransitions(t *testing.T) {
	setHAConfigHome(t)
	const name = "ha-lab"
	if err := setHAMemberIntentLocked(name, 2, true); err != nil {
		t.Fatal(err)
	}
	state, err := loadHADesiredState(name)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClusterState != haDesiredRunning || len(state.StoppedMembers) != 1 || state.StoppedMembers[0] != 2 {
		t.Fatalf("member stop intent = %+v", state)
	}
	if err := markHAClusterStoppedLocked(name); err != nil {
		t.Fatal(err)
	}
	state, err = loadHADesiredState(name)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClusterState != haDesiredStopped || !haMemberIntentionallyStopped(state, 2) {
		t.Fatalf("cluster stop intent = %+v", state)
	}
	if err := markHAClusterRunningLocked(name); err != nil {
		t.Fatal(err)
	}
	state, err = loadHADesiredState(name)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClusterState != haDesiredRunning || len(state.StoppedMembers) != 0 {
		t.Fatalf("cluster start intent = %+v", state)
	}
}
