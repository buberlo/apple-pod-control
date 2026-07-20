package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

const haRecoveryTestToken = "test-secret-value"

func TestLoadHARecoveryStateRejectsSymlink(t *testing.T) {
	path := writeHARecoveryStateFixture(t, "ha-lab")
	target := path + ".target"
	if err := os.Rename(path, target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadHARecoveryState("ha-lab"); err == nil || !strings.Contains(err.Error(), "private regular file") {
		t.Fatalf("symlink recovery-state error = %v", err)
	}
}

func TestReadExactHARecoveryStateRejectsPathSwap(t *testing.T) {
	for _, test := range []struct {
		name      string
		afterOpen bool
		want      string
	}{
		{name: "between lstat and open", want: "being opened"},
		{name: "after open", afterOpen: true, want: "being read"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeHARecoveryStateFixture(t, "ha-lab")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			replacement := path + ".replacement"
			archived := path + ".original"
			if err := os.WriteFile(replacement, data, 0o600); err != nil {
				t.Fatal(err)
			}
			opener := func(candidate string) (*os.File, error) {
				if test.afterOpen {
					file, openErr := os.Open(candidate)
					if openErr != nil {
						return nil, openErr
					}
					if renameErr := os.Rename(candidate, archived); renameErr != nil {
						_ = file.Close()
						return nil, renameErr
					}
					if renameErr := os.Rename(replacement, candidate); renameErr != nil {
						_ = file.Close()
						return nil, renameErr
					}
					return file, nil
				}
				if renameErr := os.Rename(candidate, archived); renameErr != nil {
					return nil, renameErr
				}
				if renameErr := os.Rename(replacement, candidate); renameErr != nil {
					return nil, renameErr
				}
				return os.Open(candidate)
			}

			if _, err := readExactHARecoveryStateFile(path, opener); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("swapped recovery-state error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadHARecoveryStateRejectsForeignOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing file ownership requires root")
	}
	path := writeHARecoveryStateFixture(t, "ha-lab")
	const foreignUID = 1
	if err := os.Chown(path, foreignUID, -1); err != nil {
		t.Skipf("cannot create foreign-owned recovery state: %v", err)
	}
	t.Cleanup(func() { _ = os.Chown(path, os.Geteuid(), -1) })
	if _, err := LoadHARecoveryState("ha-lab"); err == nil || !strings.Contains(err.Error(), "owned by the current user") {
		t.Fatalf("foreign-owner recovery-state error = %v", err)
	}
}

func TestLoadHARecoveryStateRejectsOversizedAndMalformedDocuments(t *testing.T) {
	t.Run("oversized", func(t *testing.T) {
		path := writeHARecoveryStateFixture(t, "ha-lab")
		if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, int(haRecoveryStateMaximum+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadHARecoveryState("ha-lab"); err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
			t.Fatalf("oversized recovery-state error = %v", err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*HARecoveryState)
		want   string
	}{
		{name: "unknown phase", mutate: func(state *HARecoveryState) { state.Phase = "mystery" }, want: "unknown phase"},
		{name: "empty identity", mutate: func(state *HARecoveryState) { state.ClusterIdentity = "" }, want: "empty cluster identity"},
		{name: "empty snapshot path", mutate: func(state *HARecoveryState) { state.SnapshotPath = "" }, want: "invalid snapshot path"},
		{name: "zero start time", mutate: func(state *HARecoveryState) { state.StartedAt = time.Time{} }, want: "invalid start/update timestamps"},
		{name: "completed without completedAt", mutate: func(state *HARecoveryState) { state.CompletedAt = time.Time{} }, want: "requires a successful"},
		{
			name: "failed success without attempt",
			mutate: func(state *HARecoveryState) {
				state.Phase = "failed"
				state.CompletedAt = time.Time{}
				state.RecoveryAttempted = false
				state.RecoverySucceeded = true
			},
			want: "inconsistent recovery flags",
		},
		{
			name: "in progress with terminal fields",
			mutate: func(state *HARecoveryState) {
				state.Phase = "verifying"
				state.CompletedAt = time.Time{}
				state.RecoverySucceeded = true
			},
			want: "inconsistent terminal fields",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			state := validHARecoveryStateFixture("ha-lab")
			test.mutate(&state)
			if err := saveHARecoveryState(state); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadHARecoveryState(state.Cluster); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("malformed recovery-state error = %v, want %q", err, test.want)
			}
		})
	}
}

func writeHARecoveryStateFixture(t *testing.T, name string) string {
	t.Helper()
	setHAConfigHome(t)
	state := validHARecoveryStateFixture(name)
	if err := saveHARecoveryState(state); err != nil {
		t.Fatal(err)
	}
	path, err := HARecoveryStatePath(name)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func validHARecoveryStateFixture(name string) HARecoveryState {
	now := time.Now().UTC()
	return HARecoveryState{
		APIVersion:        haSnapshotAPIVersion,
		Kind:              haRecoveryStateKind,
		Cluster:           name,
		ClusterIdentity:   "test-cluster-identity",
		SnapshotPath:      "/tmp/test-snapshot",
		Phase:             "completed",
		StartedAt:         now,
		UpdatedAt:         now,
		CompletedAt:       now,
		RecoverySucceeded: true,
	}
}

func TestSnapshotHAExportsProtectedEtcdSnapshotAndRestoresQuorum(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }
	output := filepath.Join(t.TempDir(), "ha-lab.snapshot")

	result, err := manager.SnapshotHA(context.Background(), config.Name, output)
	if err != nil {
		t.Fatal(err)
	}
	resolvedOutput, err := filepath.EvalSymlinks(output)
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != resolvedOutput || result.Bytes <= 0 || !validSHA256(result.DataSHA256) || result.CreatedAt.IsZero() || result.Manifest.Cluster.Name != config.Name {
		t.Fatalf("unexpected snapshot result: %+v", result)
	}
	manifestData, err := os.ReadFile(filepath.Join(output, haSnapshotManifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(manifestData), haRecoveryTestToken) {
		t.Fatal("server token value leaked into HA snapshot manifest")
	}
	directoryInfo, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	manifestInfo, err := os.Stat(filepath.Join(output, haSnapshotManifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	if directoryInfo.Mode().Perm() != 0o700 || manifestInfo.Mode().Perm() != 0o400 {
		t.Fatalf("snapshot permissions directory=%o manifest=%o", directoryInfo.Mode().Perm(), manifestInfo.Mode().Perm())
	}
	if _, _, err := validateHASnapshot(context.Background(), config, output); err != nil {
		t.Fatalf("published snapshot did not validate: %v", err)
	}

	assertRecoveryCallOrder(t, runner.calls,
		[]string{"exec", HAContainerName(config.Name, 1), "/bin/k3s", "etcd-snapshot", "save"},
		[]string{"stop", HAContainerName(config.Name, 1)},
		[]string{"run", "--detach", "--name", HASnapshotHelperContainerName(config.Name)},
		[]string{"exec", HASnapshotHelperContainerName(config.Name), "/bin/cp"},
		[]string{"delete", HASnapshotHelperContainerName(config.Name)},
		[]string{"run", "--detach", "--name", HAContainerName(config.Name, 1)},
		[]string{"exec", HAContainerName(config.Name, 1), "/bin/k3s", "etcd-snapshot", "delete"},
	)
}

func TestSnapshotHAPreservesPublishedPackageWhenInternalCleanupFails(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.failSnapshotDelete = true
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }
	output := filepath.Join(t.TempDir(), "cleanup-warning.snapshot")

	result, err := manager.SnapshotHA(context.Background(), config.Name, output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Warning, "safely published") || !strings.Contains(result.Warning, "injected snapshot cleanup failure") {
		t.Fatalf("snapshot cleanup warning = %q", result.Warning)
	}
	if _, _, err := validateHASnapshot(context.Background(), config, output); err != nil {
		t.Fatalf("cleanup failure removed or corrupted the published package: %v", err)
	}
}

func TestSnapshotHARecoversAmbiguousAppliedRuntimeMutations(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*haRecoveryRunner)
		wantError string
	}{
		{
			name: "source stop applied before client error",
			configure: func(runner *haRecoveryRunner) {
				runner.failSnapshotStopApplied = true
			},
			wantError: "injected applied snapshot stop failure",
		},
		{
			name: "helper created before client error",
			configure: func(runner *haRecoveryRunner) {
				runner.failSnapshotRunApplied = true
			},
			wantError: "injected applied snapshot helper failure",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := prepareHARecoveryTestConfig(t)
			runner := newHARecoveryRunner(t, config)
			test.configure(runner)
			manager := NewManager("container")
			manager.runner = runner
			manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

			_, err := manager.SnapshotHA(context.Background(), config.Name, filepath.Join(t.TempDir(), "ambiguous.snapshot"))
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("ambiguous snapshot error = %v", err)
			}
			if !runner.serverExists[1] || runner.serverState[1] != "running" {
				t.Fatalf("snapshot source was not recovered: exists=%v state=%q", runner.serverExists[1], runner.serverState[1])
			}
			if _, exists := runner.helpers[HASnapshotHelperContainerName(config.Name)]; exists {
				t.Fatal("ambiguous helper run left the exact recovery helper behind")
			}
			findRecoveryCall(t, runner.calls, []string{"exec", HAContainerName(config.Name, 1), "/bin/k3s", "etcd-snapshot", "delete"})
		})
	}
}

func TestSnapshotHADeletesDiscoveredNativeSnapshotAfterExportFailure(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.failSnapshotCopy = true
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }
	output := filepath.Join(t.TempDir(), "failed-export.snapshot")

	_, err := manager.SnapshotHA(context.Background(), config.Name, output)
	if err == nil || !strings.Contains(err.Error(), "injected snapshot copy failure") {
		t.Fatalf("snapshot export error = %v", err)
	}
	findRecoveryCall(t, runner.calls, []string{"exec", HAContainerName(config.Name, 1), "/bin/k3s", "etcd-snapshot", "delete"})
	if _, statErr := os.Stat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed snapshot package unexpectedly published: %v", statErr)
	}
}

func TestSnapshotHARefusesUnhealthyQuorumBeforeMutation(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.readyMembers = 1
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, member HAMember) bool { return member.ID == 1 }

	_, err := manager.SnapshotHA(context.Background(), config.Name, filepath.Join(t.TempDir(), "refused.snapshot"))
	if err == nil || !strings.Contains(err.Error(), "healthy quorum") {
		t.Fatalf("unhealthy quorum error = %v", err)
	}
	for _, call := range runner.calls {
		if isHARecoveryMutation(call) {
			t.Fatalf("runtime mutation occurred before quorum refusal: %#v", call)
		}
	}
}

func TestSnapshotHARefusesMismatchedEtcdTopologyBeforeMutation(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.failEtcdTopology = true
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	_, err := manager.SnapshotHA(context.Background(), config.Name, filepath.Join(t.TempDir(), "refused-etcd.snapshot"))
	if err == nil || !strings.Contains(err.Error(), "without exact healthy embedded-etcd topology") {
		t.Fatalf("invalid etcd topology error = %v", err)
	}
	for _, call := range runner.calls {
		if isHARecoveryMutation(call) {
			t.Fatalf("runtime mutation occurred before etcd topology refusal: %#v", call)
		}
	}
}

func TestValidateHASnapshotRejectsCorruptionTokenMismatchTraversalAndWrongCluster(t *testing.T) {
	t.Run("corruption", func(t *testing.T) {
		config := prepareHARecoveryTestConfig(t)
		input := writeHARecoveryTestSnapshot(t, config)
		file, err := os.OpenFile(filepath.Join(input, haSnapshotDataFilename), os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString("tampered"); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := validateHASnapshot(context.Background(), config, input); err == nil || !strings.Contains(err.Error(), "checksum or size mismatch") {
			t.Fatalf("corrupt snapshot error = %v", err)
		}
	})

	t.Run("token mismatch", func(t *testing.T) {
		config := prepareHARecoveryTestConfig(t)
		input := writeHARecoveryTestSnapshot(t, config)
		if err := writePrivateFileAtomic(config.TokenFile, []byte("different-secret\n")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := validateHASnapshot(context.Background(), config, input); err == nil || !strings.Contains(err.Error(), "does not match cluster") {
			t.Fatalf("token mismatch error = %v", err)
		} else if strings.Contains(err.Error(), haRecoveryTestToken) || strings.Contains(err.Error(), "different-secret") {
			t.Fatalf("token value leaked through validation error: %v", err)
		}
	})

	t.Run("path traversal", func(t *testing.T) {
		config := prepareHARecoveryTestConfig(t)
		input := writeHARecoveryTestSnapshot(t, config)
		manifest := readHARecoveryTestManifest(t, input)
		manifest.Snapshot.Name = "../escape"
		writeHARecoveryTestManifest(t, input, manifest)
		if _, _, err := validateHASnapshot(context.Background(), config, input); err == nil || !strings.Contains(err.Error(), "unsafe artifact path") {
			t.Fatalf("path traversal error = %v", err)
		}
	})

	t.Run("wrong cluster", func(t *testing.T) {
		config := prepareHARecoveryTestConfig(t)
		input := writeHARecoveryTestSnapshot(t, config)
		manifest := readHARecoveryTestManifest(t, input)
		manifest.Cluster.Name = "other"
		writeHARecoveryTestManifest(t, input, manifest)
		if _, _, err := validateHASnapshot(context.Background(), config, input); err == nil || !strings.Contains(err.Error(), "belongs to cluster") {
			t.Fatalf("wrong cluster error = %v", err)
		}
	})
}

func TestRestoreHARejectsWrongClusterBeforeRuntimeInspectionOrMutation(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	input := writeHARecoveryTestSnapshot(t, config)
	manifest := readHARecoveryTestManifest(t, input)
	manifest.Cluster.Name = "other"
	writeHARecoveryTestManifest(t, input, manifest)
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		t.Fatalf("invalid snapshot reached runtime command: %#v", arguments)
		return nil, nil, errors.New("unexpected runtime command")
	}}
	manager := NewManager("container")
	manager.runner = runner

	_, err := manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "belongs to cluster") {
		t.Fatalf("wrong cluster restore error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("invalid restore made runtime calls: %#v", runner.calls)
	}
	desired, desiredErr := loadHADesiredState(config.Name)
	if desiredErr != nil {
		t.Fatal(desiredErr)
	}
	if desired.ClusterState != haDesiredStopped {
		t.Fatalf("invalid restore changed desired state to %+v", desired)
	}
}

func TestRestoreHARequiresExactNetworkBeforeRuntimeMutation(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	runner := newHARecoveryRunner(t, config)
	runner.networkMissing = true
	manager := NewManager("container")
	manager.runner = runner

	_, err := manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "requires the exact APC-owned network") {
		t.Fatalf("missing network restore error = %v", err)
	}
	for _, call := range runner.calls {
		if isHARecoveryMutation(call) {
			t.Fatalf("restore mutated runtime before rejecting the missing network: %#v", call)
		}
	}
}

func TestRestoreHAPreservesStoppedDesiredWhenJournalCannotBePublished(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	journalPath, err := HARecoveryStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	// Force journal publication to fail. The nonterminal journal is the crash
	// barrier for the supervisor, so desired intent must remain Stopped and no
	// runtime mutation may occur when that barrier cannot be published.
	if err := os.Mkdir(journalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := newHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner

	_, err = manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "save HA recovery state") {
		t.Fatalf("journal publication error = %v", err)
	}
	for _, call := range runner.calls {
		if isHARecoveryMutation(call) {
			t.Fatalf("restore mutated runtime before journal publication: %#v", call)
		}
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredStopped {
		t.Fatalf("restore desired state = %+v, want Stopped", desired)
	}
}

func TestRestoreHARetryCleansExactStaleHelperAfterAmbiguousRuntimeErrors(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	runner := newHARecoveryRunner(t, config)
	spec := installRunningRestoreResetHelper(t, runner, config, input)
	runner.failHelperStopApplied = true
	runner.failHelperDeleteApplied = true
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	state, err := manager.RestoreHA(context.Background(), config.Name, input, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadyMembers != haMemberCount || !state.Healthy {
		t.Fatalf("retried restore state = %+v", state)
	}
	if _, exists := runner.helpers[spec.Name]; exists {
		t.Fatalf("stale restore helper %q remains after retry", spec.Name)
	}
	assertRecoveryCallOrder(t, runner.calls,
		[]string{"stop", spec.Name},
		[]string{"delete", spec.Name},
		[]string{"stop", HAContainerName(config.Name, 3)},
	)
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredRunning {
		t.Fatalf("successful restore retry desired state = %+v", desired)
	}
}

func TestRestoreHARetryRefusesForeignOrDriftedStaleHelper(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*haContainerInspect)
	}{
		{
			name: "foreign labels",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Labels["apc.dev/cluster"] = "foreign"
			},
		},
		{
			name: "foreign volume",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Mounts[0].Type.Volume.Name = "foreign-volume"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := prepareHARecoveryTestConfig(t)
			input := writeHARecoveryTestSnapshot(t, config)
			if err := markHAClusterStoppedLocked(config.Name); err != nil {
				t.Fatal(err)
			}
			runner := newHARecoveryRunner(t, config)
			spec := installRunningRestoreResetHelper(t, runner, config, input)
			helper := runner.helpers[spec.Name]
			test.mutate(&helper.record)
			runner.helpers[spec.Name] = helper
			manager := NewManager("container")
			manager.runner = runner

			_, err := manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
			if err == nil || !strings.Contains(err.Error(), "reconcile stale HA restore helper") {
				t.Fatalf("unsafe stale helper error = %v", err)
			}
			for _, call := range runner.calls {
				if isHARecoveryMutation(call) {
					t.Fatalf("unsafe stale helper allowed runtime mutation: %#v", call)
				}
			}
			desired, desiredErr := loadHADesiredState(config.Name)
			if desiredErr != nil {
				t.Fatal(desiredErr)
			}
			if desired.ClusterState != haDesiredStopped {
				t.Fatalf("unsafe stale helper changed desired state to %+v", desired)
			}
		})
	}
}

func installRunningRestoreResetHelper(t *testing.T, runner *haRecoveryRunner, config HAConfig, input string) haRecoveryHelperSpec {
	t.Helper()
	resolvedInput, err := filepath.EvalSymlinks(input)
	if err != nil {
		t.Fatal(err)
	}
	spec := restoreResetHelperSpec(config, config.Members[0], resolvedInput)
	helper := recoveryFakeHelperFromRun(t, haRestoreResetRunArguments(config, config.Members[0], spec))
	helper.record.Status.State = "running"
	runner.helpers[spec.Name] = helper
	return spec
}

func TestRestoreHAFollowsK3sThreeMemberResetSequence(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	runner := newHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	startedAt := time.Now()
	state, err := manager.RestoreHA(context.Background(), config.Name, input, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadyMembers != haMemberCount || !state.Healthy {
		t.Fatalf("restored state = %+v", state)
	}
	if runner.resetDeadline.IsZero() || runner.resetDeadline.Before(startedAt.Add(179*time.Second)) || runner.resetDeadline.After(startedAt.Add(181*time.Second)) {
		t.Fatalf("reset helper deadline = %s, want the original three-minute total restore deadline instead of the old fixed two-minute cap", runner.resetDeadline)
	}
	journal, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "completed" || !journal.RecoverySucceeded || journal.ClusterIdentity == "" {
		t.Fatalf("recovery journal = %+v", journal)
	}

	stop3 := []string{"stop", HAContainerName(config.Name, 3)}
	stop2 := []string{"stop", HAContainerName(config.Name, 2)}
	stop1 := []string{"stop", HAContainerName(config.Name, 1)}
	reset := []string{"run", "--name", HARestoreResetContainerName(config.Name)}
	start1 := []string{"run", "--detach", "--name", HAContainerName(config.Name, 1)}
	clear2 := []string{"exec", HARestoreClearContainerName(config.Name, 2), "/bin/rm", "-rf", "/data/server/db"}
	start2 := []string{"run", "--detach", "--name", HAContainerName(config.Name, 2)}
	clear3 := []string{"exec", HARestoreClearContainerName(config.Name, 3), "/bin/rm", "-rf", "/data/server/db"}
	start3 := []string{"run", "--detach", "--name", HAContainerName(config.Name, 3)}
	assertRecoveryCallOrder(t, runner.calls, stop3, stop2, stop1, reset, start1, clear2, start2, clear3, start3)

	resetCall := findRecoveryCall(t, runner.calls, reset)
	joined := strings.Join(resetCall, " ")
	if !strings.Contains(joined, "--cluster-reset --cluster-reset-restore-path /backup/etcd-snapshot") || !strings.Contains(joined, "--token-file /backup/server-token") {
		t.Fatalf("reset call lacks protected restore arguments: %#v", resetCall)
	}
	if strings.Contains(joined, haRecoveryTestToken) {
		t.Fatal("server token value leaked into reset process arguments")
	}
}

func TestRecoveryFakeDerivesCriticalHelperIdentityFromRunArguments(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	spec := snapshotHelperSpec(config, config.Members[0], t.TempDir())
	if err := manager.startHARecoveryHelper(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	runCall := findRecoveryCall(t, runner.calls, []string{"run", "--detach", "--name", spec.Name})
	actual := recoveryFakeHelperFromRun(t, runCall)
	if err := validateHARecoveryHelper(actual.record, spec); err != nil {
		t.Fatalf("correct helper argv did not reconstruct its inspect identity: %v", err)
	}

	withoutVolume := removeRecoveryFlagValue(runCall, "--volume")
	missing := recoveryFakeHelperFromRun(t, withoutVolume)
	if err := validateHARecoveryHelper(missing.record, spec); err == nil {
		t.Fatal("helper fake synthesized an omitted member volume instead of exposing it")
	}
	withExtraVolume := appendRecoveryFlagValue(runCall, "--volume", "foreign-volume:/foreign")
	extra := recoveryFakeHelperFromRun(t, withExtraVolume)
	if err := validateHARecoveryHelper(extra.record, spec); err == nil {
		t.Fatal("helper ownership accepted an extra critical volume mount")
	}

	serverRecord := configuredHAContainer(config, config.Members[0], "running")
	tests := []struct {
		name   string
		mutate func(*haContainerInspect)
	}{
		{"wrong platform", func(record *haContainerInspect) { record.Configuration.Platform.Architecture = "amd64" }},
		{"non-root process", func(record *haContainerInspect) { record.Configuration.InitProcess.User.ID.UID = 501 }},
		{"terminal process", func(record *haContainerInspect) { record.Configuration.InitProcess.Terminal = true }},
		{"extra capability", func(record *haContainerInspect) { record.Configuration.CapAdd = []string{"ALL"} }},
		{"dropped capability", func(record *haContainerInspect) { record.Configuration.CapDrop = []string{"CAP_NET_RAW"} }},
		{"writable volume option drift", func(record *haContainerInspect) { record.Configuration.Mounts[0].Options = []string{"ro"} }},
		{"unexpected network", func(record *haContainerInspect) { record.Configuration.Networks = serverRecord.Configuration.Networks }},
		{"published port", func(record *haContainerInspect) {
			record.Configuration.PublishedPorts = serverRecord.Configuration.PublishedPorts
		}},
		{"published socket", func(record *haContainerInspect) {
			record.Configuration.PublishedSockets = []json.RawMessage{json.RawMessage(`{}`)}
		}},
		{"unexpected runtime feature", func(record *haContainerInspect) { record.Configuration.Rosetta = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			drifted := recoveryFakeHelperFromRun(t, runCall)
			test.mutate(&drifted.record)
			if err := validateHARecoveryHelper(drifted.record, spec); err == nil {
				t.Fatalf("helper identity accepted %s", test.name)
			}
		})
	}
}

func TestRestoreResetHelperRequiresExactCapabilityAndNetworkIdentity(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	spec := restoreResetHelperSpec(config, config.Members[0], t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.runHARestoreReset(ctx, config, config.Members[0], spec); err != nil {
		t.Fatal(err)
	}
	runCall := findRecoveryCall(t, runner.calls, []string{"run", "--name", spec.Name})
	actual := recoveryFakeHelperFromRun(t, runCall)
	if err := validateHARecoveryHelper(actual.record, spec); err != nil {
		t.Fatalf("exact reset helper identity did not validate: %v", err)
	}

	withoutCapability := recoveryFakeHelperFromRun(t, removeRecoveryFlagValue(runCall, "--cap-add"))
	if err := validateHARecoveryHelper(withoutCapability.record, spec); err == nil {
		t.Fatal("reset helper identity accepted a missing ALL capability")
	}
	driftedNetwork := recoveryFakeHelperFromRun(t, runCall)
	driftedNetwork.record.Configuration.Networks[0].Options.MACAddress = "02:00:00:00:00:ff"
	if err := validateHARecoveryHelper(driftedNetwork.record, spec); err == nil {
		t.Fatal("reset helper identity accepted a foreign recovery MAC")
	}
}

func TestRunHARestoreResetRetriesRuntimeIPv4CollisionsBeforeK3sMutation(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.resetCollisionAddresses = []string{config.Members[1].StableIP, config.Members[2].StableIP}
	manager := NewManager("container")
	manager.runner = runner
	spec := restoreResetHelperSpec(config, config.Members[0], t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := manager.runHARestoreReset(ctx, config, config.Members[0], spec); err != nil {
		t.Fatal(err)
	}
	if runner.resetRunCount != 3 {
		t.Fatalf("restore reset attempts = %d, want two guarded collisions and one reset", runner.resetRunCount)
	}
	if _, exists := runner.helpers[spec.Name]; exists {
		t.Fatalf("restore reset helper %q was not removed", spec.Name)
	}
	deleteCount := 0
	for _, call := range runner.calls {
		if reflect.DeepEqual(call, []string{"delete", spec.Name}) {
			deleteCount++
		}
	}
	if deleteCount != 3 {
		t.Fatalf("restore reset helper deletes = %d, want one exact cleanup per attempt; calls=%#v", deleteCount, runner.calls)
	}
}

func TestRestoreRecoveryDeletesOnlyExactStoppedLegacyEnvelope(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	member := config.Members[0]
	runner.states[member.ID] = "stopped"
	runner.ready[member.ID] = false
	runner.legacy[member.ID] = true

	if err := manager.deleteStoppedHAMemberForRecovery(context.Background(), config, member); err != nil {
		t.Fatal(err)
	}
	want := []string{"delete:" + HAContainerName(config.Name, member.ID)}
	if !reflect.DeepEqual(runner.mutations, want) {
		t.Fatalf("legacy restore deletion mutations = %#v, want %#v", runner.mutations, want)
	}
}

func TestRestoreHAPropagatesFailureAndAttemptsSafeRecovery(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	runner := newHARecoveryRunner(t, config)
	runner.failMemberTwoStartOnce = true
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	_, err := manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "injected member 2 start failure") {
		t.Fatalf("restore failure = %v", err)
	}
	journal, loadErr := LoadHARecoveryState(config.Name)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if journal.Phase != "failed" || !journal.RecoveryAttempted || !journal.RecoverySucceeded {
		t.Fatalf("failed recovery journal = %+v", journal)
	}
	startPrefix := []string{"run", "--detach", "--name", HAContainerName(config.Name, 2)}
	startAttempts := 0
	for _, call := range runner.calls {
		if hasRecoveryPrefix(call, startPrefix) {
			startAttempts++
		}
	}
	if startAttempts != 2 {
		t.Fatalf("member 2 start attempts = %d, want initial failure plus recovery retry; calls=%#v", startAttempts, runner.calls)
	}
	if !runner.serverExists[3] || runner.serverState[3] != "running" {
		t.Fatalf("recovery did not rejoin member 3: exists=%v state=%q", runner.serverExists[3], runner.serverState[3])
	}
}

func TestRecoverFailedHARestoreStartsAllIntactMembersBeforeWaitingForQuorum(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.requireQuorumForReady = true
	for _, member := range config.Members {
		runner.serverState[member.ID] = "stopped"
	}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }
	progress := &haRestoreProgress{mutationStarted: true, peerDataCleared: map[int]bool{}}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	if err := manager.recoverFailedHARestore(ctx, config, t.TempDir(), progress); err != nil {
		t.Fatal(err)
	}
	start1 := []string{"run", "--detach", "--name", HAContainerName(config.Name, 1)}
	start2 := []string{"run", "--detach", "--name", HAContainerName(config.Name, 2)}
	start3 := []string{"run", "--detach", "--name", HAContainerName(config.Name, 3)}
	firstNodeRead := []string{"exec", HAContainerName(config.Name, 1), "kubectl", "get", "node"}
	assertRecoveryCallOrder(t, runner.calls, start1, start2, start3, firstNodeRead)
}

func TestHashHAArtifactHonorsCancellationAndSizeBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hashHAArtifact(canceled, path, "artifact", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hash error = %v", err)
	}
	if _, err := hashHAArtifact(context.Background(), path, "artifact", 9); err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("oversized artifact error = %v", err)
	}
	metadata, err := hashHAArtifact(context.Background(), path, "artifact", 10)
	if err != nil || metadata.Size != 10 || !validSHA256(metadata.SHA256) {
		t.Fatalf("bounded artifact metadata=%+v error=%v", metadata, err)
	}
}

func TestValidateHASnapshotUsesRestoreContextAndDeclaredVolumeBound(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	config.VolumeSize = "64"
	input := writeHARecoveryTestSnapshot(t, config)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := validateHASnapshot(canceled, config, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled validation error = %v", err)
	}

	manifest := readHARecoveryTestManifest(t, input)
	manifest.Snapshot.Size = 65
	writeHARecoveryTestManifest(t, input, manifest)
	if _, _, err := validateHASnapshot(context.Background(), config, input); err == nil || !strings.Contains(err.Error(), "exceeds declared member volume bound 64") {
		t.Fatalf("declared volume bound error = %v", err)
	}
}

func prepareHARecoveryTestConfig(t *testing.T) HAConfig {
	t.Helper()
	setHAConfigHome(t)
	config := liveHAConfig(t)
	config.StartupTimeout = 2 * time.Second
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateFileAtomic(config.TokenFile, []byte(haRecoveryTestToken+"\n")); err != nil {
		t.Fatal(err)
	}
	return config
}

func writeHARecoveryTestSnapshot(t *testing.T, config HAConfig) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(directory, haSnapshotDataFilename)
	tokenPath := filepath.Join(directory, haSnapshotTokenFilename)
	if err := os.WriteFile(snapshotPath, []byte("valid-etcd-snapshot-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte(secureHARecoveryTestToken()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshotMaximum, err := haSnapshotArtifactMaximum(config)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := hashHAArtifact(context.Background(), snapshotPath, haSnapshotDataFilename, snapshotMaximum)
	if err != nil {
		t.Fatal(err)
	}
	token, err := hashHAArtifact(context.Background(), tokenPath, haSnapshotTokenFilename, haRecoveryTokenMaximum)
	if err != nil {
		t.Fatal(err)
	}
	tokenDigest := sha256.Sum256([]byte(haRecoveryTestToken))
	identity, err := haClusterIdentity(config, hex.EncodeToString(tokenDigest[:]))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	manifest := HASnapshotManifest{
		APIVersion:    haSnapshotAPIVersion,
		Kind:          haSnapshotKind,
		FormatVersion: haSnapshotFormatVersion,
		CreatedAt:     now,
		Cluster:       HASnapshotClusterMetadata{Name: config.Name, Identity: identity},
		Topology:      haSnapshotTopology(config),
		Image:         HASnapshotImageMetadata{Reference: config.Image, Architecture: "arm64"},
		K3s: HASnapshotK3sMetadata{
			Version:        DefaultK3sVersion,
			SnapshotName:   "apc-ha-lab-test-1700000000",
			SourceMemberID: 1,
			SourceNodeName: config.Members[0].NodeName,
			CreatedAt:      now,
		},
		Snapshot:    snapshot,
		ServerToken: token,
	}
	writeHARecoveryTestManifest(t, directory, manifest)
	return directory
}

func readHARecoveryTestManifest(t *testing.T, directory string) HASnapshotManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, haSnapshotManifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	var manifest HASnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func writeHARecoveryTestManifest(t *testing.T, directory string, manifest HASnapshotManifest) {
	t.Helper()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, haSnapshotManifestFilename)
	if err := os.Chmod(path, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatal(err)
	}
}

type haRecoveryRunner struct {
	t                       *testing.T
	config                  HAConfig
	calls                   [][]string
	serverExists            map[int]bool
	serverState             map[int]string
	helpers                 map[string]haRecoveryFakeHelper
	readyMembers            int
	guestSnapshotPath       string
	failMemberTwoStartOnce  bool
	networkMissing          bool
	failSnapshotDelete      bool
	failSnapshotStopApplied bool
	failSnapshotRunApplied  bool
	failSnapshotCopy        bool
	failHelperStopApplied   bool
	failHelperDeleteApplied bool
	requireQuorumForReady   bool
	failEtcdTopology        bool
	resetDeadline           time.Time
	resetCollisionAddresses []string
	resetRunCount           int
}

type haRecoveryFakeHelper struct {
	record          haContainerInspect
	backupDirectory string
}

func newHARecoveryRunner(t *testing.T, config HAConfig) *haRecoveryRunner {
	t.Helper()
	runner := &haRecoveryRunner{
		t:                 t,
		config:            config,
		serverExists:      map[int]bool{},
		serverState:       map[int]string{},
		helpers:           map[string]haRecoveryFakeHelper{},
		readyMembers:      haMemberCount,
		guestSnapshotPath: haSnapshotGuestDirectory + "/apc-ha-lab-test-1700000000",
	}
	for _, member := range config.Members {
		runner.serverExists[member.ID] = true
		runner.serverState[member.ID] = "running"
	}
	return runner
}

func (runner *haRecoveryRunner) Run(ctx context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	call := append([]string(nil), arguments...)
	runner.calls = append(runner.calls, call)
	switch {
	case len(call) == 3 && call[0] == "network" && call[1] == "inspect":
		if runner.networkMissing {
			return nil, []byte("network not found"), errors.New("exit 1")
		}
		var record haNetworkInspect
		record.Configuration.Name = runner.config.NetworkName
		record.Configuration.IPv4Subnet = runner.config.Subnet
		record.Configuration.Labels = map[string]string{"apc.dev/managed": "true", "apc.dev/cluster": runner.config.Name}
		return marshalHAInspect(runner.t, record), nil, nil
	case len(call) == 3 && call[0] == "volume" && call[1] == "inspect":
		member := memberForHAVolume(runner.t, runner.config, call[2])
		var record haVolumeInspect
		record.Configuration.Name = call[2]
		record.Configuration.Labels = map[string]string{
			"apc.dev/managed": "true", "apc.dev/cluster": runner.config.Name,
			"apc.dev/role": "server", "apc.dev/member": strconv.Itoa(member.ID),
		}
		record.Configuration.Options = map[string]string{"size": runner.config.VolumeSize}
		return marshalHAInspect(runner.t, record), nil, nil
	case len(call) == 2 && call[0] == "inspect":
		return runner.inspect(call[1])
	case len(call) >= 3 && call[0] == "exec":
		return runner.exec(call)
	case len(call) == 2 && call[0] == "stop":
		if member, ok := runner.serverMember(call[1]); ok {
			runner.serverState[member.ID] = "stopped"
			if member.ID == 1 && runner.failSnapshotStopApplied {
				runner.failSnapshotStopApplied = false
				return nil, []byte("injected applied snapshot stop failure"), errors.New("exit 1")
			}
			return nil, nil, nil
		}
		if _, ok := runner.helpers[call[1]]; ok {
			helper := runner.helpers[call[1]]
			helper.record.Status.State = "stopped"
			runner.helpers[call[1]] = helper
			if runner.failHelperStopApplied {
				runner.failHelperStopApplied = false
				return nil, []byte("injected applied helper stop failure"), errors.New("exit 1")
			}
			return nil, nil, nil
		}
	case len(call) == 2 && call[0] == "delete":
		if member, ok := runner.serverMember(call[1]); ok {
			runner.serverExists[member.ID] = false
			delete(runner.serverState, member.ID)
			return nil, nil, nil
		}
		if _, ok := runner.helpers[call[1]]; ok {
			delete(runner.helpers, call[1])
			if runner.failHelperDeleteApplied {
				runner.failHelperDeleteApplied = false
				return nil, []byte("injected applied helper delete failure"), errors.New("exit 1")
			}
			return nil, nil, nil
		}
	case len(call) >= 4 && call[0] == "run":
		return runner.runContainer(ctx, call)
	}
	runner.t.Fatalf("unexpected HA recovery command: %#v", call)
	return nil, []byte("unexpected command"), errors.New("unexpected command")
}

func (runner *haRecoveryRunner) inspect(name string) ([]byte, []byte, error) {
	if member, ok := runner.serverMember(name); ok {
		if !runner.serverExists[member.ID] {
			return nil, []byte("container not found"), errors.New("exit 1")
		}
		return marshalHAInspect(runner.t, configuredHAContainer(runner.config, member, runner.serverState[member.ID])), nil, nil
	}
	helper, ok := runner.helpers[name]
	if !ok {
		return nil, []byte("container not found"), errors.New("exit 1")
	}
	return marshalHAInspect(runner.t, helper.record), nil, nil
}

func (runner *haRecoveryRunner) exec(call []string) ([]byte, []byte, error) {
	if member, ok := runner.serverMember(call[1]); ok {
		switch {
		case len(call) >= 8 && call[2] == "kubectl" && call[3] == "get" && call[4] == "node":
			quorumReady := true
			if runner.requireQuorumForReady {
				running := 0
				for _, candidate := range runner.config.Members {
					if runner.serverExists[candidate.ID] && runner.serverState[candidate.ID] == "running" {
						running++
					}
				}
				quorumReady = running >= 2
			}
			ready := quorumReady && member.ID <= runner.readyMembers && runner.serverExists[member.ID] && runner.serverState[member.ID] == "running"
			status := "False"
			if ready {
				status = "True"
			}
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":%q}],"nodeInfo":{"kubeletVersion":%q}}}`, member.NodeName, status, DefaultK3sVersion)), nil, nil
		case len(call) >= 5 && call[2] == "/bin/k3s" && call[3] == "etcd-snapshot" && call[4] == "save":
			return []byte("snapshot saved\n"), nil, nil
		case len(call) == 5 && call[2] == "/bin/sh" && call[3] == "-c" && call[4] == haEtcdLocalProbeScript:
			if runner.failEtcdTopology && member.ID == 2 {
				return []byte(`{"health":false}` + "\n" + haEtcdOutputSeparator + "\n"), nil, nil
			}
			return fakeHAEtcdProbeOutput(member.ID), nil, nil
		case len(call) >= 3 && call[2] == "find":
			prefix := call[len(call)-1]
			prefix = strings.TrimSuffix(prefix, "*")
			runner.guestSnapshotPath = haSnapshotGuestDirectory + "/" + prefix + "-" + member.NodeName + "-1700000000"
			return []byte(runner.guestSnapshotPath + "\n"), nil, nil
		case len(call) >= 5 && call[2] == "/bin/k3s" && call[3] == "etcd-snapshot" && call[4] == "delete":
			if runner.failSnapshotDelete {
				return nil, []byte("injected snapshot cleanup failure"), errors.New("exit 1")
			}
			return nil, nil, nil
		case len(call) == 4 && call[2] == "/bin/cat" && call[3] == "/etc/rancher/k3s/k3s.yaml":
			return []byte("apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443\n  name: default\n"), nil, nil
		}
	}
	helper, ok := runner.helpers[call[1]]
	if !ok {
		return nil, []byte("helper not found"), errors.New("exit 1")
	}
	switch {
	case len(call) >= 5 && call[2] == "/bin/cp":
		if runner.failSnapshotCopy {
			runner.failSnapshotCopy = false
			return nil, []byte("injected snapshot copy failure"), errors.New("exit 1")
		}
		destination := filepath.Join(helper.backupDirectory, filepath.Base(call[4]))
		var data []byte
		if filepath.Base(call[4]) == haSnapshotTokenFilename {
			data = []byte(secureHARecoveryTestToken() + "\n")
		} else {
			data = []byte("valid-etcd-snapshot-data")
		}
		if err := os.WriteFile(destination, data, 0o600); err != nil {
			runner.t.Fatal(err)
		}
		return nil, nil, nil
	case len(call) == 3 && call[2] == "/bin/sync":
		return nil, nil, nil
	case len(call) == 5 && call[2] == "/bin/rm":
		return nil, nil, nil
	}
	runner.t.Fatalf("unexpected helper exec for %+v: %#v", helper, call)
	return nil, []byte("unexpected helper exec"), errors.New("unexpected helper exec")
}

func (runner *haRecoveryRunner) runContainer(ctx context.Context, call []string) ([]byte, []byte, error) {
	name := valueAfterRecoveryArgument(call, "--name")
	if member, ok := runner.serverMember(name); ok {
		if member.ID == 2 && runner.failMemberTwoStartOnce {
			runner.failMemberTwoStartOnce = false
			return nil, []byte("injected member 2 start failure"), errors.New("exit 1")
		}
		runner.serverExists[member.ID] = true
		runner.serverState[member.ID] = "running"
		return nil, nil, nil
	}
	switch name {
	case HASnapshotHelperContainerName(runner.config.Name), HARestoreResetContainerName(runner.config.Name), HARestoreClearContainerName(runner.config.Name, 2), HARestoreClearContainerName(runner.config.Name, 3):
	default:
		runner.t.Fatalf("unexpected container run name %q: %#v", name, call)
	}
	helper := recoveryFakeHelperFromRun(runner.t, call)
	runner.helpers[name] = helper
	if name == HASnapshotHelperContainerName(runner.config.Name) && runner.failSnapshotRunApplied {
		runner.failSnapshotRunApplied = false
		return nil, []byte("injected applied snapshot helper failure"), errors.New("exit 1")
	}
	if name == HARestoreResetContainerName(runner.config.Name) {
		if deadline, ok := ctx.Deadline(); ok {
			runner.resetDeadline = deadline
		}
		runner.resetRunCount++
		if runner.resetRunCount <= len(runner.resetCollisionAddresses) {
			address := runner.resetCollisionAddresses[runner.resetRunCount-1]
			return nil, []byte(haRuntimeIPCollisionMarker + address + "\n"), errors.New("exit 78")
		}
	}
	return nil, nil, nil
}

func (runner *haRecoveryRunner) serverMember(name string) (HAMember, bool) {
	for _, member := range runner.config.Members {
		if HAContainerName(runner.config.Name, member.ID) == name {
			return member, true
		}
	}
	return HAMember{}, false
}

func recoveryFakeHelperFromRun(t *testing.T, arguments []string) haRecoveryFakeHelper {
	t.Helper()
	if len(arguments) < 2 || arguments[0] != "run" {
		t.Fatalf("invalid helper run arguments: %#v", arguments)
	}
	values := map[string][]string{}
	detached := false
	imageIndex := -1
	for index := 1; index < len(arguments); {
		argument := arguments[index]
		if !strings.HasPrefix(argument, "--") {
			imageIndex = index
			break
		}
		if argument == "--detach" {
			detached = true
			index++
			continue
		}
		switch argument {
		case "--name", "--arch", "--cpus", "--memory", "--volume", "--mount", "--label", "--progress", "--cap-add", "--cap-drop", "--network", "--entrypoint":
			if index+1 >= len(arguments) {
				t.Fatalf("helper flag %s has no value: %#v", argument, arguments)
			}
			values[argument] = append(values[argument], arguments[index+1])
			index += 2
		default:
			t.Fatalf("unexpected helper run flag %q: %#v", argument, arguments)
		}
	}
	if imageIndex < 0 {
		t.Fatalf("helper run has no image: %#v", arguments)
	}

	var record haContainerInspect
	record.Configuration.Platform.OS = "linux"
	if len(values["--arch"]) == 1 {
		record.Configuration.Platform.Architecture = values["--arch"][0]
	}
	record.Configuration.InitProcess.User.ID = &haContainerUserID{}
	record.Configuration.CapAdd = append([]string(nil), values["--cap-add"]...)
	record.Configuration.CapDrop = append([]string(nil), values["--cap-drop"]...)
	record.Configuration.Labels = map[string]string{}
	for _, label := range values["--label"] {
		parts := strings.SplitN(label, "=", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid helper label %q", label)
		}
		record.Configuration.Labels[parts[0]] = parts[1]
	}
	record.Configuration.Image.Reference = arguments[imageIndex]
	commandArguments := append([]string(nil), arguments[imageIndex+1:]...)
	if entrypoints := values["--entrypoint"]; len(entrypoints) == 1 {
		record.Configuration.InitProcess.Executable = entrypoints[0]
		record.Configuration.InitProcess.Arguments = commandArguments
	} else {
		if len(commandArguments) == 0 {
			t.Fatalf("helper run arguments contain no executable: %#v", arguments)
		}
		record.Configuration.InitProcess.Executable = commandArguments[0]
		record.Configuration.InitProcess.Arguments = commandArguments[1:]
	}
	if len(values["--cpus"]) != 1 || len(values["--memory"]) != 1 {
		t.Fatalf("helper must declare one CPU and memory value: %#v", arguments)
	}
	cpus, err := strconv.Atoi(values["--cpus"][0])
	if err != nil {
		t.Fatal(err)
	}
	record.Configuration.Resources.CPUs = cpus
	memoryBytes, err := parseHAByteSize(values["--memory"][0])
	if err != nil {
		t.Fatal(err)
	}
	record.Configuration.Resources.MemoryInBytes = int64(memoryBytes)
	mountCount := len(values["--volume"]) + len(values["--mount"])
	record.Configuration.Mounts = make([]struct {
		Destination string   `json:"destination"`
		Source      string   `json:"source"`
		Options     []string `json:"options"`
		Type        struct {
			Volume *struct {
				Name string `json:"name"`
			} `json:"volume"`
			VirtioFS *struct{} `json:"virtiofs"`
		} `json:"type"`
	}, mountCount)
	mountIndex := 0
	for _, volume := range values["--volume"] {
		parts := strings.SplitN(volume, ":", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid helper volume %q", volume)
		}
		record.Configuration.Mounts[mountIndex].Destination = parts[1]
		record.Configuration.Mounts[mountIndex].Type.Volume = &struct {
			Name string `json:"name"`
		}{Name: parts[0]}
		mountIndex++
	}
	backupDirectory := ""
	for _, mount := range values["--mount"] {
		fields := strings.Split(mount, ",")
		source, target := "", ""
		readOnly := false
		for _, field := range fields {
			switch {
			case strings.HasPrefix(field, "source="):
				source = strings.TrimPrefix(field, "source=")
			case strings.HasPrefix(field, "target="):
				target = strings.TrimPrefix(field, "target=")
			case field == "readonly":
				readOnly = true
			}
		}
		record.Configuration.Mounts[mountIndex].Destination = target
		record.Configuration.Mounts[mountIndex].Source = source
		record.Configuration.Mounts[mountIndex].Type.VirtioFS = &struct{}{}
		if readOnly {
			record.Configuration.Mounts[mountIndex].Options = []string{"ro"}
		}
		if target == haRecoveryBackupMount {
			backupDirectory = source
		}
		mountIndex++
	}
	if len(values["--network"]) > 0 {
		record.Configuration.Networks = make([]struct {
			Network string `json:"network"`
			Options struct {
				MACAddress string `json:"macAddress"`
				MTU        int    `json:"mtu"`
			} `json:"options"`
		}, len(values["--network"]))
		for index, network := range values["--network"] {
			fields := strings.Split(network, ",")
			record.Configuration.Networks[index].Network = fields[0]
			for _, field := range fields[1:] {
				switch {
				case strings.HasPrefix(field, "mac="):
					record.Configuration.Networks[index].Options.MACAddress = strings.TrimPrefix(field, "mac=")
				case strings.HasPrefix(field, "mtu="):
					mtu, err := strconv.Atoi(strings.TrimPrefix(field, "mtu="))
					if err != nil {
						t.Fatal(err)
					}
					record.Configuration.Networks[index].Options.MTU = mtu
				}
			}
		}
	}
	record.Status.State = "stopped"
	if detached {
		record.Status.State = "running"
	}
	return haRecoveryFakeHelper{record: record, backupDirectory: backupDirectory}
}

func valueAfterRecoveryArgument(arguments []string, flag string) string {
	for index := 0; index+1 < len(arguments); index++ {
		if arguments[index] == flag {
			return arguments[index+1]
		}
	}
	return ""
}

func removeRecoveryFlagValue(arguments []string, flag string) []string {
	result := append([]string(nil), arguments...)
	for index := 0; index+1 < len(result); index++ {
		if result[index] == flag {
			return append(result[:index], result[index+2:]...)
		}
	}
	return result
}

func appendRecoveryFlagValue(arguments []string, flag, value string) []string {
	for index := 0; index+1 < len(arguments); index++ {
		if arguments[index] != flag {
			continue
		}
		result := make([]string, 0, len(arguments)+2)
		result = append(result, arguments[:index+2]...)
		result = append(result, flag, value)
		return append(result, arguments[index+2:]...)
	}
	return append(append([]string(nil), arguments...), flag, value)
}

func assertRecoveryCallOrder(t *testing.T, calls [][]string, prefixes ...[]string) {
	t.Helper()
	position := -1
	for _, prefix := range prefixes {
		found := -1
		for index := position + 1; index < len(calls); index++ {
			if hasRecoveryPrefix(calls[index], prefix) {
				found = index
				break
			}
		}
		if found < 0 {
			t.Fatalf("call prefix %#v not found after index %d; calls=%#v", prefix, position, calls)
		}
		position = found
	}
}

func findRecoveryCall(t *testing.T, calls [][]string, prefix []string) []string {
	t.Helper()
	for _, call := range calls {
		if hasRecoveryPrefix(call, prefix) {
			return call
		}
	}
	t.Fatalf("call prefix %#v not found", prefix)
	return nil
}

func hasRecoveryPrefix(call, prefix []string) bool {
	return len(call) >= len(prefix) && reflect.DeepEqual(call[:len(prefix)], prefix)
}

func isHARecoveryMutation(call []string) bool {
	if len(call) == 0 {
		return false
	}
	if call[0] == "run" || call[0] == "stop" || call[0] == "delete" {
		return true
	}
	return len(call) >= 5 && call[0] == "exec" && call[2] == "/bin/k3s" && call[3] == "etcd-snapshot" && call[4] == "save"
}

func secureHARecoveryTestToken() string {
	return "K10" + strings.Repeat("a", sha256.Size*2) + "::server:" + haRecoveryTestToken
}
