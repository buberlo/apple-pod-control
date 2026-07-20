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
	"os/exec"
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
		{
			name: "overlapping volume provenance",
			mutate: func(state *HARecoveryState) {
				state.VolumeProvenance = &HARecoveryVolumeProvenance{
					PreexistingMemberIDs: []int{1, 2},
					NewMemberIDs:         []int{2, 3},
				}
			},
			want: "invalid member-volume provenance",
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
	if result.Path != resolvedOutput || result.Bytes <= 0 || !validSHA256(result.DataSHA256) || !validSHA256(result.ManifestSHA256) || result.CreatedAt.IsZero() || result.Manifest.Cluster.Name != config.Name {
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
	manifestDigest := sha256.Sum256(manifestData)
	if result.ManifestSHA256 != hex.EncodeToString(manifestDigest[:]) {
		t.Fatalf("manifest fingerprint = %q, want exact published manifest digest", result.ManifestSHA256)
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

func TestRecoverHARejectsWrongManifestFingerprintWithoutStateOrRuntimeMutation(t *testing.T) {
	config, input, fingerprint := prepareEmptyHARecoveryTest(t)
	runner := newEmptyHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	wrong := strings.Repeat("0", sha256.Size*2)
	if wrong == fingerprint {
		wrong = strings.Repeat("1", sha256.Size*2)
	}

	_, err := manager.RecoverHA(context.Background(), config.Name, input, wrong, 3*time.Minute)
	if err == nil || !strings.Contains(err.Error(), "fingerprint does not match") {
		t.Fatalf("wrong fingerprint recovery error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("wrong fingerprint reached runtime inspection or mutation: %#v", runner.calls)
	}
	assertHARecoveryFilesAbsent(t, config, true)
}

func TestRecoverHARejectsMutableImageWithoutStateOrRuntimeMutation(t *testing.T) {
	config, input, _ := prepareEmptyHARecoveryTest(t)
	manifest := readHARecoveryTestManifest(t, input)
	manifest.Image.Reference = "docker.io/rancher/k3s:latest"
	writeHARecoveryTestManifest(t, input, manifest)
	fingerprint := haRecoveryTestManifestFingerprint(t, input)
	runner := newEmptyHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner

	_, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	if err == nil || !strings.Contains(err.Error(), "immutable OCI sha256 digest") {
		t.Fatalf("mutable-image recovery error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("mutable-image recovery reached runtime inspection or mutation: %#v", runner.calls)
	}
	assertHARecoveryFilesAbsent(t, config, true)
}

func TestRecoverHARejectsSavedTopologyMismatchBeforeRuntimeMutation(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	fingerprint := haRecoveryTestManifestFingerprint(t, input)
	altered := config
	altered.CPUs++
	if err := saveHAConfig(altered); err != nil {
		t.Fatal(err)
	}
	runner := newHARecoveryRunner(t, altered)
	manager := NewManager("container")
	manager.runner = runner

	_, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	if err == nil || !strings.Contains(err.Error(), "topology does not match") {
		t.Fatalf("saved-topology mismatch recovery error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("saved-topology mismatch reached runtime inspection or mutation: %#v", runner.calls)
	}
	recoveryPath, pathErr := HARecoveryStatePath(config.Name)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if _, statErr := os.Lstat(recoveryPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("saved-topology mismatch published recovery journal: %v", statErr)
	}
}

func TestRecoverHAOnEmptyHostReconstructsExactStateAndRestores(t *testing.T) {
	config, input, fingerprint := prepareEmptyHARecoveryTest(t)
	stagedPath, err := haRestoreStagingPath(config)
	if err != nil {
		t.Fatal(err)
	}
	runner := newEmptyHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	if _, statErr := os.Lstat(config.KubeconfigPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("empty-host recovery unexpectedly started with a kubeconfig: %v", statErr)
	}
	apiProbeCount := 0
	var apiProbePreconditionErr error
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, _ HAMember) bool {
		apiProbeCount++
		if apiProbePreconditionErr != nil {
			return true
		}
		info, statErr := os.Lstat(config.KubeconfigPath)
		if statErr != nil {
			apiProbePreconditionErr = fmt.Errorf("final HA API probe ran before recovered kubeconfig publication: %w", statErr)
			return true
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			apiProbePreconditionErr = fmt.Errorf("recovered kubeconfig mode before final HA API probe = %v/%o, want regular/0600", info.Mode(), info.Mode().Perm())
			return true
		}
		data, readErr := os.ReadFile(config.KubeconfigPath)
		if readErr != nil {
			apiProbePreconditionErr = fmt.Errorf("read recovered kubeconfig before final HA API probe: %w", readErr)
			return true
		}
		expectedEndpoint := config.Members[0].apiEndpoint(config.ListenAddress)
		if !strings.Contains(string(data), "server: "+expectedEndpoint) {
			apiProbePreconditionErr = fmt.Errorf("recovered kubeconfig was not rewritten to the seed API endpoint before final HA API probe")
		}
		return true
	}

	state, err := manager.RecoverHA(context.Background(), config.Name, input, "sha256:"+fingerprint, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Healthy || state.ReadyMembers != haMemberCount {
		t.Fatalf("empty-host recovered state = %+v", state)
	}
	if apiProbePreconditionErr != nil {
		t.Fatal(apiProbePreconditionErr)
	}
	if apiProbeCount != haMemberCount {
		t.Fatalf("final empty-host HA API probes = %d, want %d after kubeconfig publication", apiProbeCount, haMemberCount)
	}
	stored, err := loadHAConfig(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(haSnapshotTopology(stored), haSnapshotTopology(config)) || stored.Image != config.Image {
		t.Fatalf("reconstructed config = %+v, want exact snapshot topology", stored)
	}
	configPath, err := HAConfigPath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	configInfo, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	tokenInfo, err := os.Stat(stored.TokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if configInfo.Mode().Perm() != 0o600 || tokenInfo.Mode().Perm() != 0o600 {
		t.Fatalf("recovered private state modes config=%o token=%o, want 0600", configInfo.Mode().Perm(), tokenInfo.Mode().Perm())
	}
	installedToken, err := readHARecoveryToken(stored.TokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if !sameK3sServerToken(installedToken, []byte(secureHARecoveryTestToken())) {
		t.Fatal("empty-host recovery did not install the packaged K3s credential")
	}
	journal, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "completed" || !journal.RecoverySucceeded || journal.ClusterIdentity == "" {
		t.Fatalf("empty-host recovery journal = %+v", journal)
	}
	assertRecoveryCallOrder(t, runner.calls,
		[]string{"network", "create"},
		[]string{"volume", "create"},
		[]string{"run", "--name", HARestoreResetContainerName(config.Name)},
		[]string{"run", "--detach", "--name", HAContainerName(config.Name, 1)},
	)
	resetCall := findRecoveryCall(t, runner.calls, []string{"run", "--name", HARestoreResetContainerName(config.Name)})
	wantMount := fmt.Sprintf("type=bind,source=%s,target=%s,readonly", stagedPath, haRecoveryBackupMount)
	if got := valueAfterRecoveryArgument(resetCall, "--mount"); got != wantMount {
		t.Fatalf("empty-host restore backup mount = %q, want protected staging mount %q", got, wantMount)
	}
	if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("empty-host restore staging package remains after success: %v", statErr)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, "")
}

func TestRecoverHAEmptyHostPreResetFailureNeverStartsNewVolumes(t *testing.T) {
	config, input, fingerprint := prepareEmptyHARecoveryTest(t)
	runner := newEmptyHARecoveryRunner(t, config)
	// Empty-host RecoverHA inspects member 1 once in each preflight. The third
	// inspection is deleteStoppedHAMemberForRecovery: mutationStarted is true,
	// but resetAttempted is deliberately still false.
	runner.failSeedInspectAttempt = 3
	manager := NewManager("container")
	manager.runner = runner
	apiProbeCount := 0
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool {
		apiProbeCount++
		return true
	}

	_, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	assertEmptyHostPreResetFailure(t, config, runner, err, apiProbeCount)

	// The volume provenance is durable. A retry sees the already-created
	// volumes but must still refuse the intact-cluster rollback if it fails in
	// the same pre-reset window. The first rollback's protective end-state
	// inspection accounts for the additional seed inspection.
	runner.failSeedInspectAttempt = 7
	apiProbeCount = 0
	_, retryErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	assertEmptyHostPreResetFailure(t, config, runner, retryErr, apiProbeCount)
}

func TestRestoreHARejectsFailedEmptyHostRecoveryBeforeRuntimeMutation(t *testing.T) {
	config, input, fingerprint := prepareEmptyHARecoveryTest(t)
	runner := newEmptyHARecoveryRunner(t, config)
	runner.failSeedInspectAttempt = 3
	manager := NewManager("container")
	manager.runner = runner
	apiProbeCount := 0
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool {
		apiProbeCount++
		return true
	}

	_, recoveryErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	assertEmptyHostPreResetFailure(t, config, runner, recoveryErr, apiProbeCount)
	journalBefore, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	runtimeCallsBefore := len(runner.calls)

	_, restoreErr := manager.RestoreHA(context.Background(), config.Name, input, 3*time.Minute)
	if restoreErr == nil ||
		!strings.Contains(restoreErr.Error(), "protected nonterminal recovery journal") ||
		!strings.Contains(restoreErr.Error(), "apc cluster ha recover") ||
		!strings.Contains(restoreErr.Error(), "independently retained manifest SHA-256") {
		t.Fatalf("direct restore after failed empty-host recovery error = %v", restoreErr)
	}
	if len(runner.calls) != runtimeCallsBefore {
		t.Fatalf("rejected direct restore made runtime calls: before=%d after=%d calls=%#v", runtimeCallsBefore, len(runner.calls), runner.calls[runtimeCallsBefore:])
	}
	if apiProbeCount != 0 {
		t.Fatalf("rejected direct restore made %d API probes", apiProbeCount)
	}
	journalAfter, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(journalAfter, journalBefore) {
		t.Fatalf("rejected direct restore changed recovery journal: before=%+v after=%+v", journalBefore, journalAfter)
	}
	for _, member := range config.Members {
		if runner.serverExists[member.ID] {
			t.Fatalf("rejected direct restore started new-volume member %d", member.ID)
		}
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredStopped {
		t.Fatalf("rejected direct restore desired state = %+v, want Stopped", desired)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, restoreErr.Error())
}

func TestRecoverHARetryMonotonicallyMarksNewlyMissingVolumeUnsafeBeforeCreate(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	fingerprint := haRecoveryTestManifestFingerprint(t, input)
	header, err := readHASnapshotManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := saveHARecoveryState(HARecoveryState{
		APIVersion:       haSnapshotAPIVersion,
		Kind:             haRecoveryStateKind,
		Cluster:          config.Name,
		ClusterIdentity:  header.Manifest.Cluster.Identity,
		SnapshotPath:     header.Path,
		Phase:            "validated",
		StartedAt:        now,
		UpdatedAt:        now,
		VolumeProvenance: &HARecoveryVolumeProvenance{PreexistingMemberIDs: []int{1, 2, 3}},
	}); err != nil {
		t.Fatal(err)
	}

	runner := newHARecoveryRunner(t, config)
	for _, member := range config.Members {
		runner.serverExists[member.ID] = false
		delete(runner.serverState, member.ID)
	}
	// Member 1 was preexisting in the prior journal but disappeared before
	// this retry. It must be monotonically demoted to new/unsafe.
	runner.volumeMissing[1] = true
	runner.failSeedInspectAttempt = 3
	provenanceObservedBeforeCreate := false
	runner.beforeVolumeCreate = func(memberID int) {
		if memberID != 1 {
			t.Fatalf("unexpected retry volume creation for member %d", memberID)
		}
		journal, loadErr := LoadHARecoveryState(config.Name)
		if loadErr != nil {
			t.Fatalf("load provenance before volume mutation: %v", loadErr)
		}
		want := &HARecoveryVolumeProvenance{PreexistingMemberIDs: []int{2, 3}, NewMemberIDs: []int{1}}
		if !reflect.DeepEqual(journal.VolumeProvenance, want) {
			t.Fatalf("provenance before volume create = %+v, want %+v", journal.VolumeProvenance, want)
		}
		provenanceObservedBeforeCreate = true
	}
	manager := NewManager("container")
	manager.runner = runner
	apiProbeCount := 0
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool {
		apiProbeCount++
		return true
	}

	_, recoveryErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "injected pre-reset seed inspect failure") || !strings.Contains(recoveryErr.Error(), "newly created empty HA member volumes [1]") {
		t.Fatalf("retry pre-reset failure = %v", recoveryErr)
	}
	if !provenanceObservedBeforeCreate {
		t.Fatal("updated unsafe provenance was not durably observed before volume creation")
	}
	for _, member := range config.Members {
		if runner.serverExists[member.ID] {
			t.Fatalf("retry started member %d after recreating an empty volume", member.ID)
		}
	}
	if apiProbeCount != 0 {
		t.Fatalf("unsafe retry made %d API probes", apiProbeCount)
	}
	journal, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	wantProvenance := &HARecoveryVolumeProvenance{PreexistingMemberIDs: []int{2, 3}, NewMemberIDs: []int{1}}
	if journal.Phase != "failed" || !journal.RecoveryAttempted || journal.RecoverySucceeded || !reflect.DeepEqual(journal.VolumeProvenance, wantProvenance) {
		t.Fatalf("unsafe retry journal = %+v, want provenance %+v", journal, wantProvenance)
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredStopped {
		t.Fatalf("unsafe retry desired state = %+v, want Stopped", desired)
	}
	if supervisionErr := ensureHARecoveryJournalAllowsSupervision(config.Name); supervisionErr == nil {
		t.Fatal("unsafe retry released lifecycle supervision")
	}
	assertNoHARecoveryTestSecret(t, runner.calls, recoveryErr.Error())
}

func TestRecoverHARetryPersistsMissingVolumeProvenanceBeforeRejectingInconsistentEnvelopes(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	fingerprint := haRecoveryTestManifestFingerprint(t, input)
	header, err := readHASnapshotManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := saveHARecoveryState(HARecoveryState{
		APIVersion:       haSnapshotAPIVersion,
		Kind:             haRecoveryStateKind,
		Cluster:          config.Name,
		ClusterIdentity:  header.Manifest.Cluster.Identity,
		SnapshotPath:     header.Path,
		Phase:            "validated",
		StartedAt:        now,
		UpdatedAt:        now,
		VolumeProvenance: &HARecoveryVolumeProvenance{PreexistingMemberIDs: []int{1, 2, 3}},
	}); err != nil {
		t.Fatal(err)
	}

	runner := newHARecoveryRunner(t, config)
	runner.volumeMissing[1] = true
	manager := NewManager("container")
	manager.runner = runner
	apiProbeCount := 0
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool {
		apiProbeCount++
		return true
	}

	_, recoveryErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "server envelopes exist without the complete exact network and member volumes") {
		t.Fatalf("inconsistent retry error = %v", recoveryErr)
	}
	wantProvenance := &HARecoveryVolumeProvenance{PreexistingMemberIDs: []int{2, 3}, NewMemberIDs: []int{1}}
	journal, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(journal.VolumeProvenance, wantProvenance) {
		t.Fatalf("provenance after inconsistent retry = %+v, want %+v", journal.VolumeProvenance, wantProvenance)
	}

	// Even if the missing volume is recreated outside APC before the next
	// retry, the durable demotion must prevent it from being treated as intact.
	runner.volumeMissing[1] = false
	runner.failSeedInspectAttempt = 4
	_, retryErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	if retryErr == nil || !strings.Contains(retryErr.Error(), "injected pre-reset seed inspect failure") || !strings.Contains(retryErr.Error(), "newly created empty HA member volumes [1]") {
		t.Fatalf("retry after external volume recreation = %v", retryErr)
	}
	for _, member := range config.Members {
		if !runner.serverExists[member.ID] || runner.serverState[member.ID] != "stopped" {
			t.Fatalf("member %d final state = exists=%v state=%q, want existing/stopped", member.ID, runner.serverExists[member.ID], runner.serverState[member.ID])
		}
	}
	if apiProbeCount != 0 {
		t.Fatalf("unsafe retry made %d API probes", apiProbeCount)
	}
	journal, err = LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "failed" || !journal.RecoveryAttempted || journal.RecoverySucceeded || !reflect.DeepEqual(journal.VolumeProvenance, wantProvenance) {
		t.Fatalf("unsafe recreated-volume retry journal = %+v", journal)
	}
	if supervisionErr := ensureHARecoveryJournalAllowsSupervision(config.Name); supervisionErr == nil {
		t.Fatal("unsafe recreated-volume retry released lifecycle supervision")
	}
	assertNoHARecoveryTestSecret(t, runner.calls, retryErr.Error())
}

func TestRecoverHARetryPreResetStopFailureProtectivelyStopsNewVolumeMembers(t *testing.T) {
	tests := []struct {
		name                    string
		noopServerStopMember    int
		wantRunningMember       int
		wantProtectiveStopError string
	}{
		{
			name: "transient initial stop failure converges every member",
		},
		{
			name:                    "successful no-op stop is rejected by final verification",
			noopServerStopMember:    2,
			wantRunningMember:       2,
			wantProtectiveStopError: `HA recovery member 2 remains in runtime state "running" after protective stop`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := prepareHARecoveryTestConfig(t)
			input := writeHARecoveryTestSnapshot(t, config)
			fingerprint := haRecoveryTestManifestFingerprint(t, input)
			header, err := readHASnapshotManifest(input)
			if err != nil {
				t.Fatal(err)
			}
			if err := markHAClusterStoppedLocked(config.Name); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			if err := saveHARecoveryState(HARecoveryState{
				APIVersion:        haSnapshotAPIVersion,
				Kind:              haRecoveryStateKind,
				Cluster:           config.Name,
				ClusterIdentity:   header.Manifest.Cluster.Identity,
				SnapshotPath:      header.Path,
				Phase:             "failed",
				StartedAt:         now,
				UpdatedAt:         now,
				RecoveryAttempted: true,
				VolumeProvenance:  &HARecoveryVolumeProvenance{NewMemberIDs: []int{1, 2, 3}},
			}); err != nil {
				t.Fatal(err)
			}

			runner := newHARecoveryRunner(t, config)
			runner.failRestoreStopMemberOnce = 3
			runner.noopServerStopMember = test.noopServerStopMember
			manager := NewManager("container")
			manager.runner = runner
			apiProbeCount := 0
			manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool {
				apiProbeCount++
				return true
			}

			_, recoveryErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
			if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "injected unapplied HA restore stop failure") || !strings.Contains(recoveryErr.Error(), "newly created empty HA member volumes [1 2 3]") {
				t.Fatalf("retry pre-reset stop failure = %v", recoveryErr)
			}
			if test.wantProtectiveStopError != "" && !strings.Contains(recoveryErr.Error(), test.wantProtectiveStopError) {
				t.Fatalf("retry lacks protective stop verification error %q: %v", test.wantProtectiveStopError, recoveryErr)
			}
			for _, member := range config.Members {
				wantState := "stopped"
				if member.ID == test.wantRunningMember {
					wantState = "running"
				}
				if !runner.serverExists[member.ID] || runner.serverState[member.ID] != wantState {
					t.Fatalf("member %d final state = exists=%v state=%q, want existing/%s", member.ID, runner.serverExists[member.ID], runner.serverState[member.ID], wantState)
				}
			}
			if apiProbeCount != 0 {
				t.Fatalf("unsafe retry made %d API probes", apiProbeCount)
			}
			journal, err := LoadHARecoveryState(config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if journal.Phase != "failed" || !journal.RecoveryAttempted || journal.RecoverySucceeded {
				t.Fatalf("unsafe retry journal = %+v", journal)
			}
			desired, err := loadHADesiredState(config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if desired.ClusterState != haDesiredStopped {
				t.Fatalf("unsafe retry desired state = %+v, want Stopped", desired)
			}
			if supervisionErr := ensureHARecoveryJournalAllowsSupervision(config.Name); supervisionErr == nil {
				t.Fatal("unsafe retry released lifecycle supervision")
			}
			assertNoHARecoveryTestSecret(t, runner.calls, recoveryErr.Error())
		})
	}
}

func TestRestoreHAIntactVolumesStillRecoverPreResetFailure(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	runner := newHARecoveryRunner(t, config)
	// RestoreHA has one preflight; the second seed inspection is the same
	// mutationStarted/resetAttempted failure window used by the empty-host test.
	runner.failSeedInspectAttempt = 2
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	_, err := manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "injected pre-reset seed inspect failure") {
		t.Fatalf("intact-volume pre-reset failure = %v", err)
	}
	journal, loadErr := LoadHARecoveryState(config.Name)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if journal.Phase != "failed" || !journal.RecoveryAttempted || !journal.RecoverySucceeded {
		t.Fatalf("intact-volume recovery journal = %+v", journal)
	}
	for _, member := range config.Members {
		if !runner.serverExists[member.ID] || runner.serverState[member.ID] != "running" {
			t.Fatalf("intact member %d was not recovered: exists=%v state=%q", member.ID, runner.serverExists[member.ID], runner.serverState[member.ID])
		}
	}
	desired, desiredErr := loadHADesiredState(config.Name)
	if desiredErr != nil {
		t.Fatal(desiredErr)
	}
	if desired.ClusterState != haDesiredRunning {
		t.Fatalf("intact-volume desired state = %+v, want Running", desired)
	}
	if supervisionErr := ensureHARecoveryJournalAllowsSupervision(config.Name); supervisionErr != nil {
		t.Fatalf("successful intact-volume rollback did not release lifecycle: %v", supervisionErr)
	}
}

func assertEmptyHostPreResetFailure(t *testing.T, config HAConfig, runner *haRecoveryRunner, recoveryErr error, apiProbeCount int) {
	t.Helper()
	if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "injected pre-reset seed inspect failure") || !strings.Contains(recoveryErr.Error(), "newly created empty HA member volumes") {
		t.Fatalf("empty-host pre-reset failure = %v", recoveryErr)
	}
	for _, member := range config.Members {
		if runner.serverExists[member.ID] {
			t.Fatalf("empty member %d was started as a server after pre-reset failure", member.ID)
		}
	}
	if apiProbeCount != 0 {
		t.Fatalf("empty-host pre-reset failure made %d API probes", apiProbeCount)
	}
	if _, statErr := os.Lstat(config.KubeconfigPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("empty-host pre-reset failure published a kubeconfig: %v", statErr)
	}
	journal, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "failed" || !journal.RecoveryAttempted || journal.RecoverySucceeded {
		t.Fatalf("empty-host pre-reset journal = %+v", journal)
	}
	wantNew := []int{1, 2, 3}
	if journal.VolumeProvenance == nil || len(journal.VolumeProvenance.PreexistingMemberIDs) != 0 || !reflect.DeepEqual(journal.VolumeProvenance.NewMemberIDs, wantNew) {
		t.Fatalf("empty-host volume provenance = %+v, want new members %v", journal.VolumeProvenance, wantNew)
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredStopped {
		t.Fatalf("empty-host pre-reset desired state = %+v, want Stopped", desired)
	}
	if supervisionErr := ensureHARecoveryJournalAllowsSupervision(config.Name); supervisionErr == nil {
		t.Fatal("failed empty-host recovery released lifecycle supervision")
	}
	assertNoHARecoveryTestSecret(t, runner.calls, recoveryErr.Error())
}

func TestRecoverHAInstallsMissingTokenButNeverOverwritesMismatch(t *testing.T) {
	t.Run("missing token", func(t *testing.T) {
		config := prepareHARecoveryTestConfig(t)
		input := writeHARecoveryTestSnapshot(t, config)
		fingerprint := haRecoveryTestManifestFingerprint(t, input)
		if err := os.Remove(config.TokenFile); err != nil {
			t.Fatal(err)
		}
		runner := newHARecoveryRunner(t, config)
		manager := NewManager("container")
		manager.runner = runner
		manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

		if _, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute); err != nil {
			t.Fatal(err)
		}
		installed, err := readHARecoveryToken(config.TokenFile)
		if err != nil {
			t.Fatal(err)
		}
		if !sameK3sServerToken(installed, []byte(secureHARecoveryTestToken())) {
			t.Fatal("missing on-host token was not restored from trusted package")
		}
		assertNoHARecoveryTestSecret(t, runner.calls, "")
	})

	t.Run("mismatched token", func(t *testing.T) {
		config := prepareHARecoveryTestConfig(t)
		input := writeHARecoveryTestSnapshot(t, config)
		fingerprint := haRecoveryTestManifestFingerprint(t, input)
		const different = "different-local-token"
		if err := writePrivateFileAtomic(config.TokenFile, []byte(different+"\n")); err != nil {
			t.Fatal(err)
		}
		runner := newHARecoveryRunner(t, config)
		manager := NewManager("container")
		manager.runner = runner

		_, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
		if err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
			t.Fatalf("mismatched token recovery error = %v", err)
		}
		current, readErr := os.ReadFile(config.TokenFile)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.TrimSpace(string(current)) != different {
			t.Fatal("mismatched existing token was overwritten")
		}
		assertNoHARecoveryTestSecret(t, runner.calls, err.Error())
		recoveryPath, pathErr := HARecoveryStatePath(config.Name)
		if pathErr != nil {
			t.Fatal(pathErr)
		}
		if _, statErr := os.Lstat(recoveryPath); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("token mismatch published recovery journal: %v", statErr)
		}
	})
}

func TestRecoverHARefusesForeignRuntimeResourcesBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*haRecoveryRunner)
		want      string
	}{
		{name: "foreign network", configure: func(runner *haRecoveryRunner) { runner.foreignNetwork = true }, want: "not owned"},
		{name: "foreign volume", configure: func(runner *haRecoveryRunner) { runner.foreignVolumeMember = 2 }, want: "not the expected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := prepareHARecoveryTestConfig(t)
			input := writeHARecoveryTestSnapshot(t, config)
			fingerprint := haRecoveryTestManifestFingerprint(t, input)
			runner := newHARecoveryRunner(t, config)
			test.configure(runner)
			manager := NewManager("container")
			manager.runner = runner

			_, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("foreign resource recovery error = %v, want %q", err, test.want)
			}
			for _, call := range runner.calls {
				if isHARecoveryMutation(call) || (len(call) >= 2 && (call[0] == "network" || call[0] == "volume") && call[1] == "create") {
					t.Fatalf("foreign resource was mutated: %#v", call)
				}
			}
			assertNoHARecoveryTestSecret(t, runner.calls, err.Error())
		})
	}
}

func TestRecoverHARetriesExactPartialInfrastructureUnderBlockingJournal(t *testing.T) {
	config, input, fingerprint := prepareEmptyHARecoveryTest(t)
	// Simulate an interruption after the fail-closed Stopped intent was made
	// durable but before the recovery journal could be published.
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	runner := newEmptyHARecoveryRunner(t, config)
	runner.failVolumeCreateMember = 2
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	_, firstErr := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute)
	if firstErr == nil || !strings.Contains(firstErr.Error(), "injected applied volume create failure") {
		t.Fatalf("interrupted infrastructure recovery error = %v", firstErr)
	}
	journal, err := LoadHARecoveryState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if journal.Phase != "validated" || journal.RecoverySucceeded {
		t.Fatalf("interrupted infrastructure journal = %+v", journal)
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredStopped {
		t.Fatalf("interrupted recovery desired state = %+v", desired)
	}
	if supervisionErr := ensureHARecoveryJournalAllowsSupervision(config.Name); supervisionErr == nil || !strings.Contains(supervisionErr.Error(), "nonterminal phase") {
		t.Fatalf("preparation journal did not block supervisor: %v", supervisionErr)
	}

	if _, err := manager.RecoverHA(context.Background(), config.Name, input, fingerprint, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	createCounts := map[string]int{}
	for _, call := range runner.calls {
		if len(call) >= 2 && call[1] == "create" && (call[0] == "network" || call[0] == "volume") {
			createCounts[call[len(call)-1]]++
		}
	}
	if createCounts[config.NetworkName] != 1 {
		t.Fatalf("network creation attempts = %d, want one", createCounts[config.NetworkName])
	}
	for _, member := range config.Members {
		if createCounts[HAVolumeName(config.Name, member.ID)] != 1 {
			t.Fatalf("member %d volume creation attempts = %d, want one exact applied creation", member.ID, createCounts[HAVolumeName(config.Name, member.ID)])
		}
	}
	assertNoHARecoveryTestSecret(t, runner.calls, firstErr.Error())
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

func TestRestoreHAPreservesStoppedDesiredWhenJournalCannotBeTrusted(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	stagedPath, err := haRestoreStagingPath(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	journalPath, err := HARecoveryStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	// An invalid object at the journal path cannot be trusted as the supervisor's
	// crash barrier. Restore must fail closed before staging or runtime mutation,
	// while preserving the existing Stopped intent.
	if err := os.Mkdir(journalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := newHARecoveryRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner

	_, err = manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "protected recovery journal cannot be trusted") {
		t.Fatalf("untrusted journal error = %v", err)
	}
	restoreErr := err
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
	if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restore staging package remains after pre-mutation failure: %v", statErr)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, restoreErr.Error())
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

func TestRestoreHARetryReplacesCrashStagedPackageAfterCleaningExactHelper(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	validated, err := validateHASnapshotPackage(context.Background(), config, input, true)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(validated.token)
	stagedPath, err := haRestoreStagingPath(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageHASnapshotPackage(context.Background(), validated, stagedPath); err != nil {
		t.Fatal(err)
	}
	runner := newHARecoveryRunner(t, config)
	spec := restoreResetHelperSpec(config, config.Members[0], stagedPath)
	helper := recoveryFakeHelperFromRun(t, haRestoreResetRunArguments(config, config.Members[0], spec))
	helper.record.Status.State = "running"
	runner.helpers[spec.Name] = helper
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	state, err := manager.RestoreHA(context.Background(), config.Name, input, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadyMembers != haMemberCount || !state.Healthy {
		t.Fatalf("crash-staged retry state = %+v", state)
	}
	if _, exists := runner.helpers[spec.Name]; exists {
		t.Fatalf("stale staged restore helper %q remains after retry", spec.Name)
	}
	assertRecoveryCallOrder(t, runner.calls,
		[]string{"stop", spec.Name},
		[]string{"delete", spec.Name},
		[]string{"run", "--name", spec.Name},
	)
	resetCall := findRecoveryCall(t, runner.calls, []string{"run", "--name", spec.Name})
	wantMount := fmt.Sprintf("type=bind,source=%s,target=%s,readonly", stagedPath, haRecoveryBackupMount)
	if got := valueAfterRecoveryArgument(resetCall, "--mount"); got != wantMount {
		t.Fatalf("retry restore backup mount = %q, want protected staging mount %q", got, wantMount)
	}
	if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("crash-staged package remains after successful retry: %v", statErr)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, "")
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
	stagedPath, err := haRestoreStagingPath(config)
	if err != nil {
		t.Fatal(err)
	}
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
	resolvedInput, err := filepath.EvalSymlinks(input)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Clean(journal.SnapshotPath) != filepath.Clean(resolvedInput) {
		t.Fatalf("recovery journal snapshot path = %q, want validated original package %q", journal.SnapshotPath, resolvedInput)
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
	wantMount := fmt.Sprintf("type=bind,source=%s,target=%s,readonly", stagedPath, haRecoveryBackupMount)
	if got := valueAfterRecoveryArgument(resetCall, "--mount"); got != wantMount {
		t.Fatalf("restore backup mount = %q, want protected staging mount %q", got, wantMount)
	}
	if strings.Contains(joined, "source="+input+",target="+haRecoveryBackupMount) {
		t.Fatalf("restore mounted mutable original package: %#v", resetCall)
	}
	if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restore staging package remains after success: %v", statErr)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, "")
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

func TestHARestoreResetArgumentsInstallTokenWithoutSecretInArgv(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	member := config.Members[0]
	spec := restoreResetHelperSpec(config, member, t.TempDir())
	arguments := haRestoreResetRunArguments(config, member, spec)
	joined := strings.Join(arguments, " ")
	for _, secret := range []string{haRecoveryTestToken, secureHARecoveryTestToken()} {
		if strings.Contains(joined, secret) {
			t.Fatalf("restore reset arguments leaked server token material")
		}
	}
	if spec.Executable != "/bin/sh" || len(spec.Arguments) < 8 || spec.Arguments[0] != "-c" || spec.Arguments[2] != "apc-k3s" {
		t.Fatalf("restore reset shell arguments = %#v", spec.Arguments)
	}
	if spec.Arguments[3] != "/backup/server-token" || spec.Arguments[4] != "/var/lib/rancher/k3s/server" || spec.Arguments[5] != "server" {
		t.Fatalf("restore reset token installer positional arguments = %#v", spec.Arguments[:6])
	}
	script := spec.Arguments[1]
	for _, invariant := range []string{
		"umask 077",
		`[ -L "$APC_TOKEN_SOURCE" ]`,
		`[ -L "$APC_TOKEN_DESTINATION" ]`,
		`mktemp "$APC_TOKEN_DIRECTORY/.apc-server-token.XXXXXX"`,
		`chmod 600 "$APC_TOKEN_TEMP"`,
		`sync "$APC_TOKEN_TEMP"`,
		`mv -f "$APC_TOKEN_TEMP" "$APC_TOKEN_DESTINATION"`,
		`rm -f "$APC_TOKEN_TEMP"`,
		`exec /bin/k3s "$@"`,
	} {
		if !strings.Contains(script, invariant) {
			t.Fatalf("restore reset installer script lacks %q", invariant)
		}
	}
	if got := valueAfterRecoveryArgument(spec.Arguments, "--token-file"); got != "/backup/server-token" {
		t.Fatalf("restore reset --token-file = %q, want retained read-only backup token", got)
	}
	assertNoHARecoveryTestSecret(t, [][]string{arguments}, "")
}

func TestHARestoreTokenInstallScriptPublishesPrivateFileAndRejectsSymlinks(t *testing.T) {
	t.Run("atomic private publication", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "source-token")
		destinationDirectory := filepath.Join(root, "server")
		secret := secureHARecoveryTestToken() + "\n"
		if err := os.WriteFile(source, []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		if output, err := runHARestoreTokenInstallScript(source, destinationDirectory, ""); err != nil {
			t.Fatalf("token installer failed: %v output=%q", err, output)
		}
		destination := filepath.Join(destinationDirectory, "token")
		data, err := os.ReadFile(destination)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != secret {
			t.Fatal("installed restore token differs from validated source")
		}
		info, err := os.Lstat(destination)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("installed restore token mode = %v/%o, want regular/0600", info.Mode(), info.Mode().Perm())
		}
		assertOnlyHARestoreTokenFile(t, destinationDirectory)

		if err := os.WriteFile(destination, []byte("stale-token\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(destination, 0o644); err != nil {
			t.Fatal(err)
		}
		if output, err := runHARestoreTokenInstallScript(source, destinationDirectory, ""); err != nil {
			t.Fatalf("replace existing token failed: %v output=%q", err, output)
		}
		info, err = os.Lstat(destination)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("replaced restore token mode = %o, want 0600", info.Mode().Perm())
		}
		assertOnlyHARestoreTokenFile(t, destinationDirectory)
	})

	t.Run("source symlink", func(t *testing.T) {
		root := t.TempDir()
		realSource := filepath.Join(root, "real-token")
		source := filepath.Join(root, "source-token")
		destinationDirectory := filepath.Join(root, "server")
		secret := secureHARecoveryTestToken()
		if err := os.WriteFile(realSource, []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realSource, source); err != nil {
			t.Fatal(err)
		}
		output, err := runHARestoreTokenInstallScript(source, destinationDirectory, "")
		if err == nil || !strings.Contains(output, "source validation failed") {
			t.Fatalf("source symlink installer error=%v output=%q", err, output)
		}
		assertHARestoreTokenInstallerOutputSafe(t, output, secret, source, destinationDirectory)
		if _, statErr := os.Lstat(destinationDirectory); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("source symlink created restore token state: %v", statErr)
		}
	})

	t.Run("destination symlink", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "source-token")
		destinationDirectory := filepath.Join(root, "server")
		target := filepath.Join(root, "foreign-token")
		secret := secureHARecoveryTestToken()
		if err := os.WriteFile(source, []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(destinationDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte("foreign\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(destinationDirectory, "token")); err != nil {
			t.Fatal(err)
		}
		output, err := runHARestoreTokenInstallScript(source, destinationDirectory, "")
		if err == nil || !strings.Contains(output, "destination validation failed") {
			t.Fatalf("destination symlink installer error=%v output=%q", err, output)
		}
		assertHARestoreTokenInstallerOutputSafe(t, output, secret, source, destinationDirectory)
		data, readErr := os.ReadFile(target)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(data) != "foreign\n" {
			t.Fatal("destination symlink target was modified")
		}
	})

	t.Run("destination directory symlink", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "source-token")
		realDirectory := filepath.Join(root, "real-server")
		destinationDirectory := filepath.Join(root, "server")
		secret := secureHARecoveryTestToken()
		if err := os.WriteFile(source, []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(realDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realDirectory, destinationDirectory); err != nil {
			t.Fatal(err)
		}
		output, err := runHARestoreTokenInstallScript(source, destinationDirectory, "")
		if err == nil || !strings.Contains(output, "directory validation failed") {
			t.Fatalf("destination directory symlink installer error=%v output=%q", err, output)
		}
		assertHARestoreTokenInstallerOutputSafe(t, output, secret, source, destinationDirectory, realDirectory)
		entries, readErr := os.ReadDir(realDirectory)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 0 {
			t.Fatalf("destination directory symlink received files: %#v", entries)
		}
	})

	t.Run("copy failure cleans temporary file", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "source-token")
		destinationDirectory := filepath.Join(root, "server")
		fakeBin := filepath.Join(root, "bin")
		secret := secureHARecoveryTestToken()
		if err := os.WriteFile(source, []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(fakeBin, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fakeBin, "cat"), []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		path := fakeBin + string(os.PathListSeparator) + "/bin" + string(os.PathListSeparator) + "/usr/bin"
		output, err := runHARestoreTokenInstallScript(source, destinationDirectory, path)
		if err == nil || !strings.Contains(output, "token copy failed") {
			t.Fatalf("injected copy failure error=%v output=%q", err, output)
		}
		assertHARestoreTokenInstallerOutputSafe(t, output, secret, source, destinationDirectory)
		entries, readErr := os.ReadDir(destinationDirectory)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 0 {
			t.Fatalf("copy failure left token temporary files: %#v", entries)
		}
	})
}

func runHARestoreTokenInstallScript(source, destinationDirectory, commandPath string) (string, error) {
	command := exec.Command("/bin/sh", "-c", haRestoreTokenInstallScript, "apc-token-test", source, destinationDirectory, "true")
	if commandPath != "" {
		command.Env = append(os.Environ(), "PATH="+commandPath)
	}
	output, err := command.CombinedOutput()
	return string(output), err
}

func assertOnlyHARestoreTokenFile(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "token" {
		t.Fatalf("restore token directory entries = %#v, want only token", entries)
	}
}

func assertHARestoreTokenInstallerOutputSafe(t *testing.T, output string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if value != "" && strings.Contains(output, value) {
			t.Fatalf("restore token installer output leaked %q: %q", value, output)
		}
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

func TestRunHARestoreResetDoesNotReflectCollisionAddresses(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	runner.resetCollisionAddresses = []string{
		config.Members[1].StableIP,
		config.Members[2].StableIP,
		config.Members[1].StableIP,
	}
	manager := NewManager("container")
	manager.runner = runner
	spec := restoreResetHelperSpec(config, config.Members[0], t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := manager.runHARestoreReset(ctx, config, config.Members[0], spec)
	if err == nil || !strings.Contains(err.Error(), "could not obtain a non-reserved runtime IPv4 after 3 attempts") {
		t.Fatalf("restore reset collision error = %v", err)
	}
	for _, forbidden := range append([]string{haRuntimeIPCollisionMarker}, runner.resetCollisionAddresses...) {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("restore reset collision error leaked %q: %v", forbidden, err)
		}
	}
}

func TestSanitizeHARestoreResetStdoutUsesOnlyBoundedDiagnosticKeys(t *testing.T) {
	credential := strings.Repeat("A", 512)
	unsafeOutput := strings.Join([]string{
		"permission denied while reading /Users/apc-test/private/etcd-snapshot",
		"Bearer " + credential,
		"token=" + secureHARecoveryTestToken(),
		"failed to restore snapshot from /private/var/tmp/backup",
		"no such file at 192.168.96.12",
	}, "\n")
	diagnostic := sanitizeHARestoreResetStdout([]byte(unsafeOutput))
	want := "permission denied, no such file, snapshot, restore failed, etcd/datastore"
	if diagnostic != want {
		t.Fatalf("sanitized reset diagnostic = %q, want %q", diagnostic, want)
	}
	for _, forbidden := range []string{
		credential,
		secureHARecoveryTestToken(),
		"Bearer",
		"token=",
		"192.168.96.12",
		"/Users/apc-test/private",
		"/private/var/tmp/backup",
	} {
		if strings.Contains(diagnostic, forbidden) {
			t.Fatalf("sanitized reset diagnostic leaked %q: %q", forbidden, diagnostic)
		}
	}
	if len(diagnostic) > haResetDiagnosticOutputMax {
		t.Fatalf("sanitized reset diagnostic length = %d, maximum %d", len(diagnostic), haResetDiagnosticOutputMax)
	}

	huge := bytes.Repeat([]byte("untrusted-credential-material "), haResetDiagnosticInputMax)
	huge = append(huge, []byte(" permission denied: snapshot")...)
	if got := sanitizeHARestoreResetStdout(huge); got != "permission denied, snapshot" {
		t.Fatalf("bounded head/tail reset diagnostic = %q", got)
	}
	if got := sanitizeHARestoreResetStdout([]byte("Bearer " + credential)); got != "unclassified failure output" {
		t.Fatalf("credential-only reset diagnostic = %q", got)
	}
	if got := sanitizeHARestoreResetStdout([]byte(" \n\t")); got != "" {
		t.Fatalf("whitespace reset diagnostic = %q", got)
	}
}

func TestRunHARestoreResetAddsSanitizedOutputDiagnosticsOnFailure(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	runner := newHARecoveryRunner(t, config)
	stdoutCredential := strings.Repeat("b", 256)
	stderrCredential := strings.Repeat("c", 256)
	runner.resetFailureStdout = []byte(strings.Join([]string{
		"permission denied /Users/apc-test/.config/apc/server-token",
		"Bearer " + stdoutCredential,
		"token=" + secureHARecoveryTestToken(),
		"no such file: /private/var/tmp/restore/etcd-snapshot",
		"peer=192.168.96.12 failed to restore snapshot",
	}, "\n"))
	runner.resetFailureStderr = []byte(strings.Join([]string{
		"operation not permitted at /Users/apc-test/private/reset",
		"Authorization: Bearer " + stderrCredential,
		"token=" + secureHARecoveryTestToken(),
		"peer=192.168.96.13 checksum mismatch",
	}, "\n"))
	runner.resetFailure = errors.New("exit 1 with argv --token " + secureHARecoveryTestToken() + " --mount /Users/apc-test/private/reset")
	manager := NewManager("container")
	manager.runner = runner
	spec := restoreResetHelperSpec(config, config.Members[0], t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := manager.runHARestoreReset(ctx, config, config.Members[0], spec)
	if err == nil {
		t.Fatal("failed reset helper returned no error")
	}
	message := err.Error()
	wantDiagnostic := "reset helper stdout diagnostic: permission denied, no such file, snapshot, restore failed, etcd/datastore"
	if !strings.Contains(message, wantDiagnostic) {
		t.Fatalf("reset failure lacks sanitized stdout diagnosis: %v", err)
	}
	wantStderrDiagnostic := "reset helper stderr diagnostic: operation not permitted, checksum mismatch"
	if !strings.Contains(message, wantStderrDiagnostic) {
		t.Fatalf("reset failure lacks sanitized stderr diagnosis: %v", err)
	}
	for _, forbidden := range []string{
		stdoutCredential,
		stderrCredential,
		secureHARecoveryTestToken(),
		"Bearer",
		"token=",
		"192.168.96.12",
		"192.168.96.13",
		"/Users/apc-test",
		"/private/var/tmp/restore",
		"--mount",
	} {
		if strings.Contains(message, forbidden) {
			t.Fatalf("reset failure leaked %q: %v", forbidden, err)
		}
	}
	if _, exists := runner.helpers[spec.Name]; exists {
		t.Fatalf("failed restore reset helper %q was not cleaned up", spec.Name)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, message)
}

func TestRestoreHAResetFailureSanitizesReturnedErrorAndRecoveryJournal(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	runner := newHARecoveryRunner(t, config)
	stdoutCredential := strings.Repeat("d", 256)
	stderrCredential := strings.Repeat("e", 256)
	runner.resetFailureStdout = []byte(strings.Join([]string{
		"failed to restore snapshot /private/var/tmp/restore/etcd-snapshot",
		"Bearer " + stdoutCredential,
		"token=" + secureHARecoveryTestToken(),
		"peer=192.168.96.12",
	}, "\n"))
	runner.resetFailureStderr = []byte(strings.Join([]string{
		"permission denied /Users/apc-test/private/reset",
		"Authorization: Bearer " + stderrCredential,
		"token=" + secureHARecoveryTestToken(),
		"peer=192.168.96.13 checksum mismatch",
	}, "\n"))
	runner.resetFailure = errors.New("exit 1 with argv --token " + secureHARecoveryTestToken() + " --mount /Users/apc-test/private/reset")
	manager := NewManager("container")
	manager.runner = runner

	_, err := manager.RestoreHA(context.Background(), config.Name, input, 2*time.Second)
	if err == nil {
		t.Fatal("failed reset returned no restore error")
	}
	journal, loadErr := LoadHARecoveryState(config.Name)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if journal.Phase != "failed" || !journal.RecoveryAttempted || journal.RecoverySucceeded {
		t.Fatalf("failed reset recovery journal = %+v", journal)
	}
	for label, value := range map[string]string{
		"returned error":   err.Error(),
		"recovery journal": journal.Message,
	} {
		if !strings.Contains(value, "reset helper stderr diagnostic: permission denied, checksum mismatch") ||
			!strings.Contains(value, "reset helper stdout diagnostic: snapshot, restore failed, etcd/datastore") {
			t.Fatalf("%s lacks canonical reset diagnostics: %q", label, value)
		}
		for _, forbidden := range []string{
			stdoutCredential,
			stderrCredential,
			secureHARecoveryTestToken(),
			"Bearer",
			"token=",
			"192.168.96.12",
			"192.168.96.13",
			"/Users/apc-test",
			"/private/var/tmp/restore",
			"--mount",
		} {
			if strings.Contains(value, forbidden) {
				t.Fatalf("%s leaked %q: %q", label, forbidden, value)
			}
		}
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
	stagedPath, pathErr := haRestoreStagingPath(config)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	runner := newHARecoveryRunner(t, config)
	runner.failMemberTwoStartOnce = true
	manager := NewManager("container")
	manager.runner = runner
	if _, statErr := os.Lstat(config.KubeconfigPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed-restore recovery unexpectedly started with a kubeconfig: %v", statErr)
	}
	apiProbeCount := 0
	var apiProbePreconditionErr error
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, _ HAMember) bool {
		apiProbeCount++
		if apiProbePreconditionErr != nil {
			return true
		}
		info, statErr := os.Lstat(config.KubeconfigPath)
		if statErr != nil {
			apiProbePreconditionErr = fmt.Errorf("automatic restore recovery API probe ran before kubeconfig publication: %w", statErr)
			return true
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			apiProbePreconditionErr = fmt.Errorf("automatic restore recovery kubeconfig mode before API probe = %v/%o, want regular/0600", info.Mode(), info.Mode().Perm())
			return true
		}
		data, readErr := os.ReadFile(config.KubeconfigPath)
		if readErr != nil {
			apiProbePreconditionErr = fmt.Errorf("read automatic restore recovery kubeconfig before API probe: %w", readErr)
			return true
		}
		if !strings.Contains(string(data), "server: "+config.Members[0].apiEndpoint(config.ListenAddress)) {
			apiProbePreconditionErr = fmt.Errorf("automatic restore recovery kubeconfig was not rewritten before API probe")
		}
		return true
	}

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
	if apiProbePreconditionErr != nil {
		t.Fatal(apiProbePreconditionErr)
	}
	if apiProbeCount != haMemberCount {
		t.Fatalf("automatic restore recovery API probes = %d, want %d after kubeconfig publication", apiProbeCount, haMemberCount)
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
	if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restore staging package remains after recovered failure: %v", statErr)
	}
	assertNoHARecoveryTestSecret(t, runner.calls, err.Error())
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

func TestStageHASnapshotPackageCopiesValidatedBytesWithPrivateModes(t *testing.T) {
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	validated, err := validateHASnapshotPackage(context.Background(), config, input, true)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(validated.token)
	stagedPath, err := haRestoreStagingPath(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageHASnapshotPackage(context.Background(), validated, stagedPath); err != nil {
		t.Fatal(err)
	}

	directoryInfo, err := os.Lstat(stagedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("staging directory mode = %v/%o, want directory/0700", directoryInfo.Mode(), directoryInfo.Mode().Perm())
	}
	wantModes := map[string]os.FileMode{
		haSnapshotManifestFilename: 0o400,
		haSnapshotDataFilename:     0o600,
		haSnapshotTokenFilename:    0o600,
	}
	for name, wantMode := range wantModes {
		sourceData, readErr := os.ReadFile(filepath.Join(input, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		stagedData, readErr := os.ReadFile(filepath.Join(stagedPath, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !bytes.Equal(stagedData, sourceData) {
			t.Fatalf("staged artifact %q differs from its validated source", name)
		}
		info, statErr := os.Lstat(filepath.Join(stagedPath, name))
		if statErr != nil {
			t.Fatal(statErr)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != wantMode {
			t.Fatalf("staged artifact %q mode = %v/%o, want regular/%o", name, info.Mode(), info.Mode().Perm(), wantMode)
		}
	}

	stagedValidation, err := validateHASnapshotPackage(context.Background(), config, stagedPath, true)
	if err != nil {
		t.Fatalf("staged package did not revalidate: %v", err)
	}
	clear(stagedValidation.token)
	if err := removeHAStagingDirectory(stagedPath); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("staging package remains after cleanup: %v", statErr)
	}
}

func TestStageHASnapshotPackageRejectsMutationAndPathSwapsAfterValidation(t *testing.T) {
	tests := []struct {
		name     string
		artifact string
		mutate   func(*testing.T, string)
	}{
		{
			name:     "same inode content mutation",
			artifact: haSnapshotDataFilename,
			mutate: func(t *testing.T, path string) {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[0] ^= 0xff
				file, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteAt(data, 0); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:     "manifest symlink swap",
			artifact: haSnapshotManifestFilename,
			mutate: func(t *testing.T, path string) {
				archived := path + ".validated"
				if err := os.Rename(path, archived); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Base(archived), path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:     "token inode swap with identical bytes",
			artifact: haSnapshotTokenFilename,
			mutate: func(t *testing.T, path string) {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, path+".validated"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := prepareHARecoveryTestConfig(t)
			input := writeHARecoveryTestSnapshot(t, config)
			validated, err := validateHASnapshotPackage(context.Background(), config, input, true)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(validated.token)
			stagedPath, err := haRestoreStagingPath(config)
			if err != nil {
				t.Fatal(err)
			}

			test.mutate(t, filepath.Join(input, test.artifact))
			err = stageHASnapshotPackage(context.Background(), validated, stagedPath)
			if err == nil {
				t.Fatal("staging accepted a snapshot artifact changed after validation")
			}
			assertNoHARecoveryTestSecret(t, nil, err.Error())
			if _, statErr := os.Lstat(stagedPath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed staging package was not cleaned up: %v", statErr)
			}
		})
	}
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

func prepareEmptyHARecoveryTest(t *testing.T) (HAConfig, string, string) {
	t.Helper()
	config := prepareHARecoveryTestConfig(t)
	input := writeHARecoveryTestSnapshot(t, config)
	fingerprint := haRecoveryTestManifestFingerprint(t, input)
	configPath, err := HAConfigPath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{configPath, config.TokenFile, config.KubeconfigPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	return config, input, fingerprint
}

func haRecoveryTestManifestFingerprint(t *testing.T, input string) string {
	t.Helper()
	loaded, err := readHASnapshotManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if !validSHA256(loaded.ManifestSHA256) {
		t.Fatalf("invalid test manifest fingerprint %q", loaded.ManifestSHA256)
	}
	return loaded.ManifestSHA256
}

func assertHARecoveryFilesAbsent(t *testing.T, config HAConfig, includeConfig bool) {
	t.Helper()
	paths := []string{config.TokenFile, config.KubeconfigPath}
	if includeConfig {
		configPath, err := HAConfigPath(config.Name)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, configPath, filepath.Join(filepath.Dir(configPath), haOperationLockFilename))
	}
	desiredPath, err := haDesiredStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPath, err := HARecoveryStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, desiredPath, recoveryPath)
	for _, path := range paths {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("unexpected HA recovery state at %q: %v", path, statErr)
		}
	}
}

func assertNoHARecoveryTestSecret(t *testing.T, calls [][]string, message string) {
	t.Helper()
	values := []string{haRecoveryTestToken, secureHARecoveryTestToken()}
	for _, value := range values {
		if strings.Contains(message, value) {
			t.Fatal("HA recovery error leaked token material")
		}
		for _, call := range calls {
			if strings.Contains(strings.Join(call, " "), value) {
				t.Fatalf("HA recovery command leaked token material: %#v", call)
			}
		}
	}
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
	t                         *testing.T
	config                    HAConfig
	calls                     [][]string
	serverExists              map[int]bool
	serverState               map[int]string
	helpers                   map[string]haRecoveryFakeHelper
	readyMembers              int
	guestSnapshotPath         string
	failMemberTwoStartOnce    bool
	networkMissing            bool
	volumeMissing             map[int]bool
	failVolumeCreateMember    int
	foreignNetwork            bool
	foreignVolumeMember       int
	failSnapshotDelete        bool
	failSnapshotStopApplied   bool
	failRestoreStopMemberOnce int
	noopServerStopMember      int
	failSnapshotRunApplied    bool
	failSnapshotCopy          bool
	failHelperStopApplied     bool
	failHelperDeleteApplied   bool
	requireQuorumForReady     bool
	failEtcdTopology          bool
	resetDeadline             time.Time
	resetCollisionAddresses   []string
	resetRunCount             int
	resetFailureStdout        []byte
	resetFailureStderr        []byte
	resetFailure              error
	seedInspectCount          int
	failSeedInspectAttempt    int
	beforeVolumeCreate        func(int)
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
		volumeMissing:     map[int]bool{},
		readyMembers:      haMemberCount,
		guestSnapshotPath: haSnapshotGuestDirectory + "/apc-ha-lab-test-1700000000",
	}
	for _, member := range config.Members {
		runner.serverExists[member.ID] = true
		runner.serverState[member.ID] = "running"
	}
	return runner
}

func newEmptyHARecoveryRunner(t *testing.T, config HAConfig) *haRecoveryRunner {
	t.Helper()
	runner := newHARecoveryRunner(t, config)
	runner.networkMissing = true
	for _, member := range config.Members {
		runner.volumeMissing[member.ID] = true
		runner.serverExists[member.ID] = false
		delete(runner.serverState, member.ID)
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
		if runner.foreignNetwork {
			record.Configuration.Labels["apc.dev/cluster"] = "foreign"
		}
		return marshalHAInspect(runner.t, record), nil, nil
	case len(call) >= 3 && call[0] == "network" && call[1] == "create":
		runner.networkMissing = false
		return nil, nil, nil
	case len(call) == 3 && call[0] == "volume" && call[1] == "inspect":
		member := memberForHAVolume(runner.t, runner.config, call[2])
		if runner.volumeMissing[member.ID] {
			return nil, []byte("volume not found"), errors.New("exit 1")
		}
		var record haVolumeInspect
		record.Configuration.Name = call[2]
		record.Configuration.Labels = map[string]string{
			"apc.dev/managed": "true", "apc.dev/cluster": runner.config.Name,
			"apc.dev/role": "server", "apc.dev/member": strconv.Itoa(member.ID),
		}
		record.Configuration.Options = map[string]string{"size": runner.config.VolumeSize}
		if runner.foreignVolumeMember == member.ID {
			record.Configuration.Labels["apc.dev/cluster"] = "foreign"
		}
		return marshalHAInspect(runner.t, record), nil, nil
	case len(call) >= 3 && call[0] == "volume" && call[1] == "create":
		member := memberForHAVolume(runner.t, runner.config, call[len(call)-1])
		if runner.beforeVolumeCreate != nil {
			runner.beforeVolumeCreate(member.ID)
		}
		runner.volumeMissing[member.ID] = false
		if runner.failVolumeCreateMember == member.ID {
			runner.failVolumeCreateMember = 0
			return nil, []byte("injected applied volume create failure"), errors.New("exit 1")
		}
		return nil, nil, nil
	case len(call) == 2 && call[0] == "inspect":
		return runner.inspect(call[1])
	case len(call) >= 3 && call[0] == "exec":
		return runner.exec(call)
	case len(call) == 2 && call[0] == "stop":
		if member, ok := runner.serverMember(call[1]); ok {
			if member.ID == runner.failRestoreStopMemberOnce {
				runner.failRestoreStopMemberOnce = 0
				return nil, []byte("injected unapplied HA restore stop failure"), errors.New("exit 1")
			}
			if member.ID != runner.noopServerStopMember {
				runner.serverState[member.ID] = "stopped"
			}
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
		if member.ID == 1 {
			runner.seedInspectCount++
			if runner.seedInspectCount == runner.failSeedInspectAttempt {
				return nil, []byte("injected pre-reset seed inspect failure"), errors.New("exit 1")
			}
		}
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
		if runner.resetFailure != nil {
			return append([]byte(nil), runner.resetFailureStdout...), append([]byte(nil), runner.resetFailureStderr...), runner.resetFailure
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
