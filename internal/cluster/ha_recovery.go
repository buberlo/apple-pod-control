package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	haSnapshotAPIVersion       = "apc.dev/v1alpha1"
	haSnapshotKind             = "HAEtcdSnapshot"
	haSnapshotFormatVersion    = 1
	haSnapshotManifestFilename = "manifest.json"
	haSnapshotDataFilename     = "etcd-snapshot"
	haSnapshotTokenFilename    = "server-token"
	haSnapshotGuestDirectory   = "/var/lib/rancher/k3s/server/db/snapshots"
	haRecoveryBackupMount      = "/backup"
	haRecoveryDataMount        = "/data"
	haRecoveryStateFilename    = "ha-recovery-state.json"
	haRecoveryStateKind        = "HARecoveryState"
	haRecoveryOperationTimeout = 2 * time.Minute
	haRecoveryAttemptTimeout   = 5 * time.Minute
	haRecoveryTokenMaximum     = int64(4096)
	haRecoveryStateMaximum     = int64(64 << 10)
)

// HASnapshotManifest is the immutable, versioned description of one external
// K3s embedded-etcd snapshot. It intentionally contains topology metadata, not
// host paths from HAConfig. Snapshot and token contents are bound independently
// by size and SHA-256, and Cluster.Identity binds both to the exact topology.
type HASnapshotManifest struct {
	APIVersion    string                    `json:"apiVersion"`
	Kind          string                    `json:"kind"`
	FormatVersion int                       `json:"formatVersion"`
	CreatedAt     time.Time                 `json:"createdAt"`
	Cluster       HASnapshotClusterMetadata `json:"cluster"`
	Topology      HASnapshotTopology        `json:"topology"`
	Image         HASnapshotImageMetadata   `json:"image"`
	K3s           HASnapshotK3sMetadata     `json:"k3s"`
	Snapshot      HASnapshotFileMetadata    `json:"snapshot"`
	ServerToken   HASnapshotFileMetadata    `json:"serverToken"`
}

type HASnapshotClusterMetadata struct {
	Name     string `json:"name"`
	Identity string `json:"identity"`
}

type HASnapshotTopology struct {
	NetworkName    string     `json:"networkName"`
	Subnet         string     `json:"subnet"`
	ListenAddress  string     `json:"listenAddress"`
	CPUs           int        `json:"cpus"`
	Memory         string     `json:"memory"`
	VolumeSize     string     `json:"volumeSize"`
	DisableTraefik bool       `json:"disableTraefik"`
	Members        []HAMember `json:"members"`
}

type HASnapshotImageMetadata struct {
	Reference    string `json:"reference"`
	Architecture string `json:"architecture"`
}

type HASnapshotK3sMetadata struct {
	Version        string    `json:"version"`
	SnapshotName   string    `json:"snapshotName"`
	SourceMemberID int       `json:"sourceMemberID"`
	SourceNodeName string    `json:"sourceNodeName"`
	CreatedAt      time.Time `json:"createdAt"`
}

type HASnapshotFileMetadata struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type HASnapshotResult struct {
	Path       string             `json:"path"`
	Bytes      int64              `json:"bytes"`
	DataSHA256 string             `json:"dataSHA256"`
	CreatedAt  time.Time          `json:"createdAt"`
	Manifest   HASnapshotManifest `json:"manifest"`
	Warning    string             `json:"warning,omitempty"`
}

// HARecoveryState is persisted with mode 0600 before the first runtime
// mutation. A failed restore therefore leaves an explicit phase and next step
// even if the calling terminal disappears.
type HARecoveryState struct {
	APIVersion        string    `json:"apiVersion"`
	Kind              string    `json:"kind"`
	Cluster           string    `json:"cluster"`
	ClusterIdentity   string    `json:"clusterIdentity"`
	SnapshotPath      string    `json:"snapshotPath"`
	Phase             string    `json:"phase"`
	StartedAt         time.Time `json:"startedAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
	CompletedAt       time.Time `json:"completedAt,omitempty"`
	RecoveryAttempted bool      `json:"recoveryAttempted"`
	RecoverySucceeded bool      `json:"recoverySucceeded"`
	Message           string    `json:"message,omitempty"`
	NextAction        string    `json:"nextAction,omitempty"`
}

type haRestoreProgress struct {
	mutationStarted bool
	resetAttempted  bool
	resetComplete   bool
	peerDataCleared map[int]bool
}

// HARecoveryStatePath returns the protected on-host recovery journal path for
// a local HA cluster.
func HARecoveryStatePath(name string) (string, error) {
	configPath, err := HAConfigPath(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(configPath), haRecoveryStateFilename), nil
}

func HASnapshotHelperContainerName(name string) string {
	return "apc-k3s-" + name + "-ha-snapshot-copy"
}

func HARestoreResetContainerName(name string) string {
	return "apc-k3s-" + name + "-ha-restore-reset"
}

func HARestoreClearContainerName(name string, memberID int) string {
	return fmt.Sprintf("apc-k3s-%s-ha-restore-clear-%d", name, memberID)
}

// SnapshotHA creates a native K3s etcd snapshot, copies it and the matching
// server token outside all member volumes through an APC-owned helper, verifies
// both files, restores the source member, and atomically publishes the package.
// All three members must be Ready so stopping the source member for the short
// copy window leaves a healthy two-member quorum.
func (m *Manager) SnapshotHA(ctx context.Context, name, output string) (result HASnapshotResult, err error) {
	if !dnsLabel.MatchString(name) {
		return HASnapshotResult{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	config, err := loadHAConfig(name)
	if err != nil {
		return HASnapshotResult{}, err
	}
	lock, err := acquireHAOperationLock(ctx, config.Name)
	if err != nil {
		return HASnapshotResult{}, err
	}
	defer func() { err = errors.Join(err, lock.release()) }()
	output, err = prepareHASnapshotOutput(output)
	if err != nil {
		return HASnapshotResult{}, err
	}
	token, err := readHARecoveryToken(config.TokenFile)
	if err != nil {
		return HASnapshotResult{}, err
	}
	preflightCtx, cancelPreflight := context.WithTimeout(ctx, haRecoveryOperationTimeout)
	preflight, err := m.preflightHA(preflightCtx, config, false)
	cancelPreflight()
	if err != nil {
		return HASnapshotResult{}, err
	}
	if len(preflight.volumeExists) != haMemberCount || len(preflight.containerRecord) != haMemberCount {
		return HASnapshotResult{}, fmt.Errorf("HA snapshot requires all three APC-owned member volumes and server envelopes")
	}
	statusCtx, cancelStatus := context.WithTimeout(ctx, haRecoveryOperationTimeout)
	state, err := m.StatusHA(statusCtx, name)
	cancelStatus()
	if err != nil {
		return HASnapshotResult{}, err
	}
	if !state.Healthy || state.ReadyMembers != haMemberCount {
		return HASnapshotResult{}, fmt.Errorf("refusing HA snapshot: all three members must be Ready to preserve a healthy quorum during export (ready=%d, quorum=%d)", state.ReadyMembers, state.Quorum)
	}
	if _, err := m.validateHAEtcdTopology(ctx, config); err != nil {
		return HASnapshotResult{}, fmt.Errorf("refusing HA snapshot without exact healthy embedded-etcd topology: %w", err)
	}
	sourceState := state.Members[0]
	for _, candidate := range state.Members {
		if candidate.ID < sourceState.ID {
			sourceState = candidate
		}
	}
	source := memberByID(config.Members, sourceState.ID)
	if strings.TrimSpace(sourceState.K3sVersion) == "" {
		return HASnapshotResult{}, fmt.Errorf("refusing HA snapshot: source member %d did not report a K3s version", source.ID)
	}
	if err := m.ensureNoHARecoveryHelpers(ctx, config); err != nil {
		return HASnapshotResult{}, err
	}
	prefix, err := newHASnapshotName(config.Name)
	if err != nil {
		return HASnapshotResult{}, err
	}
	if _, _, err := m.runHARecoveryCommand(ctx, "create K3s embedded-etcd snapshot",
		"exec", HAContainerName(config.Name, source.ID),
		"/bin/k3s", "etcd-snapshot", "save",
		"--name", prefix,
		"--etcd-snapshot-dir", haSnapshotGuestDirectory,
	); err != nil {
		return HASnapshotResult{}, err
	}
	guestSnapshotPath, err := m.findCreatedHASnapshot(ctx, config, source, prefix)
	if err != nil {
		return HASnapshotResult{}, err
	}
	// The native K3s snapshot lives on the protected member volume. Once its
	// exact name is known, always attempt to remove it: failed exports must not
	// accumulate large etcd snapshots, while cleanup failure after a successful
	// durable publish is reported only as a warning.
	defer func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), haRecoveryOperationTimeout)
		defer cancelCleanup()
		_, _, cleanupErr := m.runHARecoveryCommand(cleanupCtx, "remove exported K3s snapshot from member volume",
			"exec", HAContainerName(config.Name, source.ID), "/bin/k3s", "etcd-snapshot", "delete", pathpkg.Base(guestSnapshotPath),
		)
		if cleanupErr == nil {
			return
		}
		if err == nil && result.Path != "" {
			result.Warning = fmt.Sprintf("snapshot package is safely published, but the temporary in-cluster snapshot could not be removed: %v", cleanupErr)
			return
		}
		err = errors.Join(err, fmt.Errorf("remove temporary in-cluster snapshot after HA snapshot failure: %w", cleanupErr))
	}()

	parent := filepath.Dir(output)
	temporary, err := os.MkdirTemp(parent, "."+filepath.Base(output)+".partial-")
	if err != nil {
		return HASnapshotResult{}, fmt.Errorf("create temporary HA snapshot directory: %w", err)
	}
	defer os.RemoveAll(temporary)
	if err := os.Chmod(temporary, 0o700); err != nil {
		return HASnapshotResult{}, fmt.Errorf("secure temporary HA snapshot directory: %w", err)
	}

	sourceStopped := false
	helperStarted := false
	defer func() {
		if err == nil {
			return
		}
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), haRecoveryAttemptTimeout)
		defer cancel()
		var recoveryErrors []error
		if helperStarted {
			recoveryErrors = append(recoveryErrors, m.deleteHARecoveryHelper(recoveryCtx, snapshotHelperSpec(config, source, temporary)))
		}
		if sourceStopped {
			recoveryErrors = append(recoveryErrors, m.startHAMemberForRecovery(recoveryCtx, config, source))
			if recoveryErrors[len(recoveryErrors)-1] == nil {
				recoveryErrors = append(recoveryErrors, m.waitHAMemberReady(recoveryCtx, config, source, config.StartupTimeout))
			}
		}
		if recoveryErr := errors.Join(recoveryErrors...); recoveryErr != nil {
			err = errors.Join(err, fmt.Errorf("recover source member after HA snapshot failure: %w", recoveryErr))
		}
	}()

	// Treat the stop as applied until exact inspection proves otherwise. The
	// runtime may stop the VM and still return a transport/client error.
	sourceStopped = true
	if err := m.runHABounded(ctx, fmt.Sprintf("stop HA snapshot source member %d", source.ID), "stop", HAContainerName(config.Name, source.ID)); err != nil {
		return HASnapshotResult{}, err
	}
	spec := snapshotHelperSpec(config, source, temporary)
	// As with stop, a detached run may create the helper before reporting an
	// error. Exact owned cleanup is therefore armed before issuing the command.
	helperStarted = true
	if err := m.startHARecoveryHelper(ctx, spec); err != nil {
		return HASnapshotResult{}, err
	}
	if _, _, err := m.runHARecoveryCommand(ctx, "copy K3s etcd snapshot outside member volume",
		"exec", spec.Name, "/bin/cp", pathpkg.Join(haRecoveryDataMount, strings.TrimPrefix(guestSnapshotPath, "/var/lib/rancher/k3s/")), pathpkg.Join("/backup", haSnapshotDataFilename),
	); err != nil {
		return HASnapshotResult{}, err
	}
	if _, _, err := m.runHARecoveryCommand(ctx, "copy matching K3s server token into snapshot package",
		"exec", spec.Name, "/bin/cp", pathpkg.Join(haRecoveryDataMount, "server/token"), pathpkg.Join("/backup", haSnapshotTokenFilename),
	); err != nil {
		return HASnapshotResult{}, err
	}
	if _, _, err := m.runHARecoveryCommand(ctx, "flush HA snapshot package", "exec", spec.Name, "/bin/sync"); err != nil {
		return HASnapshotResult{}, err
	}
	if err := m.deleteHARecoveryHelper(ctx, spec); err != nil {
		return HASnapshotResult{}, err
	}
	helperStarted = false
	if err := secureHAArtifact(filepath.Join(temporary, haSnapshotDataFilename)); err != nil {
		return HASnapshotResult{}, err
	}
	if err := secureHAArtifact(filepath.Join(temporary, haSnapshotTokenFilename)); err != nil {
		return HASnapshotResult{}, err
	}
	backupToken, err := readStrictTokenFile(filepath.Join(temporary, haSnapshotTokenFilename), true)
	if err != nil {
		return HASnapshotResult{}, fmt.Errorf("validate copied HA server token: %w", err)
	}
	if !sameK3sServerToken(token, backupToken) {
		return HASnapshotResult{}, fmt.Errorf("copied HA server token does not match the protected cluster token")
	}
	snapshotMaximum, err := haSnapshotArtifactMaximum(config)
	if err != nil {
		return HASnapshotResult{}, err
	}
	snapshotMetadata, err := hashHAArtifact(ctx, filepath.Join(temporary, haSnapshotDataFilename), haSnapshotDataFilename, snapshotMaximum)
	if err != nil {
		return HASnapshotResult{}, err
	}
	tokenMetadata, err := hashHAArtifact(ctx, filepath.Join(temporary, haSnapshotTokenFilename), haSnapshotTokenFilename, haRecoveryTokenMaximum)
	if err != nil {
		return HASnapshotResult{}, err
	}
	if snapshotMetadata.Size == 0 {
		return HASnapshotResult{}, fmt.Errorf("K3s snapshot is empty")
	}
	tokenCredential, err := k3sServerTokenCredential(backupToken)
	if err != nil {
		return HASnapshotResult{}, fmt.Errorf("validate K3s server token binding: %w", err)
	}
	tokenDigest := sha256.Sum256(tokenCredential)
	identity, err := haClusterIdentity(config, hex.EncodeToString(tokenDigest[:]))
	if err != nil {
		return HASnapshotResult{}, err
	}
	createdAt := time.Now().UTC()
	manifest := HASnapshotManifest{
		APIVersion:    haSnapshotAPIVersion,
		Kind:          haSnapshotKind,
		FormatVersion: haSnapshotFormatVersion,
		CreatedAt:     createdAt,
		Cluster:       HASnapshotClusterMetadata{Name: config.Name, Identity: identity},
		Topology:      haSnapshotTopology(config),
		Image:         HASnapshotImageMetadata{Reference: config.Image, Architecture: "arm64"},
		K3s: HASnapshotK3sMetadata{
			Version:        sourceState.K3sVersion,
			SnapshotName:   pathpkg.Base(guestSnapshotPath),
			SourceMemberID: source.ID,
			SourceNodeName: source.NodeName,
			CreatedAt:      createdAt,
		},
		Snapshot:    snapshotMetadata,
		ServerToken: tokenMetadata,
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return HASnapshotResult{}, fmt.Errorf("encode HA snapshot manifest: %w", err)
	}
	manifestPath := filepath.Join(temporary, haSnapshotManifestFilename)
	if err := writePrivateFileAtomic(manifestPath, append(manifestData, '\n')); err != nil {
		return HASnapshotResult{}, fmt.Errorf("write HA snapshot manifest: %w", err)
	}
	if err := os.Chmod(manifestPath, 0o400); err != nil {
		return HASnapshotResult{}, fmt.Errorf("make HA snapshot manifest immutable: %w", err)
	}
	if err := syncHAFile(manifestPath); err != nil {
		return HASnapshotResult{}, fmt.Errorf("flush HA snapshot manifest metadata: %w", err)
	}

	if err := m.startHAMemberForRecovery(ctx, config, source); err != nil {
		return HASnapshotResult{}, err
	}
	if err := m.waitHAMemberReady(ctx, config, source, config.StartupTimeout); err != nil {
		return HASnapshotResult{}, err
	}
	sourceStopped = false
	readyCtx, cancelReady := context.WithTimeout(ctx, config.StartupTimeout)
	_, readyErr := m.waitHAClusterReady(readyCtx, config)
	cancelReady()
	if readyErr != nil {
		return HASnapshotResult{}, readyErr
	}
	if err := syncHADirectory(temporary); err != nil {
		return HASnapshotResult{}, fmt.Errorf("durably flush HA snapshot package: %w", err)
	}
	if err := os.Rename(temporary, output); err != nil {
		return HASnapshotResult{}, fmt.Errorf("publish HA snapshot: %w", err)
	}
	result = HASnapshotResult{
		Path:       output,
		Bytes:      snapshotMetadata.Size + tokenMetadata.Size,
		DataSHA256: snapshotMetadata.SHA256,
		CreatedAt:  manifest.CreatedAt,
		Manifest:   manifest,
	}
	if err := syncHADirectory(parent); err != nil {
		return result, fmt.Errorf("durably publish HA snapshot package at %q: %w", output, err)
	}
	return result, nil
}

// RestoreHA validates the complete external package and the exact saved HA
// identity before any runtime mutation. It then follows K3s's documented
// three-server restore sequence and requires every member to become Ready.
func (m *Manager) RestoreHA(ctx context.Context, name, input string, timeout time.Duration) (result HAState, err error) {
	if !dnsLabel.MatchString(name) {
		return HAState{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	config, err := loadHAConfig(name)
	if err != nil {
		return HAState{}, err
	}
	if timeout < 0 {
		return HAState{}, fmt.Errorf("HA restore timeout must not be negative")
	}
	if timeout == 0 {
		timeout = config.StartupTimeout
	}
	if timeout < time.Second {
		return HAState{}, fmt.Errorf("HA restore timeout must be at least 1s")
	}
	config.StartupTimeout = timeout
	restoreCtx, cancelRestore := context.WithTimeout(ctx, timeout)
	defer cancelRestore()
	lock, err := acquireHAOperationLock(restoreCtx, config.Name)
	if err != nil {
		return HAState{}, err
	}
	defer func() { err = errors.Join(err, lock.release()) }()
	manifest, input, err := validateHASnapshot(restoreCtx, config, input)
	if err != nil {
		return HAState{}, err
	}
	if err := restoreCtx.Err(); err != nil {
		return HAState{}, fmt.Errorf("HA restore deadline reached while validating snapshot: %w", err)
	}
	preflightCtx, cancelPreflight := context.WithTimeout(restoreCtx, haRecoveryOperationTimeout)
	preflight, err := m.preflightHA(preflightCtx, config, true)
	cancelPreflight()
	if err != nil {
		return HAState{}, err
	}
	if len(preflight.volumeExists) != haMemberCount {
		return HAState{}, fmt.Errorf("HA restore requires all three exact APC-owned member volumes; found %d", len(preflight.volumeExists))
	}
	if !preflight.networkExists {
		return HAState{}, fmt.Errorf("HA restore requires the exact APC-owned network %q before any member mutation", config.NetworkName)
	}
	if err := m.cleanupStaleHARestoreHelpers(restoreCtx, config, input); err != nil {
		return HAState{}, err
	}
	if err := m.ensureNoHARecoveryHelpers(restoreCtx, config); err != nil {
		return HAState{}, err
	}
	now := time.Now().UTC()
	recoveryState := HARecoveryState{
		APIVersion:      haSnapshotAPIVersion,
		Kind:            haRecoveryStateKind,
		Cluster:         config.Name,
		ClusterIdentity: manifest.Cluster.Identity,
		SnapshotPath:    input,
		Phase:           "validated",
		StartedAt:       now,
		UpdatedAt:       now,
		NextAction:      "stop all three HA members",
	}
	if err := saveHARecoveryState(recoveryState); err != nil {
		return HAState{}, err
	}
	// The durable nonterminal journal must exist before changing desired state.
	// If persisting Running is ambiguous or the process dies here, the
	// supervisor sees the journal and fails closed instead of starting ordinary
	// K3s members outside the restore sequence.
	if err := markHAClusterRunningLocked(config.Name); err != nil {
		return HAState{}, fmt.Errorf("persist running HA intent before restore: %w", err)
	}
	if err := restoreCtx.Err(); err != nil {
		return HAState{}, fmt.Errorf("HA restore deadline reached before member mutation: %w", err)
	}
	progress := haRestoreProgress{peerDataCleared: map[int]bool{}}
	defer func() {
		if err == nil || !progress.mutationStarted {
			return
		}
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), haRecoveryAttemptTimeout)
		defer cancel()
		recoveryErr := m.recoverFailedHARestore(recoveryCtx, config, input, &progress)
		recoveryState.Phase = "failed"
		recoveryState.UpdatedAt = time.Now().UTC()
		recoveryState.RecoveryAttempted = true
		recoveryState.RecoverySucceeded = recoveryErr == nil
		recoveryState.Message = err.Error()
		if recoveryErr == nil {
			recoveryState.NextAction = "verify workloads and create a fresh HA snapshot"
		} else {
			recoveryState.NextAction = "inspect this recovery state and keep all member volumes; rerun restore after resolving the reported error"
			err = errors.Join(err, fmt.Errorf("automatic HA recovery attempt failed: %w", recoveryErr))
		}
		if stateErr := saveHARecoveryState(recoveryState); stateErr != nil {
			err = errors.Join(err, fmt.Errorf("persist failed HA recovery state: %w", stateErr))
		}
	}()

	if err := updateHARecoveryPhase(&recoveryState, "stopping-members", "reset member 1 from the validated K3s snapshot"); err != nil {
		return HAState{}, err
	}
	progress.mutationStarted = true
	for index := len(config.Members) - 1; index >= 0; index-- {
		member := config.Members[index]
		record, exists := preflight.containerRecord[member.ID]
		if !exists || strings.EqualFold(record.Status.State, "stopped") {
			continue
		}
		if err := m.runHABounded(restoreCtx, fmt.Sprintf("stop HA restore member %d", member.ID), "stop", HAContainerName(config.Name, member.ID)); err != nil {
			return HAState{}, err
		}
	}

	seed := memberByID(config.Members, 1)
	if err := updateHARecoveryPhase(&recoveryState, "resetting-member-1", "start restored member 1 normally"); err != nil {
		return HAState{}, err
	}
	if err := m.deleteStoppedHAMemberForRecovery(restoreCtx, config, seed); err != nil {
		return HAState{}, err
	}
	progress.resetAttempted = true
	resetSpec := restoreResetHelperSpec(config, seed, input)
	if err := m.runHARestoreReset(restoreCtx, config, seed, resetSpec); err != nil {
		return HAState{}, err
	}
	progress.resetComplete = true
	if err := updateHARecoveryPhase(&recoveryState, "starting-member-1", "clear stale etcd data on members 2 and 3"); err != nil {
		return HAState{}, err
	}
	if err := m.startHAMemberForRecovery(restoreCtx, config, seed); err != nil {
		return HAState{}, err
	}
	if err := m.waitHAMemberReady(restoreCtx, config, seed, config.StartupTimeout); err != nil {
		return HAState{}, err
	}

	if err := updateHARecoveryPhase(&recoveryState, "rejoining-peers", "wait for all three restored members"); err != nil {
		return HAState{}, err
	}
	for _, peer := range config.Members[1:] {
		if err := m.clearHAPeerDataForRecovery(restoreCtx, config, peer); err != nil {
			return HAState{}, err
		}
		progress.peerDataCleared[peer.ID] = true
		if err := m.startHAMemberForRecovery(restoreCtx, config, peer); err != nil {
			return HAState{}, err
		}
		if err := m.waitHAMemberReady(restoreCtx, config, peer, config.StartupTimeout); err != nil {
			return HAState{}, err
		}
	}

	if err := updateHARecoveryPhase(&recoveryState, "verifying", "none"); err != nil {
		return HAState{}, err
	}
	state, err := m.waitHAClusterReady(restoreCtx, config)
	if err != nil {
		return HAState{}, err
	}
	kubeconfig, err := m.readKubeconfig(restoreCtx, HAContainerName(config.Name, seed.ID), seed.apiEndpoint(config.ListenAddress))
	if err != nil {
		return HAState{}, err
	}
	if err := writePrivateFileAtomic(config.KubeconfigPath, kubeconfig); err != nil {
		return HAState{}, fmt.Errorf("refresh restored HA kubeconfig: %w", err)
	}
	state.Kubeconfig = config.KubeconfigPath
	recoveryState.Phase = "completed"
	recoveryState.UpdatedAt = time.Now().UTC()
	recoveryState.CompletedAt = recoveryState.UpdatedAt
	recoveryState.RecoverySucceeded = true
	recoveryState.Message = "all three K3s embedded-etcd members are Ready"
	recoveryState.NextAction = "verify workloads and create a fresh HA snapshot"
	if err := saveHARecoveryState(recoveryState); err != nil {
		return HAState{}, err
	}
	return state, nil
}

type haRecoveryHelperSpec struct {
	Name            string
	Role            string
	Member          HAMember
	Image           string
	Executable      string
	Arguments       []string
	CapAdd          []string
	CPUs            int
	Memory          string
	VolumeName      string
	VolumeMount     string
	BackupDirectory string
	BackupReadOnly  bool
	TokenDirectory  string
	NetworkName     string
}

func snapshotHelperSpec(config HAConfig, member HAMember, output string) haRecoveryHelperSpec {
	return haRecoveryHelperSpec{
		Name:            HASnapshotHelperContainerName(config.Name),
		Role:            "ha-snapshot-copy",
		Member:          member,
		Image:           backupImage,
		Executable:      "sleep",
		Arguments:       []string{"300"},
		CPUs:            1,
		Memory:          "512M",
		VolumeName:      HAVolumeName(config.Name, member.ID),
		VolumeMount:     haRecoveryDataMount,
		BackupDirectory: output,
	}
}

func restoreResetHelperSpec(config HAConfig, member HAMember, input string) haRecoveryHelperSpec {
	return haRecoveryHelperSpec{
		Name:            HARestoreResetContainerName(config.Name),
		Role:            "ha-restore-reset",
		Member:          member,
		Image:           config.Image,
		Executable:      "/bin/sh",
		Arguments:       haRestoreResetInitArguments(config, member),
		CapAdd:          []string{"ALL"},
		CPUs:            config.CPUs,
		Memory:          config.Memory,
		VolumeName:      HAVolumeName(config.Name, member.ID),
		VolumeMount:     "/var/lib/rancher/k3s",
		BackupDirectory: input,
		BackupReadOnly:  true,
		NetworkName:     config.NetworkName,
	}
}

func restoreClearHelperSpec(config HAConfig, member HAMember) haRecoveryHelperSpec {
	return haRecoveryHelperSpec{
		Name:        HARestoreClearContainerName(config.Name, member.ID),
		Role:        "ha-restore-clear",
		Member:      member,
		Image:       backupImage,
		Executable:  "sleep",
		Arguments:   []string{"300"},
		CPUs:        1,
		Memory:      "512M",
		VolumeName:  HAVolumeName(config.Name, member.ID),
		VolumeMount: haRecoveryDataMount,
	}
}

func (m *Manager) startHARecoveryHelper(ctx context.Context, spec haRecoveryHelperSpec) error {
	if err := validateHARecoveryHelperSpec(spec); err != nil {
		return err
	}
	if record, err := m.inspectHARecoveryContainer(ctx, spec.Name); err == nil {
		if ownershipErr := validateHARecoveryHelper(record, spec); ownershipErr != nil {
			return ownershipErr
		}
		return fmt.Errorf("HA recovery helper %q already exists", spec.Name)
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	arguments := []string{
		"run", "--detach", "--name", spec.Name,
		"--arch", "arm64", "--cpus", strconv.Itoa(spec.CPUs), "--memory", spec.Memory,
		"--volume", spec.VolumeName + ":" + spec.VolumeMount,
	}
	network := "default,mtu=1280"
	if spec.NetworkName != "" {
		network = fmt.Sprintf("%s,mac=%s,mtu=1280", spec.NetworkName, spec.Member.MAC)
	}
	arguments = append(arguments, "--network", network)
	for _, capability := range spec.CapAdd {
		arguments = append(arguments, "--cap-add", capability)
	}
	if spec.BackupDirectory != "" {
		mount := fmt.Sprintf("type=bind,source=%s,target=/backup", spec.BackupDirectory)
		if spec.BackupReadOnly {
			mount += ",readonly"
		}
		arguments = append(arguments, "--mount", mount)
	}
	if spec.TokenDirectory != "" {
		arguments = append(arguments, "--mount", fmt.Sprintf("type=bind,source=%s,target=/run/secrets/apc,readonly", spec.TokenDirectory))
	}
	arguments = append(arguments,
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster="+clusterNameFromRecoveryHelper(spec.Name),
		"--label", "apc.dev/role="+spec.Role,
		"--label", "apc.dev/member="+strconv.Itoa(spec.Member.ID),
		"--progress", "plain",
		spec.Image,
	)
	arguments = append(arguments, spec.Executable)
	arguments = append(arguments, spec.Arguments...)
	_, _, err := m.runHARecoveryCommand(ctx, "start HA recovery helper "+spec.Name, arguments...)
	return err
}

func (m *Manager) deleteHARecoveryHelper(ctx context.Context, spec haRecoveryHelperSpec) error {
	record, err := m.inspectHARecoveryContainer(ctx, spec.Name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateHARecoveryHelper(record, spec); err != nil {
		return err
	}
	if !strings.EqualFold(record.Status.State, "stopped") {
		stopErr := m.runHABounded(ctx, "stop HA recovery helper "+spec.Name, "stop", spec.Name)
		record, err = m.inspectHARecoveryContainer(ctx, spec.Name)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return errors.Join(stopErr, fmt.Errorf("reconcile stopped HA recovery helper %q: %w", spec.Name, err))
		}
		if validationErr := validateHARecoveryHelper(record, spec); validationErr != nil {
			return errors.Join(stopErr, validationErr)
		}
		if !strings.EqualFold(record.Status.State, "stopped") {
			return errors.Join(stopErr, fmt.Errorf("HA recovery helper %q remains in runtime state %q after stop", spec.Name, record.Status.State))
		}
	}
	deleteErr := m.runHABounded(ctx, "delete HA recovery helper "+spec.Name, "delete", spec.Name)
	record, err = m.inspectHARecoveryContainer(ctx, spec.Name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Join(deleteErr, fmt.Errorf("reconcile deleted HA recovery helper %q: %w", spec.Name, err))
	}
	if validationErr := validateHARecoveryHelper(record, spec); validationErr != nil {
		return errors.Join(deleteErr, validationErr)
	}
	return errors.Join(deleteErr, fmt.Errorf("HA recovery helper %q still exists after delete", spec.Name))
}

func validateHARecoveryHelperSpec(spec haRecoveryHelperSpec) error {
	if spec.Name == "" || spec.Role == "" || spec.Member.ID < 1 || spec.Member.ID > haMemberCount || spec.Executable == "" || spec.VolumeName == "" || !pathpkg.IsAbs(spec.VolumeMount) || spec.CPUs < 1 || spec.Memory == "" {
		return fmt.Errorf("invalid HA recovery helper specification")
	}
	for _, hostPath := range []string{spec.BackupDirectory, spec.TokenDirectory} {
		if hostPath != "" {
			if !filepath.IsAbs(hostPath) || strings.ContainsAny(hostPath, ",\x00\r\n") {
				return fmt.Errorf("HA recovery helper host paths must be absolute and mount-safe")
			}
		}
	}
	return nil
}

func clusterNameFromRecoveryHelper(name string) string {
	value := strings.TrimPrefix(name, "apc-k3s-")
	for _, suffix := range []string{"-ha-snapshot-copy", "-ha-restore-reset", "-ha-restore-clear-1", "-ha-restore-clear-2", "-ha-restore-clear-3"} {
		if strings.HasSuffix(value, suffix) {
			return strings.TrimSuffix(value, suffix)
		}
	}
	return ""
}

func (m *Manager) inspectHARecoveryContainer(ctx context.Context, name string) (haContainerInspect, error) {
	operationCtx, cancel := context.WithTimeout(ctx, haRuntimeOperationTimeout)
	defer cancel()
	return m.inspectHAContainer(operationCtx, name)
}

func validateHARecoveryHelper(record haContainerInspect, spec haRecoveryHelperSpec) error {
	clusterName := clusterNameFromRecoveryHelper(spec.Name)
	if clusterName == "" {
		return fmt.Errorf("invalid APC HA recovery helper name %q", spec.Name)
	}
	if err := validateHALabels(record.Configuration.Labels, clusterName, spec.Role, spec.Member.ID); err != nil {
		return fmt.Errorf("helper %q: %w", spec.Name, err)
	}
	if record.Configuration.Image.Reference != spec.Image {
		return fmt.Errorf("helper %q uses image %q, expected %q", spec.Name, record.Configuration.Image.Reference, spec.Image)
	}
	if record.Configuration.Platform.OS != "linux" || record.Configuration.Platform.Architecture != "arm64" {
		return fmt.Errorf("helper %q does not use the exact linux/arm64 platform", spec.Name)
	}
	process := record.Configuration.InitProcess
	if process.Executable != spec.Executable || !reflect.DeepEqual(process.Arguments, spec.Arguments) || process.Terminal || process.User.ID == nil || process.User.ID.UID != 0 || process.User.ID.GID != 0 {
		return fmt.Errorf("helper %q does not match the exact root, non-TTY APC recovery process", spec.Name)
	}
	if !slices.Equal(record.Configuration.CapAdd, spec.CapAdd) || len(record.Configuration.CapDrop) != 0 {
		return fmt.Errorf("helper %q does not use the exact APC recovery capabilities", spec.Name)
	}
	memoryBytes, memoryErr := parseHAByteSize(spec.Memory)
	if memoryErr != nil || record.Configuration.Resources.CPUs != spec.CPUs || uint64(record.Configuration.Resources.MemoryInBytes) != memoryBytes {
		return fmt.Errorf("helper %q does not match the exact APC recovery resources", spec.Name)
	}
	volumeMatches := false
	backupMatches := spec.BackupDirectory == ""
	tokenMatches := spec.TokenDirectory == ""
	expectedMounts := 1
	if spec.BackupDirectory != "" {
		expectedMounts++
	}
	if spec.TokenDirectory != "" {
		expectedMounts++
	}
	if len(record.Configuration.Mounts) != expectedMounts {
		return fmt.Errorf("helper %q has %d configured mounts, expected exactly %d", spec.Name, len(record.Configuration.Mounts), expectedMounts)
	}
	for _, mount := range record.Configuration.Mounts {
		if mount.Destination == spec.VolumeMount && mount.Type.Volume != nil && mount.Type.Volume.Name == spec.VolumeName && mount.Type.VirtioFS == nil && len(mount.Options) == 0 && !volumeMatches {
			volumeMatches = true
		}
		if mount.Destination == "/backup" && mount.Source == spec.BackupDirectory && mount.Type.VirtioFS != nil && mount.Type.Volume == nil && !backupMatches {
			expectedOptions := []string(nil)
			if spec.BackupReadOnly {
				expectedOptions = []string{"ro"}
			}
			backupMatches = slices.Equal(mount.Options, expectedOptions)
		}
		if mount.Destination == "/run/secrets/apc" && mount.Source == spec.TokenDirectory && mount.Type.VirtioFS != nil && mount.Type.Volume == nil && slices.Equal(mount.Options, []string{"ro"}) && !tokenMatches {
			tokenMatches = true
		}
	}
	if !volumeMatches || !backupMatches || !tokenMatches {
		return fmt.Errorf("helper %q does not use the exact protected member volume and backup/token mounts", spec.Name)
	}
	if spec.NetworkName != "" {
		if len(record.Configuration.Networks) != 1 {
			return fmt.Errorf("helper %q has %d configured networks, expected exactly one recovery network", spec.Name, len(record.Configuration.Networks))
		}
		networkMatches := false
		for _, network := range record.Configuration.Networks {
			if network.Network == spec.NetworkName && strings.EqualFold(network.Options.MACAddress, spec.Member.MAC) && network.Options.MTU == 1280 {
				networkMatches = true
				break
			}
		}
		if !networkMatches {
			return fmt.Errorf("helper %q does not use the exact recovery network identity", spec.Name)
		}
	} else {
		if len(record.Configuration.Networks) != 1 {
			return fmt.Errorf("helper %q has %d configured networks, expected exactly the default recovery network", spec.Name, len(record.Configuration.Networks))
		}
		network := record.Configuration.Networks[0]
		if network.Network != "default" || network.Options.MACAddress != "" || network.Options.MTU != 1280 {
			return fmt.Errorf("helper %q does not use the exact default recovery network identity", spec.Name)
		}
	}
	if len(record.Configuration.PublishedPorts) != 0 || len(record.Configuration.PublishedSockets) != 0 {
		return fmt.Errorf("helper %q unexpectedly publishes a port or socket", spec.Name)
	}
	if record.Configuration.ReadOnly || record.Configuration.Rosetta || record.Configuration.SSH || record.Configuration.UseInit || record.Configuration.Virtualization || len(record.Configuration.Sysctls) != 0 {
		return fmt.Errorf("helper %q enables an unexpected runtime feature", spec.Name)
	}
	return nil
}

func (m *Manager) runHARecoveryCommand(ctx context.Context, operation string, arguments ...string) ([]byte, []byte, error) {
	operationCtx, cancel := context.WithTimeout(ctx, haRecoveryOperationTimeout)
	defer cancel()
	stdout, stderr, err := m.runner.Run(operationCtx, m.binary, arguments...)
	if err == nil {
		return stdout, stderr, nil
	}
	if errors.Is(operationCtx.Err(), context.DeadlineExceeded) {
		return stdout, stderr, fmt.Errorf("%s timed out after %s: %w", operation, haRecoveryOperationTimeout, context.DeadlineExceeded)
	}
	return stdout, stderr, commandError(operation, stderr, err)
}

// runHARecoveryCommandWithinContext lets a restore reset use the caller's
// remaining total restore deadline instead of imposing a second fixed cap.
// RestoreHA always supplies a deadline-bearing context here.
func (m *Manager) runHARecoveryCommandWithinContext(ctx context.Context, operation string, arguments ...string) ([]byte, []byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, nil, fmt.Errorf("%s requires a bounded restore context", operation)
	}
	stdout, stderr, err := m.runner.Run(ctx, m.binary, arguments...)
	if contextErr := ctx.Err(); contextErr != nil {
		return stdout, stderr, fmt.Errorf("%s exceeded the remaining HA restore deadline: %w", operation, contextErr)
	}
	if err == nil {
		return stdout, stderr, nil
	}
	return stdout, stderr, commandError(operation, stderr, err)
}

func (m *Manager) findCreatedHASnapshot(ctx context.Context, config HAConfig, member HAMember, prefix string) (string, error) {
	stdout, _, err := m.runHARecoveryCommand(ctx, "locate newly created K3s snapshot",
		"exec", HAContainerName(config.Name, member.ID),
		"find", haSnapshotGuestDirectory, "-maxdepth", "1", "-type", "f", "-name", prefix+"*",
	)
	if err != nil {
		return "", err
	}
	var matches []string
	for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			matches = append(matches, line)
		}
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("K3s snapshot discovery returned %d files for prefix %q", len(matches), prefix)
	}
	guestPath := pathpkg.Clean(matches[0])
	if pathpkg.Dir(guestPath) != haSnapshotGuestDirectory || !strings.HasPrefix(pathpkg.Base(guestPath), prefix) || !validHAArtifactName(pathpkg.Base(guestPath)) {
		return "", fmt.Errorf("K3s returned unsafe snapshot path %q", matches[0])
	}
	return guestPath, nil
}

func newHASnapshotName(clusterName string) (string, error) {
	random := make([]byte, 6)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return "", fmt.Errorf("generate HA snapshot name: %w", err)
	}
	name := fmt.Sprintf("apc-%s-%s-%s", clusterName, time.Now().UTC().Format("20060102t150405z"), hex.EncodeToString(random))
	if !validHAArtifactName(name) {
		return "", fmt.Errorf("generated invalid HA snapshot name")
	}
	return name, nil
}

func (m *Manager) runHARestoreReset(ctx context.Context, config HAConfig, member HAMember, spec haRecoveryHelperSpec) (err error) {
	if err := validateHARecoveryHelperSpec(spec); err != nil {
		return err
	}
	if _, inspectErr := m.inspectHARecoveryContainer(ctx, spec.Name); inspectErr == nil {
		return fmt.Errorf("HA restore reset helper %q already exists", spec.Name)
	} else if !errors.Is(inspectErr, ErrNotFound) {
		return inspectErr
	}
	arguments := haRestoreResetRunArguments(config, member, spec)
	attempted := make([]string, 0, haRuntimeAddressRetryLimit)
	for attempt := 1; attempt <= haRuntimeAddressRetryLimit; attempt++ {
		_, stderr, runErr := m.runHARecoveryCommandWithinContext(ctx, "reset K3s embedded-etcd member 1 from snapshot", arguments...)
		cleanupErr := m.deleteHARecoveryHelper(ctx, spec)
		collision := haRuntimeIPCollisionFromOutput(stderr, config, member)
		if collision == nil {
			if runErr != nil {
				return errors.Join(runErr, cleanupErr)
			}
			return cleanupErr
		}
		attempted = append(attempted, collision.address)
		if cleanupErr != nil {
			return errors.Join(fmt.Errorf("reset K3s embedded-etcd member 1: %w", collision), cleanupErr)
		}
	}
	return fmt.Errorf(
		"reset K3s embedded-etcd member 1 could not obtain a non-reserved runtime IPv4 after %d attempts (allocated: %s); apple/container 1.0 does not support fixed IPv4 on container run; retry after checking the dedicated network for foreign attachments",
		haRuntimeAddressRetryLimit,
		strings.Join(attempted, ", "),
	)
}

func haRestoreResetRunArguments(config HAConfig, member HAMember, spec haRecoveryHelperSpec) []string {
	arguments := []string{
		"run", "--name", spec.Name,
		"--arch", "arm64", "--cpus", strconv.Itoa(config.CPUs), "--memory", config.Memory,
		"--network", fmt.Sprintf("%s,mac=%s,mtu=1280", config.NetworkName, member.MAC),
		"--entrypoint", spec.Executable,
		"--volume", spec.VolumeName + ":" + spec.VolumeMount,
		"--mount", fmt.Sprintf("type=bind,source=%s,target=%s,readonly", spec.BackupDirectory, haRecoveryBackupMount),
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster=" + config.Name,
		"--label", "apc.dev/role=" + spec.Role,
		"--label", "apc.dev/member=" + strconv.Itoa(member.ID),
		"--progress", "plain",
	}
	for _, capability := range spec.CapAdd {
		arguments = append(arguments, "--cap-add", capability)
	}
	arguments = append(arguments, config.Image)
	arguments = append(arguments, spec.Arguments...)
	return arguments
}

func haRestoreResetInitArguments(config HAConfig, member HAMember) []string {
	arguments := haInitArguments(config, member)
	result := make([]string, 0, len(arguments)+4)
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--cluster-init" {
			continue
		}
		if argument == "--token-file" && index+1 < len(arguments) {
			result = append(result, argument, pathpkg.Join(haRecoveryBackupMount, haSnapshotTokenFilename))
			index++
			continue
		}
		result = append(result, argument)
	}
	return append(result,
		"--cluster-reset",
		"--cluster-reset-restore-path", pathpkg.Join(haRecoveryBackupMount, haSnapshotDataFilename),
		"--etcd-s3=false",
	)
}

func (m *Manager) deleteStoppedHAMemberForRecovery(ctx context.Context, config HAConfig, member HAMember) error {
	record, err := m.inspectHARecoveryContainer(ctx, HAContainerName(config.Name, member.ID))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateHAContainer(record, config, member); err != nil {
		return err
	}
	if !strings.EqualFold(record.Status.State, "stopped") {
		return fmt.Errorf("refusing to delete HA member %d because it is not stopped", member.ID)
	}
	return m.runHABounded(ctx, fmt.Sprintf("delete stopped HA restore member %d envelope", member.ID), "delete", HAContainerName(config.Name, member.ID))
}

func (m *Manager) startHAMemberForRecovery(ctx context.Context, config HAConfig, member HAMember) error {
	record, err := m.inspectHARecoveryContainer(ctx, HAContainerName(config.Name, member.ID))
	switch {
	case err == nil:
		if err := validateHAContainer(record, config, member); err != nil {
			return err
		}
		if strings.EqualFold(record.Status.State, "running") {
			return nil
		}
		if err := m.runHABounded(ctx, fmt.Sprintf("delete stopped HA recovery member %d envelope", member.ID), "delete", HAContainerName(config.Name, member.ID)); err != nil {
			return err
		}
	case errors.Is(err, ErrNotFound):
	default:
		return err
	}
	return m.runHAServerEnvelope(ctx, config, member, fmt.Sprintf("start HA recovery member %d", member.ID), haRecoveryOperationTimeout)
}

func (m *Manager) clearHAPeerDataForRecovery(ctx context.Context, config HAConfig, member HAMember) (err error) {
	if member.ID == 1 {
		return fmt.Errorf("refusing to clear restored seed member data")
	}
	if err := m.deleteStoppedHAMemberForRecovery(ctx, config, member); err != nil {
		return err
	}
	spec := restoreClearHelperSpec(config, member)
	if err := m.startHARecoveryHelper(ctx, spec); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, m.deleteHARecoveryHelper(context.WithoutCancel(ctx), spec))
	}()
	if _, _, err := m.runHARecoveryCommand(ctx, fmt.Sprintf("clear stale embedded-etcd database on member %d", member.ID),
		"exec", spec.Name, "/bin/rm", "-rf", pathpkg.Join(spec.VolumeMount, "server/db"),
	); err != nil {
		return err
	}
	return nil
}

func (m *Manager) ensureNoHARecoveryHelpers(ctx context.Context, config HAConfig) error {
	specs := []haRecoveryHelperSpec{
		snapshotHelperSpec(config, config.Members[0], filepath.Dir(config.TokenFile)),
		restoreResetHelperSpec(config, config.Members[0], filepath.Dir(config.TokenFile)),
	}
	for _, peer := range config.Members[1:] {
		specs = append(specs, restoreClearHelperSpec(config, peer))
	}
	for _, spec := range specs {
		_, err := m.inspectHARecoveryContainer(ctx, spec.Name)
		if err == nil {
			return fmt.Errorf("refusing HA restore while recovery helper %q already exists", spec.Name)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	return nil
}

func (m *Manager) cleanupStaleHARestoreHelpers(ctx context.Context, config HAConfig, input string) error {
	specs := []haRecoveryHelperSpec{restoreResetHelperSpec(config, config.Members[0], input)}
	for _, peer := range config.Members[1:] {
		specs = append(specs, restoreClearHelperSpec(config, peer))
	}
	for _, spec := range specs {
		if err := m.deleteHARecoveryHelper(ctx, spec); err != nil {
			return fmt.Errorf("reconcile stale HA restore helper %q before retry: %w", spec.Name, err)
		}
	}
	return nil
}

func (m *Manager) recoverFailedHARestore(ctx context.Context, config HAConfig, input string, progress *haRestoreProgress) error {
	var cleanupErrors []error
	cleanupSpecs := []haRecoveryHelperSpec{
		restoreResetHelperSpec(config, config.Members[0], input),
	}
	for _, peer := range config.Members[1:] {
		cleanupSpecs = append(cleanupSpecs, restoreClearHelperSpec(config, peer))
	}
	for _, spec := range cleanupSpecs {
		if err := m.deleteHARecoveryHelper(ctx, spec); err != nil {
			cleanupErrors = append(cleanupErrors, err)
		}
	}
	if progress.resetAttempted && !progress.resetComplete {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("member 1 reset outcome is uncertain; all server members were intentionally left stopped to protect the member volumes"))
		return errors.Join(cleanupErrors...)
	}
	if !progress.resetAttempted {
		// An intact three-member etcd cluster cannot make the first server Ready
		// while the other two remain stopped. Reconcile every exact original
		// envelope first, then wait once for the restored quorum/cluster.
		for _, member := range config.Members {
			if err := m.startHAMemberForRecovery(ctx, config, member); err != nil {
				cleanupErrors = append(cleanupErrors, err)
			}
		}
		if len(cleanupErrors) != 0 {
			return errors.Join(cleanupErrors...)
		}
		readyCtx, cancel := context.WithTimeout(ctx, config.StartupTimeout)
		_, readyErr := m.waitHAClusterReady(readyCtx, config)
		cancel()
		cleanupErrors = append(cleanupErrors, readyErr)
		return errors.Join(cleanupErrors...)
	}

	seed := memberByID(config.Members, 1)
	if err := m.startHAMemberForRecovery(ctx, config, seed); err != nil {
		cleanupErrors = append(cleanupErrors, err)
		return errors.Join(cleanupErrors...)
	}
	if err := m.waitHAMemberReady(ctx, config, seed, config.StartupTimeout); err != nil {
		cleanupErrors = append(cleanupErrors, err)
		return errors.Join(cleanupErrors...)
	}
	for _, peer := range config.Members[1:] {
		if !progress.peerDataCleared[peer.ID] {
			if err := m.clearHAPeerDataForRecovery(ctx, config, peer); err != nil {
				cleanupErrors = append(cleanupErrors, err)
				continue
			}
			progress.peerDataCleared[peer.ID] = true
		}
		if err := m.startHAMemberForRecovery(ctx, config, peer); err != nil {
			cleanupErrors = append(cleanupErrors, err)
			continue
		}
		if err := m.waitHAMemberReady(ctx, config, peer, config.StartupTimeout); err != nil {
			cleanupErrors = append(cleanupErrors, err)
		}
	}
	if len(cleanupErrors) == 0 {
		readyCtx, cancel := context.WithTimeout(ctx, config.StartupTimeout)
		_, readyErr := m.waitHAClusterReady(readyCtx, config)
		cancel()
		cleanupErrors = append(cleanupErrors, readyErr)
	}
	return errors.Join(cleanupErrors...)
}

func prepareHASnapshotOutput(output string) (string, error) {
	if strings.TrimSpace(output) == "" {
		return "", fmt.Errorf("HA snapshot output directory is required")
	}
	abs, err := filepath.Abs(output)
	if err != nil {
		return "", fmt.Errorf("resolve HA snapshot output path: %w", err)
	}
	abs = filepath.Clean(abs)
	if filepath.Base(abs) == "." || abs == string(filepath.Separator) || strings.ContainsAny(abs, ",\x00\r\n") {
		return "", fmt.Errorf("HA snapshot output path is not mount-safe")
	}
	parent := filepath.Dir(abs)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create HA snapshot parent directory: %w", err)
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("resolve HA snapshot parent directory: %w", err)
	}
	abs = filepath.Join(resolvedParent, filepath.Base(abs))
	if _, err := os.Lstat(abs); err == nil {
		return "", fmt.Errorf("HA snapshot output %q already exists", abs)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect HA snapshot output: %w", err)
	}
	return abs, nil
}

func validateHASnapshot(ctx context.Context, config HAConfig, input string) (HASnapshotManifest, string, error) {
	if err := ctx.Err(); err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("validate HA snapshot: %w", err)
	}
	if strings.TrimSpace(input) == "" {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot input directory is required")
	}
	abs, err := filepath.Abs(input)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("resolve HA snapshot path: %w", err)
	}
	abs = filepath.Clean(abs)
	if strings.ContainsAny(abs, ",\x00\r\n") {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot path is not mount-safe")
	}
	lstat, err := os.Lstat(abs)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("inspect HA snapshot directory: %w", err)
	}
	if lstat.Mode()&os.ModeSymlink != 0 || !lstat.IsDir() {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot path must be a real directory, not a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("resolve HA snapshot directory: %w", err)
	}
	abs = resolved
	info, err := os.Stat(abs)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("read HA snapshot directory: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot directory permissions must be 0700 or stricter")
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("list HA snapshot directory: %w", err)
	}
	wantEntries := []string{haSnapshotDataFilename, haSnapshotManifestFilename, haSnapshotTokenFilename}
	gotEntries := make([]string, 0, len(entries))
	for _, entry := range entries {
		gotEntries = append(gotEntries, entry.Name())
	}
	sort.Strings(gotEntries)
	sort.Strings(wantEntries)
	if !reflect.DeepEqual(gotEntries, wantEntries) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot directory must contain exactly %s", strings.Join(wantEntries, ", "))
	}

	manifestPath := filepath.Join(abs, haSnapshotManifestFilename)
	manifestInfo, err := secureHAArtifactInfo(manifestPath)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("validate HA snapshot manifest: %w", err)
	}
	if manifestInfo.Mode().Perm()&0o222 != 0 {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot manifest must be immutable (mode 0400 or stricter)")
	}
	if manifestInfo.Size() > 1<<20 {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot manifest exceeds 1 MiB")
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("read HA snapshot manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(manifestData))
	decoder.DisallowUnknownFields()
	var manifest HASnapshotManifest
	if err := decoder.Decode(&manifest); err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("decode HA snapshot manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("decode HA snapshot manifest: %w", err)
	}
	if manifest.APIVersion != haSnapshotAPIVersion || manifest.Kind != haSnapshotKind || manifest.FormatVersion != haSnapshotFormatVersion {
		return HASnapshotManifest{}, "", fmt.Errorf("unsupported HA snapshot format")
	}
	if manifest.CreatedAt.IsZero() || manifest.CreatedAt.After(time.Now().UTC().Add(5*time.Minute)) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot manifest has an invalid creation time")
	}
	if manifest.Cluster.Name != config.Name {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot belongs to cluster %q, not %q", manifest.Cluster.Name, config.Name)
	}
	if !validSHA256(manifest.Cluster.Identity) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot contains an invalid cluster identity")
	}
	if !reflect.DeepEqual(manifest.Topology, haSnapshotTopology(config)) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot topology does not match saved cluster %q", config.Name)
	}
	if manifest.Image.Reference != config.Image || manifest.Image.Architecture != "arm64" {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot image identity does not match saved cluster %q", config.Name)
	}
	source := memberByID(config.Members, manifest.K3s.SourceMemberID)
	if source.NodeName == "" || source.NodeName != manifest.K3s.SourceNodeName || strings.TrimSpace(manifest.K3s.Version) == "" || !validHAArtifactName(manifest.K3s.SnapshotName) || manifest.K3s.CreatedAt.IsZero() || manifest.K3s.CreatedAt.After(time.Now().UTC().Add(5*time.Minute)) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot contains invalid K3s snapshot metadata")
	}
	if manifest.Snapshot.Name != haSnapshotDataFilename || manifest.ServerToken.Name != haSnapshotTokenFilename {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot manifest contains an unsafe artifact path")
	}
	if manifest.Snapshot.Size <= 0 || manifest.ServerToken.Size <= 0 || manifest.ServerToken.Size > haRecoveryTokenMaximum || !validSHA256(manifest.Snapshot.SHA256) || !validSHA256(manifest.ServerToken.SHA256) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot manifest contains invalid artifact metadata")
	}
	snapshotMaximum, err := haSnapshotArtifactMaximum(config)
	if err != nil {
		return HASnapshotManifest{}, "", err
	}
	if manifest.Snapshot.Size > snapshotMaximum {
		return HASnapshotManifest{}, "", fmt.Errorf("HA etcd snapshot size %d exceeds declared member volume bound %d", manifest.Snapshot.Size, snapshotMaximum)
	}
	actualSnapshot, err := hashHAArtifact(ctx, filepath.Join(abs, haSnapshotDataFilename), haSnapshotDataFilename, snapshotMaximum)
	if err != nil {
		return HASnapshotManifest{}, "", err
	}
	actualToken, err := hashHAArtifact(ctx, filepath.Join(abs, haSnapshotTokenFilename), haSnapshotTokenFilename, haRecoveryTokenMaximum)
	if err != nil {
		return HASnapshotManifest{}, "", err
	}
	if !sameHAFileMetadata(actualSnapshot, manifest.Snapshot) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA etcd snapshot checksum or size mismatch")
	}
	if !sameHAFileMetadata(actualToken, manifest.ServerToken) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA server token checksum or size mismatch")
	}
	backupToken, err := readStrictTokenFile(filepath.Join(abs, haSnapshotTokenFilename), true)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("validate backed-up HA server token: %w", err)
	}
	currentToken, err := readHARecoveryToken(config.TokenFile)
	if err != nil {
		return HASnapshotManifest{}, "", err
	}
	if !sameK3sServerToken(backupToken, currentToken) {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot server token does not match cluster %q", config.Name)
	}
	tokenCredential, err := k3sServerTokenCredential(backupToken)
	if err != nil {
		return HASnapshotManifest{}, "", fmt.Errorf("validate K3s server token binding: %w", err)
	}
	tokenDigest := sha256.Sum256(tokenCredential)
	expectedIdentity, err := haClusterIdentity(config, hex.EncodeToString(tokenDigest[:]))
	if err != nil {
		return HASnapshotManifest{}, "", err
	}
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(manifest.Cluster.Identity)), []byte(expectedIdentity)) != 1 {
		return HASnapshotManifest{}, "", fmt.Errorf("HA snapshot cluster identity does not match the explicit saved identity of cluster %q", config.Name)
	}
	return manifest, abs, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("multiple JSON documents are not allowed")
	}
	return err
}

func haSnapshotTopology(config HAConfig) HASnapshotTopology {
	members := append([]HAMember(nil), config.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return HASnapshotTopology{
		NetworkName:    config.NetworkName,
		Subnet:         config.Subnet,
		ListenAddress:  config.ListenAddress,
		CPUs:           config.CPUs,
		Memory:         config.Memory,
		VolumeSize:     config.VolumeSize,
		DisableTraefik: config.DisableTraefik,
		Members:        members,
	}
}

func haClusterIdentity(config HAConfig, tokenSHA256 string) (string, error) {
	payload := struct {
		Name        string                  `json:"name"`
		Topology    HASnapshotTopology      `json:"topology"`
		Image       HASnapshotImageMetadata `json:"image"`
		TokenSHA256 string                  `json:"tokenSHA256"`
	}{
		Name:        config.Name,
		Topology:    haSnapshotTopology(config),
		Image:       HASnapshotImageMetadata{Reference: config.Image, Architecture: "arm64"},
		TokenSHA256: strings.ToLower(tokenSHA256),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode HA cluster identity: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validHAArtifactName(value string) bool {
	if value == "" || len(value) > 255 || value == "." || value == ".." || strings.ContainsAny(value, "/\\\x00\r\n\t ") {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func syncHAFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(syncErr, closeErr)
}

func syncHADirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	info, statErr := directory.Stat()
	if statErr == nil && !info.IsDir() {
		statErr = fmt.Errorf("path is not a directory")
	}
	syncErr := error(nil)
	if statErr == nil {
		syncErr = directory.Sync()
	}
	closeErr := directory.Close()
	return errors.Join(statErr, syncErr, closeErr)
}

func secureHAArtifact(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect HA snapshot artifact: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("HA snapshot artifact %q must be a regular file", filepath.Base(path))
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure HA snapshot artifact %q: %w", filepath.Base(path), err)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open HA snapshot artifact %q: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync HA snapshot artifact %q: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close HA snapshot artifact %q: %w", filepath.Base(path), err)
	}
	return nil
}

func secureHAArtifactInfo(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("artifact %q must be a regular file, not a symbolic link", filepath.Base(path))
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("artifact %q permissions must be 0600 or stricter", filepath.Base(path))
	}
	return info, nil
}

func haSnapshotArtifactMaximum(config HAConfig) (int64, error) {
	bytes, err := parseHAByteSize(config.VolumeSize)
	if err != nil {
		return 0, fmt.Errorf("derive HA snapshot size bound from member volume: %w", err)
	}
	maximumInt64 := uint64(^uint64(0) >> 1)
	if bytes > maximumInt64 {
		bytes = maximumInt64
	}
	return int64(bytes), nil
}

func hashHAArtifact(ctx context.Context, path, name string, maximumBytes int64) (HASnapshotFileMetadata, error) {
	if err := ctx.Err(); err != nil {
		return HASnapshotFileMetadata{}, fmt.Errorf("checksum HA snapshot artifact %q: %w", name, err)
	}
	if maximumBytes <= 0 {
		return HASnapshotFileMetadata{}, fmt.Errorf("HA snapshot artifact %q has an invalid size bound", name)
	}
	lstat, err := secureHAArtifactInfo(path)
	if err != nil {
		return HASnapshotFileMetadata{}, fmt.Errorf("validate HA snapshot artifact: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return HASnapshotFileMetadata{}, fmt.Errorf("open HA snapshot artifact %q: %w", name, err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return HASnapshotFileMetadata{}, fmt.Errorf("stat opened HA snapshot artifact %q: %w", name, err)
	}
	if !os.SameFile(lstat, openedInfo) || !openedInfo.Mode().IsRegular() {
		return HASnapshotFileMetadata{}, fmt.Errorf("HA snapshot artifact %q changed during validation", name)
	}
	if openedInfo.Size() > maximumBytes {
		return HASnapshotFileMetadata{}, fmt.Errorf("HA snapshot artifact %q exceeds maximum size %d", name, maximumBytes)
	}
	hash := sha256.New()
	buffer := make([]byte, 1<<20)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return HASnapshotFileMetadata{}, fmt.Errorf("checksum HA snapshot artifact %q: %w", name, err)
		}
		count, readErr := file.Read(buffer)
		if count > 0 {
			if int64(count) > maximumBytes-size {
				return HASnapshotFileMetadata{}, fmt.Errorf("HA snapshot artifact %q exceeds maximum size %d", name, maximumBytes)
			}
			_, _ = hash.Write(buffer[:count])
			size += int64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return HASnapshotFileMetadata{}, fmt.Errorf("checksum HA snapshot artifact %q: %w", name, readErr)
		}
	}
	if size != openedInfo.Size() {
		return HASnapshotFileMetadata{}, fmt.Errorf("HA snapshot artifact %q changed size during validation", name)
	}
	return HASnapshotFileMetadata{Name: name, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func sameHAFileMetadata(actual, expected HASnapshotFileMetadata) bool {
	return actual.Name == expected.Name && actual.Size == expected.Size && subtle.ConstantTimeCompare([]byte(actual.SHA256), []byte(strings.ToLower(expected.SHA256))) == 1
}

func readStrictTokenFile(path string, requireProtected bool) ([]byte, error) {
	if requireProtected {
		if _, err := secureHAArtifactInfo(path); err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read token file: %w", err)
	}
	if len(data) == 0 || len(data) > 4096 {
		return nil, fmt.Errorf("token file must contain one non-empty token")
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("token file must contain one non-empty token without whitespace")
	}
	return []byte(token), nil
}

// k3sServerTokenCredential normalizes K3s's on-disk secure token and APC's
// bootstrap short token to the credential used to encrypt bootstrap data. K3s
// always writes the former as K10<CA-SHA256>::server:<secret>, while the first
// self-signed server must be bootstrapped with the latter because its CA does
// not exist yet.
func k3sServerTokenCredential(token []byte) ([]byte, error) {
	value := strings.TrimSpace(string(token))
	if value == "" || strings.ContainsAny(value, " \t\r\n") {
		return nil, fmt.Errorf("server token is empty or contains whitespace")
	}
	if strings.HasPrefix(value, "K10") {
		parts := strings.SplitN(value, "::", 2)
		if len(parts) != 2 || len(parts[0]) != 3+sha256.Size*2 {
			return nil, fmt.Errorf("server token has an invalid K3s secure-token prefix")
		}
		if _, err := hex.DecodeString(parts[0][3:]); err != nil {
			return nil, fmt.Errorf("server token has an invalid K3s CA hash")
		}
		value = parts[1]
		if !strings.HasPrefix(value, "server:") {
			return nil, fmt.Errorf("K3s secure token does not contain server credentials")
		}
	}
	value = strings.TrimPrefix(value, "server:")
	if value == "" || strings.ContainsAny(value, " \t\r\n") {
		return nil, fmt.Errorf("server token has an empty credential")
	}
	return []byte(value), nil
}

func sameK3sServerToken(left, right []byte) bool {
	leftCredential, leftErr := k3sServerTokenCredential(left)
	rightCredential, rightErr := k3sServerTokenCredential(right)
	return leftErr == nil && rightErr == nil && subtle.ConstantTimeCompare(leftCredential, rightCredential) == 1
}

func readHARecoveryToken(path string) ([]byte, error) {
	if err := validatePrivateTokenFile(path); err != nil {
		return nil, fmt.Errorf("validate protected HA server token: %w", err)
	}
	lstat, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect protected HA server token: %w", err)
	}
	if lstat.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("protected HA server token must not be a symbolic link")
	}
	return readStrictTokenFile(path, true)
}

func saveHARecoveryState(state HARecoveryState) error {
	path, err := HARecoveryStatePath(state.Cluster)
	if err != nil {
		return err
	}
	if len(state.Message) > 4096 {
		state.Message = state.Message[:4096]
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode HA recovery state: %w", err)
	}
	if err := writePrivateFileAtomic(path, append(data, '\n')); err != nil {
		return fmt.Errorf("save HA recovery state: %w", err)
	}
	return nil
}

// LoadHARecoveryState reads the protected durable restore journal for CLI and
// operator diagnostics.
func LoadHARecoveryState(name string) (HARecoveryState, error) {
	path, err := HARecoveryStatePath(name)
	if err != nil {
		return HARecoveryState{}, err
	}
	data, err := readExactHARecoveryStateFile(path, openHARecoveryStateFile)
	if err != nil {
		return HARecoveryState{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state HARecoveryState
	if err := decoder.Decode(&state); err != nil {
		return HARecoveryState{}, fmt.Errorf("decode HA recovery state: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return HARecoveryState{}, fmt.Errorf("decode HA recovery state: %w", err)
	}
	if state.APIVersion != haSnapshotAPIVersion || state.Kind != haRecoveryStateKind || state.Cluster != name {
		return HARecoveryState{}, fmt.Errorf("HA recovery state identity does not match cluster %q", name)
	}
	if err := validateHARecoveryState(state); err != nil {
		return HARecoveryState{}, err
	}
	return state, nil
}

type haRecoveryStateOpenFunc func(string) (*os.File, error)

func openHARecoveryStateFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

func readExactHARecoveryStateFile(path string, openFile haRecoveryStateOpenFunc) (data []byte, err error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read HA recovery state: %w", err)
	}
	if err := validateHARecoveryStateFileInfo(pathInfo); err != nil {
		return nil, err
	}
	file, err := openFile(path)
	if err != nil {
		return nil, fmt.Errorf("open HA recovery state without following links: %w", err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened HA recovery state: %w", err)
	}
	if err := validateHARecoveryStateFileInfo(openedInfo); err != nil {
		return nil, err
	}
	if !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("HA recovery state changed while it was being opened")
	}
	data, err = io.ReadAll(io.LimitReader(file, haRecoveryStateMaximum+1))
	if err != nil {
		return nil, fmt.Errorf("read HA recovery state: %w", err)
	}
	if int64(len(data)) > haRecoveryStateMaximum {
		return nil, fmt.Errorf("HA recovery state exceeds maximum size %d", haRecoveryStateMaximum)
	}
	finalInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("reinspect HA recovery state after reading: %w", err)
	}
	if err := validateHARecoveryStateFileInfo(finalInfo); err != nil {
		return nil, err
	}
	if !os.SameFile(openedInfo, finalInfo) {
		return nil, fmt.Errorf("HA recovery state changed while it was being read")
	}
	return data, nil
}

func validateHARecoveryStateFileInfo(info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !haPathOwnedByEffectiveUser(info) {
		return fmt.Errorf("HA recovery state must be one private regular file owned by the current user")
	}
	return nil
}

func validateHARecoveryState(state HARecoveryState) error {
	knownPhase := false
	for _, phase := range []string{"validated", "stopping-members", "resetting-member-1", "starting-member-1", "rejoining-peers", "verifying", "failed", "completed"} {
		if state.Phase == phase {
			knownPhase = true
			break
		}
	}
	if !knownPhase {
		return fmt.Errorf("HA recovery state has unknown phase %q", state.Phase)
	}
	if strings.TrimSpace(state.ClusterIdentity) == "" {
		return fmt.Errorf("HA recovery state has an empty cluster identity")
	}
	if strings.TrimSpace(state.SnapshotPath) == "" || !filepath.IsAbs(state.SnapshotPath) || strings.ContainsAny(state.SnapshotPath, "\x00\r\n") {
		return fmt.Errorf("HA recovery state has an invalid snapshot path")
	}
	nowLimit := time.Now().UTC().Add(5 * time.Minute)
	if state.StartedAt.IsZero() || state.UpdatedAt.IsZero() || state.UpdatedAt.Before(state.StartedAt) || state.StartedAt.After(nowLimit) || state.UpdatedAt.After(nowLimit) {
		return fmt.Errorf("HA recovery state has invalid start/update timestamps")
	}
	switch state.Phase {
	case "completed":
		if !state.RecoverySucceeded || state.CompletedAt.IsZero() || state.CompletedAt.Before(state.UpdatedAt) || state.CompletedAt.After(nowLimit) {
			return fmt.Errorf("completed HA recovery state requires a successful, sane completion timestamp")
		}
	case "failed":
		if !state.RecoveryAttempted || !state.CompletedAt.IsZero() {
			return fmt.Errorf("failed HA recovery state has inconsistent recovery flags or completion timestamp")
		}
	default:
		if state.RecoveryAttempted || state.RecoverySucceeded || !state.CompletedAt.IsZero() {
			return fmt.Errorf("in-progress HA recovery state has inconsistent terminal fields")
		}
	}
	return nil
}

func updateHARecoveryPhase(state *HARecoveryState, phase, nextAction string) error {
	state.Phase = phase
	state.UpdatedAt = time.Now().UTC()
	state.NextAction = nextAction
	return saveHARecoveryState(*state)
}
