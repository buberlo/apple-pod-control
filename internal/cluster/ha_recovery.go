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
	haRestoreStageDirectory    = ".ha-restore-stage"
	haRecoveryStateKind        = "HARecoveryState"
	haRecoveryOperationTimeout = 2 * time.Minute
	haRecoveryAttemptTimeout   = 5 * time.Minute
	haRecoveryTokenMaximum     = int64(4096)
	haRecoveryStateMaximum     = int64(64 << 10)
	haResetDiagnosticInputMax  = 32 << 10
	haResetDiagnosticKeysMax   = 8
	haResetDiagnosticOutputMax = 256
)

const haRestoreTokenInstallScript = `umask 077; if [ "$#" -lt 3 ]; then echo "APC restore token installer configuration is invalid" >&2; exit 1; fi; APC_TOKEN_SOURCE=$1; APC_TOKEN_DIRECTORY=$2; shift 2; APC_TOKEN_DESTINATION=$APC_TOKEN_DIRECTORY/token; APC_TOKEN_TEMP=; apc_token_cleanup() { APC_TOKEN_STATUS=$?; trap - EXIT; if [ -n "$APC_TOKEN_TEMP" ]; then rm -f "$APC_TOKEN_TEMP" >/dev/null 2>&1; fi; exit "$APC_TOKEN_STATUS"; }; trap apc_token_cleanup EXIT; trap 'exit 1' HUP INT TERM; if [ -L "$APC_TOKEN_SOURCE" ] || [ ! -f "$APC_TOKEN_SOURCE" ] || [ ! -s "$APC_TOKEN_SOURCE" ]; then echo "APC restore token source validation failed" >&2; exit 1; fi; if [ -L "$APC_TOKEN_DIRECTORY" ] || { [ -e "$APC_TOKEN_DIRECTORY" ] && [ ! -d "$APC_TOKEN_DIRECTORY" ]; }; then echo "APC restore token directory validation failed" >&2; exit 1; fi; if ! mkdir -p "$APC_TOKEN_DIRECTORY" 2>/dev/null; then echo "APC restore token directory creation failed" >&2; exit 1; fi; if [ -L "$APC_TOKEN_DESTINATION" ] || { [ -e "$APC_TOKEN_DESTINATION" ] && [ ! -f "$APC_TOKEN_DESTINATION" ]; }; then echo "APC restore token destination validation failed" >&2; exit 1; fi; APC_TOKEN_TEMP=$(mktemp "$APC_TOKEN_DIRECTORY/.apc-server-token.XXXXXX" 2>/dev/null) || { echo "APC restore token temporary file creation failed" >&2; exit 1; }; if ! cat "$APC_TOKEN_SOURCE" >"$APC_TOKEN_TEMP" 2>/dev/null; then echo "APC restore token copy failed" >&2; exit 1; fi; if ! chmod 600 "$APC_TOKEN_TEMP" 2>/dev/null; then echo "APC restore token permission update failed" >&2; exit 1; fi; if ! sync "$APC_TOKEN_TEMP" 2>/dev/null; then echo "APC restore token flush failed" >&2; exit 1; fi; if ! mv -f "$APC_TOKEN_TEMP" "$APC_TOKEN_DESTINATION" 2>/dev/null; then echo "APC restore token publication failed" >&2; exit 1; fi; APC_TOKEN_TEMP=; if [ -L "$APC_TOKEN_DESTINATION" ] || [ ! -f "$APC_TOKEN_DESTINATION" ]; then echo "APC restore token publication validation failed" >&2; exit 1; fi; if ! sync "$APC_TOKEN_DIRECTORY" 2>/dev/null; then echo "APC restore token directory flush failed" >&2; exit 1; fi; trap - EXIT HUP INT TERM`

// HASnapshotManifest is the immutable, versioned description of one external
// K3s embedded-etcd snapshot. It intentionally contains topology metadata, not
// host paths from HAConfig. Snapshot and token contents are checksummed
// independently; Cluster.Identity binds the token credential to the exact
// topology and image. Empty-host recovery separately requires a trusted digest
// of the complete manifest so the snapshot checksum is externally anchored.
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
	Path           string             `json:"path"`
	Bytes          int64              `json:"bytes"`
	DataSHA256     string             `json:"dataSHA256"`
	ManifestSHA256 string             `json:"manifestSHA256"`
	CreatedAt      time.Time          `json:"createdAt"`
	Manifest       HASnapshotManifest `json:"manifest"`
	Warning        string             `json:"warning,omitempty"`
}

// haSnapshotPackage contains validated package metadata and the K3s server
// token. Token is deliberately private so it can never be emitted
// through CLI JSON/YAML output.
type haSnapshotPackage struct {
	Manifest       HASnapshotManifest
	Path           string
	ManifestSHA256 string
	token          []byte
	manifestInfo   os.FileInfo
	snapshotInfo   os.FileInfo
	tokenInfo      os.FileInfo
}

// HARecoveryState is persisted with mode 0600 before the first runtime
// mutation. A failed restore therefore leaves an explicit phase and next step
// even if the calling terminal disappears.
type HARecoveryState struct {
	APIVersion        string                      `json:"apiVersion"`
	Kind              string                      `json:"kind"`
	Cluster           string                      `json:"cluster"`
	ClusterIdentity   string                      `json:"clusterIdentity"`
	SnapshotPath      string                      `json:"snapshotPath"`
	Phase             string                      `json:"phase"`
	StartedAt         time.Time                   `json:"startedAt"`
	UpdatedAt         time.Time                   `json:"updatedAt"`
	CompletedAt       time.Time                   `json:"completedAt,omitempty"`
	RecoveryAttempted bool                        `json:"recoveryAttempted"`
	RecoverySucceeded bool                        `json:"recoverySucceeded"`
	Message           string                      `json:"message,omitempty"`
	NextAction        string                      `json:"nextAction,omitempty"`
	VolumeProvenance  *HARecoveryVolumeProvenance `json:"volumeProvenance,omitempty"`
}

// HARecoveryVolumeProvenance durably distinguishes volumes which could hold
// the intact pre-recovery etcd cluster from volumes RecoverHA had to create.
// The latter must never be started as an ordinary cluster before member 1 has
// successfully completed the snapshot reset.
type HARecoveryVolumeProvenance struct {
	PreexistingMemberIDs []int `json:"preexistingMemberIDs"`
	NewMemberIDs         []int `json:"newMemberIDs"`
}

type haRestoreProgress struct {
	mutationStarted  bool
	resetAttempted   bool
	resetComplete    bool
	newVolumeMembers map[int]bool
	peerDataCleared  map[int]bool
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
	manifestData = append(manifestData, '\n')
	manifestPath := filepath.Join(temporary, haSnapshotManifestFilename)
	if err := writePrivateFileAtomic(manifestPath, manifestData); err != nil {
		return HASnapshotResult{}, fmt.Errorf("write HA snapshot manifest: %w", err)
	}
	if err := os.Chmod(manifestPath, 0o400); err != nil {
		return HASnapshotResult{}, fmt.Errorf("make HA snapshot manifest immutable: %w", err)
	}
	if err := syncHAFile(manifestPath); err != nil {
		return HASnapshotResult{}, fmt.Errorf("flush HA snapshot manifest metadata: %w", err)
	}
	manifestMetadata, err := hashHAArtifact(ctx, manifestPath, haSnapshotManifestFilename, 1<<20)
	if err != nil {
		return HASnapshotResult{}, fmt.Errorf("fingerprint HA snapshot manifest: %w", err)
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
		Path:           output,
		Bytes:          snapshotMetadata.Size + tokenMetadata.Size,
		DataSHA256:     snapshotMetadata.SHA256,
		ManifestSHA256: manifestMetadata.SHA256,
		CreatedAt:      manifest.CreatedAt,
		Manifest:       manifest,
	}
	if err := syncHADirectory(parent); err != nil {
		return result, fmt.Errorf("durably publish HA snapshot package at %q: %w", output, err)
	}
	return result, nil
}

// RecoverHA reconstructs the exact saved local HA topology from a protected
// snapshot package after the on-host configuration, token, network, or member
// volumes have been lost. Unlike ordinary RestoreHA, recovery has no local
// token trust anchor, so the SHA-256 fingerprint of the immutable manifest is
// mandatory and must have been retained independently from the package.
func (m *Manager) RecoverHA(ctx context.Context, name, input, expectedManifestSHA256 string, timeout time.Duration) (result HAState, err error) {
	if !dnsLabel.MatchString(name) {
		return HAState{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	expectedManifestSHA256, err = normalizeExpectedHAManifestSHA256(expectedManifestSHA256)
	if err != nil {
		return HAState{}, err
	}
	defaults, err := DefaultHAConfig(name)
	if err != nil {
		return HAState{}, err
	}
	if timeout < 0 {
		return HAState{}, fmt.Errorf("HA recovery timeout must not be negative")
	}
	if timeout == 0 {
		timeout = defaults.StartupTimeout
	}
	if timeout < time.Second {
		return HAState{}, fmt.Errorf("HA recovery timeout must be at least 1s")
	}
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, timeout)
	defer cancelRecovery()
	// Authenticate the exact manifest bytes before creating even the persistent
	// operation-lock inode. Full package and target validation is repeated below
	// while holding the lock before any recovery state or runtime mutation.
	header, err := readHASnapshotManifest(input)
	if err != nil {
		return HAState{}, err
	}
	if subtle.ConstantTimeCompare([]byte(header.ManifestSHA256), []byte(expectedManifestSHA256)) != 1 {
		return HAState{}, fmt.Errorf("HA snapshot manifest fingerprint does not match the independently retained SHA-256")
	}
	if header.Manifest.Cluster.Name != name {
		return HAState{}, fmt.Errorf("HA snapshot belongs to cluster %q, not %q", header.Manifest.Cluster.Name, name)
	}
	if !immutableImageReference.MatchString(header.Manifest.Image.Reference) {
		return HAState{}, fmt.Errorf("HA empty-host recovery requires an immutable OCI sha256 digest image reference")
	}
	lock, err := acquireHAOperationLock(recoveryCtx, name)
	if err != nil {
		return HAState{}, err
	}
	defer func() { err = errors.Join(err, lock.release()) }()

	config, configExists, err := loadOrReconstructHARecoveryConfig(name, header.Manifest)
	if err != nil {
		return HAState{}, err
	}
	config.StartupTimeout = timeout
	validated, err := validateHASnapshotPackage(recoveryCtx, config, header.Path, false)
	if err != nil {
		return HAState{}, err
	}
	defer clear(validated.token)
	if subtle.ConstantTimeCompare([]byte(validated.ManifestSHA256), []byte(expectedManifestSHA256)) != 1 {
		return HAState{}, fmt.Errorf("HA snapshot manifest changed during recovery validation")
	}
	if err := checkHALegacyCollision(name); err != nil {
		return HAState{}, err
	}

	preflightCtx, cancelPreflight := context.WithTimeout(recoveryCtx, haRecoveryOperationTimeout)
	preflight, err := m.preflightHA(preflightCtx, config, true)
	cancelPreflight()
	if err != nil {
		return HAState{}, err
	}

	retryState, err := validateHARecoveryRetryState(config, validated, configExists)
	if err != nil {
		return HAState{}, err
	}
	retrying := retryState != nil
	volumeProvenance := mergeHARecoveryVolumeProvenance(config, preflight, nil)
	if retrying {
		if retryState.VolumeProvenance == nil {
			return HAState{}, fmt.Errorf("interrupted HA recovery lacks durable member-volume provenance; refusing to treat existing volumes as an intact pre-recovery cluster")
		}
		volumeProvenance = mergeHARecoveryVolumeProvenance(config, preflight, retryState.VolumeProvenance)
	}
	if len(preflight.containerRecord) > 0 && (!preflight.networkExists || len(preflight.volumeExists) != haMemberCount) {
		if retrying && !reflect.DeepEqual(retryState.VolumeProvenance, volumeProvenance) {
			updatedRetryState := *retryState
			updatedRetryState.VolumeProvenance = volumeProvenance
			updatedRetryState.UpdatedAt = time.Now().UTC()
			if err := saveHARecoveryState(updatedRetryState); err != nil {
				return HAState{}, fmt.Errorf("persist unsafe member-volume provenance before rejecting inconsistent HA recovery target: %w", err)
			}
		}
		return HAState{}, fmt.Errorf("refusing inconsistent HA recovery target: server envelopes exist without the complete exact network and member volumes")
	}
	tokenMissing, err := validateHARecoveryTargetToken(config.TokenFile, validated.token)
	if err != nil {
		return HAState{}, err
	}
	if !configExists && !retrying {
		if err := ensureEmptyHARecoveryFileState(config); err != nil {
			return HAState{}, err
		}
		if preflight.networkExists || len(preflight.volumeExists) != 0 || len(preflight.containerRecord) != 0 {
			return HAState{}, fmt.Errorf("refusing empty-host HA recovery because APC-owned runtime resources already exist without a matching recovery journal")
		}
	}
	stagedInput, err := m.prepareStagedHASnapshot(recoveryCtx, config, validated)
	if err != nil {
		return HAState{}, err
	}
	defer func() {
		err = errors.Join(err, m.cleanupStagedHASnapshot(context.WithoutCancel(recoveryCtx), config, stagedInput))
	}()

	// Desired Stopped and a nonterminal journal are durable before publishing a
	// usable config or issuing the first runtime mutation. The operation lock
	// prevents a concurrently running supervisor from observing the brief state
	// transition when recovering an existing token-less configuration.
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		return HAState{}, fmt.Errorf("persist stopped HA intent before empty-host recovery: %w", err)
	}
	now := time.Now().UTC()
	preparationState := HARecoveryState{
		APIVersion:       haSnapshotAPIVersion,
		Kind:             haRecoveryStateKind,
		Cluster:          config.Name,
		ClusterIdentity:  validated.Manifest.Cluster.Identity,
		SnapshotPath:     validated.Path,
		Phase:            "validated",
		StartedAt:        now,
		UpdatedAt:        now,
		NextAction:       "create or validate exact recovery network and member volumes",
		VolumeProvenance: volumeProvenance,
	}
	if err := saveHARecoveryState(preparationState); err != nil {
		return HAState{}, err
	}
	if tokenMissing {
		if err := installHARecoveryToken(config.TokenFile, validated.token); err != nil {
			return HAState{}, err
		}
	}
	if !configExists {
		persisted := config
		persisted.StartupTimeout = defaults.StartupTimeout
		if err := saveHAConfig(persisted); err != nil {
			return HAState{}, err
		}
	}
	if err := m.ensureHARecoveryInfrastructure(recoveryCtx, config, preflight); err != nil {
		return HAState{}, err
	}

	finalPreflightCtx, cancelFinalPreflight := context.WithTimeout(recoveryCtx, haRecoveryOperationTimeout)
	preflight, err = m.preflightHA(finalPreflightCtx, config, true)
	cancelFinalPreflight()
	if err != nil {
		return HAState{}, err
	}
	if !preflight.networkExists || len(preflight.volumeExists) != haMemberCount {
		return HAState{}, fmt.Errorf("HA recovery preparation did not produce the exact network and all three member volumes")
	}
	return m.restoreHAValidatedLocked(recoveryCtx, config, validated.Manifest, validated.Path, stagedInput, preflight, volumeProvenance)
}

func normalizeExpectedHAManifestSHA256(value string) (string, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "sha256:")
	value = strings.ToLower(value)
	if !validSHA256(value) {
		return "", fmt.Errorf("independently retained HA manifest SHA-256 must be exactly 64 hexadecimal characters")
	}
	return value, nil
}

func loadOrReconstructHARecoveryConfig(name string, manifest HASnapshotManifest) (HAConfig, bool, error) {
	config, err := loadHAConfig(name)
	if err == nil {
		return config, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return HAConfig{}, false, err
	}
	defaults, err := DefaultHAConfig(name)
	if err != nil {
		return HAConfig{}, false, err
	}
	config = defaults
	config.NetworkName = manifest.Topology.NetworkName
	config.Subnet = manifest.Topology.Subnet
	config.Image = manifest.Image.Reference
	config.ListenAddress = manifest.Topology.ListenAddress
	config.CPUs = manifest.Topology.CPUs
	config.Memory = manifest.Topology.Memory
	config.VolumeSize = manifest.Topology.VolumeSize
	config.DisableTraefik = manifest.Topology.DisableTraefik
	config.Members = append([]HAMember(nil), manifest.Topology.Members...)
	config, err = normalizeHAConfig(config)
	if err != nil {
		return HAConfig{}, false, fmt.Errorf("reconstruct HA configuration from snapshot: %w", err)
	}
	return config, false, nil
}

func validateHARecoveryRetryState(config HAConfig, snapshot haSnapshotPackage, configExists bool) (*HARecoveryState, error) {
	journal, err := LoadHARecoveryState(config.Name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("validate existing HA recovery journal: %w", err)
	}
	if haRecoveryJournalRuntimeSafe(journal) {
		if !configExists {
			return nil, fmt.Errorf("completed HA recovery journal cannot be reused without its saved configuration")
		}
		return nil, nil
	}
	if journal.ClusterIdentity != snapshot.Manifest.Cluster.Identity || filepath.Clean(journal.SnapshotPath) != filepath.Clean(snapshot.Path) {
		return nil, fmt.Errorf("existing HA recovery journal belongs to a different snapshot identity")
	}
	if !configExists {
		desiredPath, pathErr := haDesiredStatePath(config.Name)
		if pathErr != nil {
			return nil, pathErr
		}
		if _, statErr := os.Lstat(desiredPath); statErr != nil {
			return nil, fmt.Errorf("interrupted HA recovery is missing its durable stopped desired state: %w", statErr)
		}
		desired, desiredErr := loadHADesiredState(config.Name)
		if desiredErr != nil {
			return nil, desiredErr
		}
		if desired.ClusterState != haDesiredStopped {
			return nil, fmt.Errorf("interrupted HA recovery desired state is not Stopped")
		}
	}
	return &journal, nil
}

func haRecoveryJournalRuntimeSafe(journal HARecoveryState) bool {
	return journal.RecoverySucceeded &&
		(journal.Phase == "completed" || (journal.Phase == "failed" && journal.RecoveryAttempted))
}

func haRecoveryJournalRequiresRecover(journal HARecoveryState) bool {
	return !haRecoveryJournalRuntimeSafe(journal) &&
		journal.VolumeProvenance != nil && len(journal.VolumeProvenance.NewMemberIDs) != 0
}

// ensureHARecoveryJournalAllowsRestore prevents ordinary same-host restore
// from erasing RecoverHA's monotone empty-volume provenance. Only RecoverHA
// has the independently retained manifest digest required to resume that
// lineage safely.
func ensureHARecoveryJournalAllowsRestore(name string) error {
	journal, err := LoadHARecoveryState(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("refusing HA restore because the protected recovery journal cannot be trusted: %w", err)
	}
	if !haRecoveryJournalRequiresRecover(journal) {
		return nil
	}
	return fmt.Errorf(
		"refusing HA restore while the protected nonterminal recovery journal records newly created empty member volumes %v; retry `apc cluster ha recover %s` with the same snapshot package and its independently retained manifest SHA-256",
		journal.VolumeProvenance.NewMemberIDs,
		name,
	)
}

func mergeHARecoveryVolumeProvenance(config HAConfig, preflight haPreflight, prior *HARecoveryVolumeProvenance) *HARecoveryVolumeProvenance {
	newMembers := make(map[int]bool, haMemberCount)
	if prior != nil {
		for _, memberID := range prior.NewMemberIDs {
			newMembers[memberID] = true
		}
	}
	// Provenance is monotone across retries: a volume that was ever absent
	// during this recovery lineage can never be promoted back to preexisting,
	// even after ensureHARecoveryInfrastructure creates it successfully.
	for _, member := range config.Members {
		if !preflight.volumeExists[member.ID] {
			newMembers[member.ID] = true
		}
	}
	provenance := &HARecoveryVolumeProvenance{}
	for _, member := range config.Members {
		if !newMembers[member.ID] {
			provenance.PreexistingMemberIDs = append(provenance.PreexistingMemberIDs, member.ID)
		} else {
			provenance.NewMemberIDs = append(provenance.NewMemberIDs, member.ID)
		}
	}
	return provenance
}

func validateHARecoveryTargetToken(path string, backupToken []byte) (bool, error) {
	currentToken, err := readHARecoveryToken(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !sameK3sServerToken(currentToken, backupToken) {
		return false, fmt.Errorf("refusing to overwrite an existing HA server token that differs from the trusted snapshot package")
	}
	return false, nil
}

func ensureEmptyHARecoveryFileState(config HAConfig) error {
	desiredPath, err := haDesiredStatePath(config.Name)
	if err != nil {
		return err
	}
	for _, path := range []string{config.TokenFile, config.KubeconfigPath} {
		if _, statErr := os.Lstat(path); statErr == nil {
			return fmt.Errorf("refusing empty-host HA recovery because unexpected local state already exists at %q", path)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("inspect empty-host HA recovery state: %w", statErr)
		}
	}
	if _, statErr := os.Lstat(desiredPath); statErr == nil {
		desired, loadErr := loadHADesiredState(config.Name)
		if loadErr != nil {
			return loadErr
		}
		if desired.ClusterState != haDesiredStopped {
			return fmt.Errorf("refusing empty-host HA recovery because its orphaned desired state is not Stopped")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect empty-host HA recovery desired state: %w", statErr)
	}
	return nil
}

func installHARecoveryToken(path string, token []byte) (err error) {
	if _, statErr := os.Lstat(path); statErr == nil {
		return fmt.Errorf("refusing to overwrite existing HA recovery token file")
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect HA recovery token destination: %w", statErr)
	}
	if _, tokenErr := k3sServerTokenCredential(token); tokenErr != nil {
		return fmt.Errorf("validate HA recovery token before installation: %w", tokenErr)
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create HA recovery token directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure HA recovery token directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".server-token.recovery-*")
	if err != nil {
		return fmt.Errorf("create temporary HA recovery token: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { err = errors.Join(err, os.Remove(temporaryPath)) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure temporary HA recovery token: %w", err)
	}
	data := append(append([]byte(nil), token...), '\n')
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary HA recovery token: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("flush temporary HA recovery token: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary HA recovery token: %w", err)
	}
	// A hard link publishes the fully flushed inode atomically and, unlike
	// rename, fails rather than replacing a token created after our preflight.
	if err := os.Link(temporaryPath, path); err != nil {
		return fmt.Errorf("publish HA recovery token without replacement: %w", err)
	}
	if err := syncHADirectory(directory); err != nil {
		return fmt.Errorf("durably publish HA recovery token: %w", err)
	}
	return nil
}

func (m *Manager) ensureHARecoveryInfrastructure(ctx context.Context, config HAConfig, preflight haPreflight) error {
	if !preflight.networkExists {
		if err := m.createHAOwnedNetwork(ctx, config); err != nil {
			return err
		}
	}
	for _, member := range config.Members {
		if preflight.volumeExists[member.ID] {
			continue
		}
		if err := m.createHAOwnedVolume(ctx, config, member); err != nil {
			return err
		}
	}
	return nil
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
	if err := ensureHARecoveryJournalAllowsRestore(config.Name); err != nil {
		return HAState{}, err
	}
	validated, err := validateHASnapshotPackage(restoreCtx, config, input, true)
	if err != nil {
		return HAState{}, err
	}
	defer clear(validated.token)
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
	stagedInput, err := m.prepareStagedHASnapshot(restoreCtx, config, validated)
	if err != nil {
		return HAState{}, err
	}
	defer func() {
		err = errors.Join(err, m.cleanupStagedHASnapshot(context.WithoutCancel(restoreCtx), config, stagedInput))
	}()
	return m.restoreHAValidatedLocked(restoreCtx, config, validated.Manifest, validated.Path, stagedInput, preflight, nil)
}

// restoreHAValidatedLocked executes the destructive restore sequence after its
// caller has validated the package, current topology, and exact runtime
// resources while holding the per-cluster HA operation lock.
func (m *Manager) restoreHAValidatedLocked(restoreCtx context.Context, config HAConfig, manifest HASnapshotManifest, input, stagedInput string, preflight haPreflight, volumeProvenance *HARecoveryVolumeProvenance) (result HAState, err error) {
	now := time.Now().UTC()
	recoveryState := HARecoveryState{
		APIVersion:       haSnapshotAPIVersion,
		Kind:             haRecoveryStateKind,
		Cluster:          config.Name,
		ClusterIdentity:  manifest.Cluster.Identity,
		SnapshotPath:     input,
		Phase:            "validated",
		StartedAt:        now,
		UpdatedAt:        now,
		NextAction:       "stop all three HA members",
		VolumeProvenance: volumeProvenance,
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
	progress := haRestoreProgress{newVolumeMembers: map[int]bool{}, peerDataCleared: map[int]bool{}}
	if volumeProvenance != nil {
		for _, memberID := range volumeProvenance.NewMemberIDs {
			progress.newVolumeMembers[memberID] = true
		}
	}
	defer func() {
		if err == nil || !progress.mutationStarted {
			return
		}
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(restoreCtx), haRecoveryAttemptTimeout)
		defer cancel()
		recoveryErr := m.recoverFailedHARestore(recoveryCtx, config, stagedInput, &progress)
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
	resetSpec := restoreResetHelperSpec(config, seed, stagedInput)
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
	if err := m.refreshHARestoreKubeconfig(restoreCtx, config, seed); err != nil {
		return HAState{}, err
	}
	state, err := m.waitHAClusterReady(restoreCtx, config)
	if err != nil {
		return HAState{}, err
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

func (m *Manager) refreshHARestoreKubeconfig(ctx context.Context, config HAConfig, seed HAMember) error {
	kubeconfig, err := m.readKubeconfig(ctx, HAContainerName(config.Name, seed.ID), seed.apiEndpoint(config.ListenAddress))
	if err != nil {
		return err
	}
	if err := writePrivateFileAtomic(config.KubeconfigPath, kubeconfig); err != nil {
		return fmt.Errorf("refresh restored HA kubeconfig: %w", err)
	}
	return nil
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

// runHARestoreResetCommandWithinContext lets a restore reset use the caller's
// remaining total restore deadline instead of imposing a second fixed cap.
// RestoreHA always supplies a deadline-bearing context here.
func (m *Manager) runHARestoreResetCommandWithinContext(ctx context.Context, operation string, arguments ...string) ([]byte, []byte, error) {
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
	// The reset helper executes K3s and apple/container may reflect its argv,
	// bind paths, runtime addresses, or credentials on stderr. Keep the raw
	// bytes only for the guarded collision parser in runHARestoreReset; never
	// place them, or the runner error which may include argv, in a returned
	// error or the durable recovery journal.
	diagnostic := sanitizeHARestoreResetStderr(stderr)
	if diagnostic == "" {
		return stdout, stderr, fmt.Errorf("%s failed", operation)
	}
	return stdout, stderr, fmt.Errorf("%s failed; reset helper stderr diagnostic: %s", operation, diagnostic)
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
	for attempt := 1; attempt <= haRuntimeAddressRetryLimit; attempt++ {
		stdout, stderr, runErr := m.runHARestoreResetCommandWithinContext(ctx, "reset K3s embedded-etcd member 1 from snapshot", arguments...)
		cleanupErr := m.deleteHARecoveryHelper(ctx, spec)
		collision := haRuntimeIPCollisionFromOutput(stderr, config, member)
		if collision == nil {
			if runErr != nil {
				return errors.Join(withHARestoreResetStdoutDiagnostic(runErr, stdout), cleanupErr)
			}
			return cleanupErr
		}
		if cleanupErr != nil {
			return errors.Join(errors.New("reset K3s embedded-etcd member 1 encountered a reserved runtime IPv4 collision"), cleanupErr)
		}
	}
	return fmt.Errorf(
		"reset K3s embedded-etcd member 1 could not obtain a non-reserved runtime IPv4 after %d attempts; apple/container 1.0 does not support fixed IPv4 on container run; retry after checking the dedicated network for foreign attachments",
		haRuntimeAddressRetryLimit,
	)
}

func withHARestoreResetStdoutDiagnostic(runErr error, stdout []byte) error {
	if runErr == nil {
		return nil
	}
	diagnostic := sanitizeHARestoreResetStdout(stdout)
	if diagnostic == "" {
		return runErr
	}
	return fmt.Errorf("%w; reset helper stdout diagnostic: %s", runErr, diagnostic)
}

// sanitizeHARestoreResetOutput deliberately emits only fixed diagnostic keys,
// never substrings from helper output. K3s/container failures can include
// tokens, host bind paths, runtime IPs, argv, or other credentials on either
// stream. A bounded head/tail window and fixed result vocabulary make the
// diagnostic useful without reflecting attacker-controlled or secret values.
func sanitizeHARestoreResetStdout(stdout []byte) string {
	return sanitizeHARestoreResetOutput(stdout)
}

func sanitizeHARestoreResetStderr(stderr []byte) string {
	return sanitizeHARestoreResetOutput(stderr)
}

func sanitizeHARestoreResetOutput(output []byte) string {
	if len(output) == 0 {
		return ""
	}
	window := output
	if len(window) > haResetDiagnosticInputMax {
		half := haResetDiagnosticInputMax / 2
		bounded := make([]byte, 0, haResetDiagnosticInputMax+1)
		bounded = append(bounded, window[:half]...)
		bounded = append(bounded, '\n')
		bounded = append(bounded, window[len(window)-half:]...)
		window = bounded
	}
	lower := strings.ToLower(string(window))
	if strings.TrimSpace(lower) == "" {
		return ""
	}
	rules := []struct {
		key     string
		phrases []string
	}{
		{key: "permission denied", phrases: []string{"permission denied", "access denied"}},
		{key: "operation not permitted", phrases: []string{"operation not permitted"}},
		{key: "no such file", phrases: []string{"no such file", "file not found", "not found"}},
		{key: "snapshot", phrases: []string{"snapshot"}},
		{key: "restore failed", phrases: []string{"restore failed", "failed to restore", "restore failure"}},
		{key: "checksum mismatch", phrases: []string{"checksum mismatch", "checksum failed", "digest mismatch"}},
		{key: "corrupt data", phrases: []string{"corrupt", "malformed database"}},
		{key: "read-only filesystem", phrases: []string{"read-only file system", "read only file system"}},
		{key: "no space left", phrases: []string{"no space left", "disk full"}},
		{key: "I/O error", phrases: []string{"input/output error", "i/o error"}},
		{key: "timeout", phrases: []string{"timed out", "timeout", "deadline exceeded"}},
		{key: "etcd/datastore", phrases: []string{"etcd", "datastore"}},
	}
	keys := make([]string, 0, haResetDiagnosticKeysMax)
	for _, rule := range rules {
		for _, phrase := range rule.phrases {
			if strings.Contains(lower, phrase) {
				keys = append(keys, rule.key)
				break
			}
		}
		if len(keys) == haResetDiagnosticKeysMax {
			break
		}
	}
	if len(keys) == 0 {
		return "unclassified failure output"
	}
	diagnostic := strings.Join(keys, ", ")
	if len(diagnostic) > haResetDiagnosticOutputMax {
		return diagnostic[:haResetDiagnosticOutputMax]
	}
	return diagnostic
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
	if len(arguments) < 4 || arguments[0] != "-c" || strings.TrimSpace(arguments[1]) == "" || strings.TrimSpace(arguments[2]) == "" {
		return []string{"-c", `echo "APC restore token installer initialization failed" >&2; exit 1`, "apc-k3s"}
	}
	result := make([]string, 0, len(arguments)+6)
	result = append(result,
		arguments[0],
		haRestoreTokenInstallScript+"; "+arguments[1],
		arguments[2],
		pathpkg.Join(haRecoveryBackupMount, haSnapshotTokenFilename),
		pathpkg.Join("/var/lib/rancher/k3s", "server"),
	)
	for index := 3; index < len(arguments); index++ {
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

func (m *Manager) cleanupStaleHARestoreHelpers(ctx context.Context, config HAConfig, input, stagedInput string) error {
	resetName := HARestoreResetContainerName(config.Name)
	record, inspectErr := m.inspectHARecoveryContainer(ctx, resetName)
	if inspectErr == nil {
		var matched *haRecoveryHelperSpec
		for _, candidatePath := range []string{stagedInput, input} {
			if candidatePath == "" || (matched != nil && candidatePath == matched.BackupDirectory) {
				continue
			}
			candidate := restoreResetHelperSpec(config, config.Members[0], candidatePath)
			if validateHARecoveryHelper(record, candidate) == nil {
				matched = &candidate
				break
			}
		}
		if matched == nil {
			return fmt.Errorf("reconcile stale HA restore helper %q before retry: helper does not use the current protected staging directory", resetName)
		}
		if err := m.deleteHARecoveryHelper(ctx, *matched); err != nil {
			return fmt.Errorf("reconcile stale HA restore helper %q before retry: %w", resetName, err)
		}
	} else if !errors.Is(inspectErr, ErrNotFound) {
		return inspectErr
	}

	specs := make([]haRecoveryHelperSpec, 0, len(config.Members)-1)
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

func haRestoreStagingPath(config HAConfig) (string, error) {
	configPath, err := HAConfigPath(config.Name)
	if err != nil {
		return "", err
	}
	path := filepath.Join(filepath.Dir(configPath), haRestoreStageDirectory)
	if !filepath.IsAbs(path) || filepath.Base(path) != haRestoreStageDirectory {
		return "", fmt.Errorf("derive protected HA restore staging path")
	}
	return path, nil
}

func (m *Manager) prepareStagedHASnapshot(ctx context.Context, config HAConfig, snapshot haSnapshotPackage) (string, error) {
	stagedPath, err := haRestoreStagingPath(config)
	if err != nil {
		return "", err
	}
	if err := m.cleanupStaleHARestoreHelpers(ctx, config, snapshot.Path, stagedPath); err != nil {
		return "", err
	}
	if err := m.ensureNoHARecoveryHelpers(ctx, config); err != nil {
		return "", err
	}
	if err := removeHAStagingDirectory(stagedPath); err != nil {
		return "", err
	}
	if err := stageHASnapshotPackage(ctx, snapshot, stagedPath); err != nil {
		return "", err
	}
	return stagedPath, nil
}

func (m *Manager) cleanupStagedHASnapshot(ctx context.Context, config HAConfig, stagedPath string) error {
	if stagedPath == "" {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, haRecoveryOperationTimeout)
	defer cancel()
	if _, err := m.inspectHARecoveryContainer(cleanupCtx, HARestoreResetContainerName(config.Name)); err == nil {
		return fmt.Errorf("retain protected HA restore staging package because its reset helper still exists")
	} else if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("verify HA restore staging cleanup safety: %w", err)
	}
	return removeHAStagingDirectory(stagedPath)
}

func removeHAStagingDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect protected HA restore staging directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || !haPathOwnedByEffectiveUser(info) {
		return fmt.Errorf("HA restore staging path must be a private real directory owned by the current user")
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove protected HA restore staging directory: %w", err)
	}
	if err := syncHADirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("durably remove protected HA restore staging directory: %w", err)
	}
	return nil
}

func stageHASnapshotPackage(ctx context.Context, snapshot haSnapshotPackage, destination string) (err error) {
	if snapshot.manifestInfo == nil || snapshot.snapshotInfo == nil || snapshot.tokenInfo == nil {
		return fmt.Errorf("validated HA snapshot is missing source file identities")
	}
	if _, statErr := os.Lstat(destination); !errors.Is(statErr, os.ErrNotExist) {
		if statErr == nil {
			return fmt.Errorf("protected HA restore staging directory already exists")
		}
		return fmt.Errorf("inspect HA restore staging destination: %w", statErr)
	}
	parent := filepath.Dir(destination)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("inspect HA restore staging parent: %w", err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() || parentInfo.Mode().Perm()&0o077 != 0 || !haPathOwnedByEffectiveUser(parentInfo) {
		return fmt.Errorf("HA restore staging parent must be a private real directory owned by the current user")
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return fmt.Errorf("create protected HA restore staging directory: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, removeHAStagingDirectory(destination))
		}
	}()
	if err := os.Chmod(destination, 0o700); err != nil {
		return fmt.Errorf("protect HA restore staging directory: %w", err)
	}
	directoryDescriptor, err := syscall.Open(destination, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open protected HA restore staging directory without following links: %w", err)
	}
	directory := os.NewFile(uintptr(directoryDescriptor), destination)
	if directory == nil {
		_ = syscall.Close(directoryDescriptor)
		return fmt.Errorf("open protected HA restore staging directory")
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	directoryInfo, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened HA restore staging directory: %w", err)
	}
	pathInfo, err := os.Lstat(destination)
	if err != nil {
		return fmt.Errorf("inspect HA restore staging directory path: %w", err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 || !os.SameFile(directoryInfo, pathInfo) || !haPathOwnedByEffectiveUser(directoryInfo) {
		return fmt.Errorf("HA restore staging directory changed while it was being opened")
	}
	destinationRoot, err := os.OpenRoot(destination)
	if err != nil {
		return fmt.Errorf("open protected HA restore staging root: %w", err)
	}
	defer func() { err = errors.Join(err, destinationRoot.Close()) }()
	rootInfo, err := destinationRoot.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect protected HA restore staging root: %w", err)
	}
	if !os.SameFile(directoryInfo, rootInfo) {
		return fmt.Errorf("HA restore staging directory changed while its root was being opened")
	}

	manifestMetadata := HASnapshotFileMetadata{
		Name: haSnapshotManifestFilename, Size: snapshot.manifestInfo.Size(), SHA256: snapshot.ManifestSHA256,
	}
	artifacts := []struct {
		metadata HASnapshotFileMetadata
		identity os.FileInfo
		mode     os.FileMode
	}{
		{metadata: manifestMetadata, identity: snapshot.manifestInfo, mode: 0o400},
		{metadata: snapshot.Manifest.Snapshot, identity: snapshot.snapshotInfo, mode: 0o600},
		{metadata: snapshot.Manifest.ServerToken, identity: snapshot.tokenInfo, mode: 0o600},
	}
	for _, artifact := range artifacts {
		if err := copyHAStagedArtifact(ctx, filepath.Join(snapshot.Path, artifact.metadata.Name), destinationRoot, artifact.metadata, artifact.identity, artifact.mode); err != nil {
			return err
		}
	}
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("flush protected HA restore staging package: %w", err)
	}
	if err := reinspectHAStagingDirectory(destination, directoryInfo); err != nil {
		return err
	}
	if err := syncHADirectory(parent); err != nil {
		return fmt.Errorf("durably publish protected HA restore staging package: %w", err)
	}
	return nil
}

func reinspectHAStagingDirectory(path string, opened os.FileInfo) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("reinspect protected HA restore staging directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 || !os.SameFile(opened, info) || !haPathOwnedByEffectiveUser(info) {
		return fmt.Errorf("HA restore staging directory changed while the package was being copied")
	}
	return nil
}

func copyHAStagedArtifact(ctx context.Context, sourcePath string, destinationRoot *os.Root, expected HASnapshotFileMetadata, identity os.FileInfo, mode os.FileMode) (err error) {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stage HA snapshot artifact %q: %w", expected.Name, err)
	}
	if expected.Name == "" || filepath.Base(expected.Name) != expected.Name || expected.Size < 0 || !validSHA256(strings.ToLower(expected.SHA256)) || identity == nil || destinationRoot == nil {
		return fmt.Errorf("validated HA snapshot artifact metadata is incomplete")
	}
	source, sourceInfo, err := openHAArtifactNoFollow(sourcePath, identity)
	if err != nil {
		return fmt.Errorf("stage HA snapshot artifact %q: %w", expected.Name, err)
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if sourceInfo.Size() != expected.Size {
		return fmt.Errorf("HA snapshot artifact %q changed size after validation", expected.Name)
	}
	destination, err := destinationRoot.OpenFile(expected.Name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
	if err != nil {
		return fmt.Errorf("create staged HA snapshot artifact %q: %w", expected.Name, err)
	}
	destinationOpen := true
	defer func() {
		if destinationOpen {
			err = errors.Join(err, destination.Close())
		}
	}()
	if err := destination.Chmod(mode); err != nil {
		return fmt.Errorf("protect staged HA snapshot artifact %q: %w", expected.Name, err)
	}

	hash := sha256.New()
	buffer := make([]byte, 1<<20)
	var copied int64
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("stage HA snapshot artifact %q: %w", expected.Name, err)
		}
		count, readErr := source.Read(buffer)
		if count > 0 {
			if int64(count) > expected.Size-copied {
				return fmt.Errorf("HA snapshot artifact %q changed size after validation", expected.Name)
			}
			written, writeErr := destination.Write(buffer[:count])
			if writeErr != nil {
				return fmt.Errorf("write staged HA snapshot artifact %q: %w", expected.Name, writeErr)
			}
			if written != count {
				return fmt.Errorf("write staged HA snapshot artifact %q: %w", expected.Name, io.ErrShortWrite)
			}
			_, _ = hash.Write(buffer[:count])
			copied += int64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read HA snapshot artifact %q for staging: %w", expected.Name, readErr)
		}
	}
	actualDigest := hex.EncodeToString(hash.Sum(nil))
	if copied != expected.Size || subtle.ConstantTimeCompare([]byte(actualDigest), []byte(strings.ToLower(expected.SHA256))) != 1 {
		return fmt.Errorf("HA snapshot artifact %q changed after validation", expected.Name)
	}
	if err := reinspectHAArtifact(sourcePath, sourceInfo); err != nil {
		return fmt.Errorf("stage HA snapshot artifact %q: %w", expected.Name, err)
	}
	if err := destination.Sync(); err != nil {
		return fmt.Errorf("flush staged HA snapshot artifact %q: %w", expected.Name, err)
	}
	if err := destination.Close(); err != nil {
		destinationOpen = false
		return fmt.Errorf("close staged HA snapshot artifact %q: %w", expected.Name, err)
	}
	destinationOpen = false
	info, err := destinationRoot.Lstat(expected.Name)
	if err != nil {
		return fmt.Errorf("inspect staged HA snapshot artifact %q: %w", expected.Name, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != mode.Perm() || info.Size() != expected.Size {
		return fmt.Errorf("staged HA snapshot artifact %q failed final mode or size validation", expected.Name)
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
		newMemberIDs := make([]int, 0, len(progress.newVolumeMembers))
		for _, member := range config.Members {
			if progress.newVolumeMembers[member.ID] {
				newMemberIDs = append(newMemberIDs, member.ID)
			}
		}
		if len(newMemberIDs) != 0 {
			// RecoverHA created these volumes without any etcd state. Starting
			// ordinary server envelopes here would manufacture a different empty
			// cluster and falsely turn an unattempted snapshot reset into a
			// successful rollback. Keep both desired and actual lifecycle stopped;
			// the failed journal remains the supervisor barrier until retry.
			if err := markHAClusterStoppedLocked(config.Name); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("persist stopped HA intent after pre-reset empty-volume failure: %w", err))
			}
			if err := m.stopExactHARecoveryServerEnvelopes(ctx, config); err != nil {
				cleanupErrors = append(cleanupErrors, err)
			}
			cleanupErrors = append(cleanupErrors, fmt.Errorf("refusing to start newly created empty HA member volumes %v before the trusted snapshot reset has been attempted", newMemberIDs))
			return errors.Join(cleanupErrors...)
		}
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
		seed := memberByID(config.Members, 1)
		if err := m.waitHAMemberReady(ctx, config, seed, config.StartupTimeout); err != nil {
			cleanupErrors = append(cleanupErrors, err)
			return errors.Join(cleanupErrors...)
		}
		if err := m.refreshHARestoreKubeconfig(ctx, config, seed); err != nil {
			cleanupErrors = append(cleanupErrors, err)
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
		if err := m.refreshHARestoreKubeconfig(ctx, config, seed); err != nil {
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

// stopExactHARecoveryServerEnvelopes converges every existing, exactly owned
// server envelope to Stopped after a pre-reset recovery failure. It keeps
// going after individual failures and re-inspects every stop target so an
// ambiguous runtime error cannot be mistaken for a safe final state.
func (m *Manager) stopExactHARecoveryServerEnvelopes(ctx context.Context, config HAConfig) error {
	var stopErrors []error
	for _, member := range config.Members {
		name := HAContainerName(config.Name, member.ID)
		record, err := m.inspectHARecoveryContainer(ctx, name)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("inspect HA recovery member %d before protective stop: %w", member.ID, err))
			continue
		}
		if err := validateHAContainer(record, config, member); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("refuse protective stop for untrusted HA recovery member %d: %w", member.ID, err))
			continue
		}

		var stopErr error
		if !strings.EqualFold(record.Status.State, "stopped") {
			stopErr = m.runHABounded(ctx, fmt.Sprintf("protectively stop HA recovery member %d", member.ID), "stop", name)
		}

		finalRecord, finalErr := m.inspectHARecoveryContainer(ctx, name)
		if errors.Is(finalErr, ErrNotFound) {
			continue
		}
		if finalErr != nil {
			stopErrors = append(stopErrors, errors.Join(stopErr, fmt.Errorf("verify protective stop for HA recovery member %d: %w", member.ID, finalErr)))
			continue
		}
		if err := validateHAContainer(finalRecord, config, member); err != nil {
			stopErrors = append(stopErrors, errors.Join(stopErr, fmt.Errorf("verify exact identity after protective stop for HA recovery member %d: %w", member.ID, err)))
			continue
		}
		if !strings.EqualFold(finalRecord.Status.State, "stopped") {
			stopErrors = append(stopErrors, errors.Join(stopErr, fmt.Errorf("HA recovery member %d remains in runtime state %q after protective stop", member.ID, finalRecord.Status.State)))
		}
	}
	return errors.Join(stopErrors...)
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
	validated, err := validateHASnapshotPackage(ctx, config, input, true)
	if err != nil {
		return HASnapshotManifest{}, "", err
	}
	return validated.Manifest, validated.Path, nil
}

// readHASnapshotManifest validates the immutable package envelope and returns a
// fingerprint of the exact manifest bytes. It deliberately does not trust any
// topology or token value until validateHASnapshotPackage binds them together.
func readHASnapshotManifest(input string) (haSnapshotPackage, error) {
	if strings.TrimSpace(input) == "" {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot input directory is required")
	}
	abs, err := filepath.Abs(input)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("resolve HA snapshot path: %w", err)
	}
	abs = filepath.Clean(abs)
	if strings.ContainsAny(abs, ",\x00\r\n") {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot path is not mount-safe")
	}
	lstat, err := os.Lstat(abs)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("inspect HA snapshot directory: %w", err)
	}
	if lstat.Mode()&os.ModeSymlink != 0 || !lstat.IsDir() {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot path must be a real directory, not a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("resolve HA snapshot directory: %w", err)
	}
	abs = resolved
	info, err := os.Stat(abs)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("read HA snapshot directory: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot directory permissions must be 0700 or stricter")
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("list HA snapshot directory: %w", err)
	}
	wantEntries := []string{haSnapshotDataFilename, haSnapshotManifestFilename, haSnapshotTokenFilename}
	gotEntries := make([]string, 0, len(entries))
	for _, entry := range entries {
		gotEntries = append(gotEntries, entry.Name())
	}
	sort.Strings(gotEntries)
	sort.Strings(wantEntries)
	if !reflect.DeepEqual(gotEntries, wantEntries) {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot directory must contain exactly %s", strings.Join(wantEntries, ", "))
	}

	manifestPath := filepath.Join(abs, haSnapshotManifestFilename)
	manifestData, manifestInfo, err := readHAArtifactBytes(manifestPath, haSnapshotManifestFilename, 1<<20, nil)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("validate HA snapshot manifest: %w", err)
	}
	if manifestInfo.Mode().Perm()&0o222 != 0 {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot manifest must be immutable (mode 0400 or stricter)")
	}
	decoder := json.NewDecoder(bytes.NewReader(manifestData))
	decoder.DisallowUnknownFields()
	var manifest HASnapshotManifest
	if err := decoder.Decode(&manifest); err != nil {
		return haSnapshotPackage{}, fmt.Errorf("decode HA snapshot manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return haSnapshotPackage{}, fmt.Errorf("decode HA snapshot manifest: %w", err)
	}
	if manifest.APIVersion != haSnapshotAPIVersion || manifest.Kind != haSnapshotKind || manifest.FormatVersion != haSnapshotFormatVersion {
		return haSnapshotPackage{}, fmt.Errorf("unsupported HA snapshot format")
	}
	if manifest.CreatedAt.IsZero() || manifest.CreatedAt.After(time.Now().UTC().Add(5*time.Minute)) {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot manifest has an invalid creation time")
	}
	manifestDigest := sha256.Sum256(manifestData)
	return haSnapshotPackage{
		Manifest:       manifest,
		Path:           abs,
		ManifestSHA256: hex.EncodeToString(manifestDigest[:]),
		manifestInfo:   manifestInfo,
	}, nil
}

func validateHASnapshotPackage(ctx context.Context, config HAConfig, input string, requireCurrentToken bool) (haSnapshotPackage, error) {
	if err := ctx.Err(); err != nil {
		return haSnapshotPackage{}, fmt.Errorf("validate HA snapshot: %w", err)
	}
	validated, err := readHASnapshotManifest(input)
	if err != nil {
		return haSnapshotPackage{}, err
	}
	manifest := validated.Manifest
	if manifest.Cluster.Name != config.Name {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot belongs to cluster %q, not %q", manifest.Cluster.Name, config.Name)
	}
	if !validSHA256(manifest.Cluster.Identity) {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot contains an invalid cluster identity")
	}
	if !reflect.DeepEqual(manifest.Topology, haSnapshotTopology(config)) {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot topology does not match saved cluster %q", config.Name)
	}
	if manifest.Image.Reference != config.Image || manifest.Image.Architecture != "arm64" {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot image identity does not match saved cluster %q", config.Name)
	}
	source := memberByID(config.Members, manifest.K3s.SourceMemberID)
	if source.NodeName == "" || source.NodeName != manifest.K3s.SourceNodeName || strings.TrimSpace(manifest.K3s.Version) == "" || !validHAArtifactName(manifest.K3s.SnapshotName) || manifest.K3s.CreatedAt.IsZero() || manifest.K3s.CreatedAt.After(time.Now().UTC().Add(5*time.Minute)) {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot contains invalid K3s snapshot metadata")
	}
	if manifest.Snapshot.Name != haSnapshotDataFilename || manifest.ServerToken.Name != haSnapshotTokenFilename {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot manifest contains an unsafe artifact path")
	}
	if manifest.Snapshot.Size <= 0 || manifest.ServerToken.Size <= 0 || manifest.ServerToken.Size > haRecoveryTokenMaximum || !validSHA256(manifest.Snapshot.SHA256) || !validSHA256(manifest.ServerToken.SHA256) {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot manifest contains invalid artifact metadata")
	}
	snapshotMaximum, err := haSnapshotArtifactMaximum(config)
	if err != nil {
		return haSnapshotPackage{}, err
	}
	if manifest.Snapshot.Size > snapshotMaximum {
		return haSnapshotPackage{}, fmt.Errorf("HA etcd snapshot size %d exceeds declared member volume bound %d", manifest.Snapshot.Size, snapshotMaximum)
	}
	actualSnapshot, snapshotInfo, err := hashHAArtifactWithIdentity(ctx, filepath.Join(validated.Path, haSnapshotDataFilename), haSnapshotDataFilename, snapshotMaximum, nil)
	if err != nil {
		return haSnapshotPackage{}, err
	}
	actualToken, tokenInfo, err := hashHAArtifactWithIdentity(ctx, filepath.Join(validated.Path, haSnapshotTokenFilename), haSnapshotTokenFilename, haRecoveryTokenMaximum, nil)
	if err != nil {
		return haSnapshotPackage{}, err
	}
	if !sameHAFileMetadata(actualSnapshot, manifest.Snapshot) {
		return haSnapshotPackage{}, fmt.Errorf("HA etcd snapshot checksum or size mismatch")
	}
	if !sameHAFileMetadata(actualToken, manifest.ServerToken) {
		return haSnapshotPackage{}, fmt.Errorf("HA server token checksum or size mismatch")
	}
	backupToken, err := readStrictTokenFileWithIdentity(filepath.Join(validated.Path, haSnapshotTokenFilename), tokenInfo)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("validate backed-up HA server token: %w", err)
	}
	if requireCurrentToken {
		currentToken, err := readHARecoveryToken(config.TokenFile)
		if err != nil {
			return haSnapshotPackage{}, err
		}
		if !sameK3sServerToken(backupToken, currentToken) {
			return haSnapshotPackage{}, fmt.Errorf("HA snapshot server token does not match cluster %q", config.Name)
		}
	}
	tokenCredential, err := k3sServerTokenCredential(backupToken)
	if err != nil {
		return haSnapshotPackage{}, fmt.Errorf("validate K3s server token binding: %w", err)
	}
	tokenDigest := sha256.Sum256(tokenCredential)
	expectedIdentity, err := haClusterIdentity(config, hex.EncodeToString(tokenDigest[:]))
	if err != nil {
		return haSnapshotPackage{}, err
	}
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(manifest.Cluster.Identity)), []byte(expectedIdentity)) != 1 {
		return haSnapshotPackage{}, fmt.Errorf("HA snapshot cluster identity does not match the explicit saved identity of cluster %q", config.Name)
	}
	validated.token = append([]byte(nil), backupToken...)
	validated.snapshotInfo = snapshotInfo
	validated.tokenInfo = tokenInfo
	return validated, nil
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

func openHAArtifactNoFollow(path string, expected os.FileInfo) (*os.File, os.FileInfo, error) {
	before, err := secureHAArtifactInfo(path)
	if err != nil {
		return nil, nil, err
	}
	if expected != nil && (!os.SameFile(expected, before) || expected.Size() != before.Size() || expected.Mode() != before.Mode()) {
		return nil, nil, fmt.Errorf("artifact %q was replaced after validation", filepath.Base(path))
	}
	fileDescriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open artifact %q without following links: %w", filepath.Base(path), err)
	}
	file := os.NewFile(uintptr(fileDescriptor), path)
	if file == nil {
		_ = syscall.Close(fileDescriptor)
		return nil, nil, fmt.Errorf("open artifact %q", filepath.Base(path))
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("inspect opened artifact %q: %w", filepath.Base(path), err)
	}
	if !opened.Mode().IsRegular() || opened.Mode().Perm()&0o077 != 0 || !os.SameFile(before, opened) || (expected != nil && (!os.SameFile(expected, opened) || expected.Size() != opened.Size() || expected.Mode() != opened.Mode())) {
		_ = file.Close()
		return nil, nil, fmt.Errorf("artifact %q changed while it was being opened", filepath.Base(path))
	}
	return file, opened, nil
}

func reinspectHAArtifact(path string, opened os.FileInfo) error {
	finalInfo, err := secureHAArtifactInfo(path)
	if err != nil {
		return err
	}
	if !os.SameFile(opened, finalInfo) || opened.Size() != finalInfo.Size() || opened.Mode() != finalInfo.Mode() {
		return fmt.Errorf("artifact %q changed while it was being read", filepath.Base(path))
	}
	return nil
}

func readHAArtifactBytes(path, name string, maximumBytes int64, expected os.FileInfo) (data []byte, identity os.FileInfo, err error) {
	if maximumBytes <= 0 {
		return nil, nil, fmt.Errorf("HA snapshot artifact %q has an invalid size bound", name)
	}
	file, opened, err := openHAArtifactNoFollow(path, expected)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if opened.Size() > maximumBytes {
		return nil, nil, fmt.Errorf("HA snapshot artifact %q exceeds maximum size %d", name, maximumBytes)
	}
	data, err = io.ReadAll(io.LimitReader(file, maximumBytes+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read HA snapshot artifact %q: %w", name, err)
	}
	if int64(len(data)) > maximumBytes || int64(len(data)) != opened.Size() {
		return nil, nil, fmt.Errorf("HA snapshot artifact %q changed size while it was being read", name)
	}
	if err := reinspectHAArtifact(path, opened); err != nil {
		return nil, nil, err
	}
	return data, opened, nil
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
	metadata, _, err := hashHAArtifactWithIdentity(ctx, path, name, maximumBytes, nil)
	return metadata, err
}

func hashHAArtifactWithIdentity(ctx context.Context, path, name string, maximumBytes int64, expected os.FileInfo) (HASnapshotFileMetadata, os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return HASnapshotFileMetadata{}, nil, fmt.Errorf("checksum HA snapshot artifact %q: %w", name, err)
	}
	if maximumBytes <= 0 {
		return HASnapshotFileMetadata{}, nil, fmt.Errorf("HA snapshot artifact %q has an invalid size bound", name)
	}
	file, openedInfo, err := openHAArtifactNoFollow(path, expected)
	if err != nil {
		return HASnapshotFileMetadata{}, nil, fmt.Errorf("validate HA snapshot artifact: %w", err)
	}
	defer file.Close()
	if openedInfo.Size() > maximumBytes {
		return HASnapshotFileMetadata{}, nil, fmt.Errorf("HA snapshot artifact %q exceeds maximum size %d", name, maximumBytes)
	}
	hash := sha256.New()
	buffer := make([]byte, 1<<20)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return HASnapshotFileMetadata{}, nil, fmt.Errorf("checksum HA snapshot artifact %q: %w", name, err)
		}
		count, readErr := file.Read(buffer)
		if count > 0 {
			if int64(count) > maximumBytes-size {
				return HASnapshotFileMetadata{}, nil, fmt.Errorf("HA snapshot artifact %q exceeds maximum size %d", name, maximumBytes)
			}
			_, _ = hash.Write(buffer[:count])
			size += int64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return HASnapshotFileMetadata{}, nil, fmt.Errorf("checksum HA snapshot artifact %q: %w", name, readErr)
		}
	}
	if size != openedInfo.Size() {
		return HASnapshotFileMetadata{}, nil, fmt.Errorf("HA snapshot artifact %q changed size during validation", name)
	}
	if err := reinspectHAArtifact(path, openedInfo); err != nil {
		return HASnapshotFileMetadata{}, nil, err
	}
	return HASnapshotFileMetadata{Name: name, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, openedInfo, nil
}

func sameHAFileMetadata(actual, expected HASnapshotFileMetadata) bool {
	return actual.Name == expected.Name && actual.Size == expected.Size && subtle.ConstantTimeCompare([]byte(actual.SHA256), []byte(strings.ToLower(expected.SHA256))) == 1
}

func readStrictTokenFile(path string, requireProtected bool) ([]byte, error) {
	if requireProtected {
		data, _, err := readHAArtifactBytes(path, filepath.Base(path), haRecoveryTokenMaximum, nil)
		if err != nil {
			return nil, err
		}
		return parseStrictToken(data)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read token file: %w", err)
	}
	return parseStrictToken(data)
}

func readStrictTokenFileWithIdentity(path string, expected os.FileInfo) ([]byte, error) {
	data, _, err := readHAArtifactBytes(path, haSnapshotTokenFilename, haRecoveryTokenMaximum, expected)
	if err != nil {
		return nil, err
	}
	return parseStrictToken(data)
}

func parseStrictToken(data []byte) ([]byte, error) {
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
	if err := validateHARecoveryVolumeProvenance(state.VolumeProvenance); err != nil {
		return err
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

func validateHARecoveryVolumeProvenance(provenance *HARecoveryVolumeProvenance) error {
	if provenance == nil {
		return nil
	}
	seen := make(map[int]bool, haMemberCount)
	for _, memberIDs := range [][]int{provenance.PreexistingMemberIDs, provenance.NewMemberIDs} {
		previous := 0
		for _, memberID := range memberIDs {
			if memberID < 1 || memberID > haMemberCount || memberID <= previous || seen[memberID] {
				return fmt.Errorf("HA recovery state has invalid member-volume provenance")
			}
			seen[memberID] = true
			previous = memberID
		}
	}
	if len(seen) != haMemberCount {
		return fmt.Errorf("HA recovery state has incomplete member-volume provenance")
	}
	return nil
}

func updateHARecoveryPhase(state *HARecoveryState, phase, nextAction string) error {
	state.Phase = phase
	state.UpdatedAt = time.Now().UTC()
	state.NextAction = nextAction
	return saveHARecoveryState(*state)
}
