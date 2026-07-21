package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

const (
	haDesiredStateFilename   = "ha-desired-state.json"
	haDesiredStateAPIVersion = "apc.dev/v1alpha1"
	haDesiredStateKind       = "HADesiredState"
	haDesiredRunning         = "Running"
	haDesiredStopped         = "Stopped"
	haDesiredStateMaximum    = int64(16 << 10)
)

// HADesiredState records operator intent separately from observed VM state.
// Callers mutate it only while holding the per-cluster HA operation lock; the
// supervisor takes the same lock before reading intent and reconciling VMs.
type HADesiredState struct {
	APIVersion     string    `json:"apiVersion"`
	Kind           string    `json:"kind"`
	Cluster        string    `json:"cluster"`
	ClusterState   string    `json:"clusterState"`
	StoppedMembers []int     `json:"stoppedMembers,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

func haDesiredStatePath(name string) (string, error) {
	configPath, err := HAConfigPath(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(configPath), haDesiredStateFilename), nil
}

func defaultHADesiredState(name string) HADesiredState {
	return HADesiredState{
		APIVersion:   haDesiredStateAPIVersion,
		Kind:         haDesiredStateKind,
		Cluster:      name,
		ClusterState: haDesiredRunning,
	}
}

func loadHADesiredState(name string) (HADesiredState, error) {
	path, err := haDesiredStatePath(name)
	if err != nil {
		return HADesiredState{}, err
	}
	data, err := readExactHADesiredStateFile(path, openHADesiredStateFile)
	if errors.Is(err, os.ErrNotExist) {
		return defaultHADesiredState(name), nil
	}
	if err != nil {
		return HADesiredState{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state HADesiredState
	if err := decoder.Decode(&state); err != nil {
		return HADesiredState{}, fmt.Errorf("decode HA desired state: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return HADesiredState{}, fmt.Errorf("decode HA desired state: %w", err)
	}
	if err := validateHADesiredState(state, name); err != nil {
		return HADesiredState{}, err
	}
	return state, nil
}

type haDesiredStateOpenFunc func(string) (*os.File, error)

func openHADesiredStateFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

func readExactHADesiredStateFile(path string, openFile haDesiredStateOpenFunc) (data []byte, err error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read HA desired state: %w", err)
	}
	if err := validateHADesiredStateFileInfo(pathInfo); err != nil {
		return nil, err
	}
	file, err := openFile(path)
	if err != nil {
		return nil, fmt.Errorf("open HA desired state without following links: %w", err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened HA desired state: %w", err)
	}
	if err := validateHADesiredStateFileInfo(openedInfo); err != nil {
		return nil, err
	}
	if !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("HA desired state changed while it was being opened")
	}
	if openedInfo.Size() > haDesiredStateMaximum {
		return nil, fmt.Errorf("HA desired state exceeds maximum size %d", haDesiredStateMaximum)
	}
	data, err = io.ReadAll(io.LimitReader(file, haDesiredStateMaximum+1))
	if err != nil {
		return nil, fmt.Errorf("read HA desired state: %w", err)
	}
	if int64(len(data)) > haDesiredStateMaximum {
		return nil, fmt.Errorf("HA desired state exceeds maximum size %d", haDesiredStateMaximum)
	}
	finalInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("reinspect HA desired state after reading: %w", err)
	}
	if err := validateHADesiredStateFileInfo(finalInfo); err != nil {
		return nil, err
	}
	if !os.SameFile(openedInfo, finalInfo) {
		return nil, fmt.Errorf("HA desired state changed while it was being read")
	}
	return data, nil
}

func validateHADesiredStateFileInfo(info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !haPathOwnedByEffectiveUser(info) {
		return fmt.Errorf("HA desired state must be one private regular file owned by the current user")
	}
	return nil
}

func validateHADesiredState(state HADesiredState, name string) error {
	if state.APIVersion != haDesiredStateAPIVersion || state.Kind != haDesiredStateKind || state.Cluster != name {
		return fmt.Errorf("HA desired state identity does not match cluster %q", name)
	}
	if state.ClusterState != haDesiredRunning && state.ClusterState != haDesiredStopped {
		return fmt.Errorf("HA desired cluster state must be %q or %q", haDesiredRunning, haDesiredStopped)
	}
	previous := 0
	if len(state.StoppedMembers) > 1 {
		return fmt.Errorf("HA desired state may intentionally stop at most one member while the cluster is running")
	}
	for _, id := range state.StoppedMembers {
		if id < 1 || id > haMemberCount || id <= previous {
			return fmt.Errorf("HA desired stopped members must be unique sorted IDs 1, 2, or 3")
		}
		previous = id
	}
	if state.UpdatedAt.IsZero() || state.UpdatedAt.After(time.Now().UTC().Add(5*time.Minute)) {
		return fmt.Errorf("HA desired state has an invalid update time")
	}
	return nil
}

func saveHADesiredState(state HADesiredState) error {
	state.APIVersion = haDesiredStateAPIVersion
	state.Kind = haDesiredStateKind
	state.UpdatedAt = time.Now().UTC()
	state.StoppedMembers = append([]int(nil), state.StoppedMembers...)
	sort.Ints(state.StoppedMembers)
	if err := validateHADesiredState(state, state.Cluster); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode HA desired state: %w", err)
	}
	path, err := haDesiredStatePath(state.Cluster)
	if err != nil {
		return err
	}
	if err := writePrivateFileAtomic(path, append(data, '\n')); err != nil {
		return fmt.Errorf("save HA desired state: %w", err)
	}
	return nil
}

func haMemberIntentionallyStopped(state HADesiredState, id int) bool {
	for _, stopped := range state.StoppedMembers {
		if stopped == id {
			return true
		}
	}
	return false
}

func setHAMemberIntentLocked(name string, id int, stopped bool) error {
	state, err := loadHADesiredState(name)
	if err != nil {
		return err
	}
	members := make([]int, 0, haMemberCount)
	for _, candidate := range state.StoppedMembers {
		if candidate != id {
			members = append(members, candidate)
		}
	}
	if stopped {
		members = append(members, id)
	}
	state.StoppedMembers = members
	return saveHADesiredState(state)
}

// markHAClusterStoppedLocked and markHAClusterRunningLocked are integration
// points for StopHA/DeleteHA(--keep-data) and StartHA respectively. Their
// callers already own the HA operation lock.
func markHAClusterStoppedLocked(name string) error {
	state, err := loadHADesiredState(name)
	if err != nil {
		return err
	}
	state.ClusterState = haDesiredStopped
	return saveHADesiredState(state)
}

func markHAClusterRunningLocked(name string) error {
	state, err := loadHADesiredState(name)
	if err != nil {
		return err
	}
	state.ClusterState = haDesiredRunning
	state.StoppedMembers = nil
	return saveHADesiredState(state)
}
