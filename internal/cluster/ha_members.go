package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// StopHAMember stops exactly one healthy HA server while preserving the
// two-member embedded-etcd quorum. A second member cannot be intentionally
// stopped until the first has returned Ready through StartHAMember.
func (m *Manager) StopHAMember(ctx context.Context, name string, id int, timeout time.Duration) (result HAState, err error) {
	return m.withHAMemberOperation(ctx, name, id, timeout, func(operationCtx context.Context, config HAConfig, member HAMember, _ time.Duration) (HAState, error) {
		// Desired state is committed before the runtime mutation. If the caller is
		// interrupted after container stop reaches containerd, the supervisor will
		// continue enforcing the operator's request instead of starting it again.
		if err := setHAMemberIntentLocked(config.Name, member.ID, true); err != nil {
			return HAState{}, fmt.Errorf("persist intentional stop for HA member %d: %w", member.ID, err)
		}
		state, _, err := m.stopHAMemberLocked(operationCtx, config, member)
		return state, err
	})
}

// StartHAMember reconciles only one stopped or missing HA server envelope. The
// other two members must still be Ready and API-ready before reconstruction.
func (m *Manager) StartHAMember(ctx context.Context, name string, id int, timeout time.Duration) (result HAState, err error) {
	return m.withHAMemberOperation(ctx, name, id, timeout, func(operationCtx context.Context, config HAConfig, member HAMember, _ time.Duration) (HAState, error) {
		if err := setHAMemberIntentLocked(config.Name, member.ID, false); err != nil {
			return HAState{}, fmt.Errorf("clear intentional stop for HA member %d: %w", member.ID, err)
		}
		return m.startHAMemberLocked(operationCtx, config, member)
	})
}

// RestartHAMember performs one quorum-safe stop/start cycle and succeeds only
// after all three node/API pairs are Ready again.
func (m *Manager) RestartHAMember(ctx context.Context, name string, id int, timeout time.Duration) (result HAState, err error) {
	return m.withHAMemberOperation(ctx, name, id, timeout, func(operationCtx context.Context, config HAConfig, member HAMember, operationTimeout time.Duration) (result HAState, err error) {
		return m.restartHAMemberOperation(operationCtx, ctx, config, member, operationTimeout)
	})
}

func (m *Manager) restartHAMemberOperation(operationCtx, recoveryBase context.Context, config HAConfig, member HAMember, operationTimeout time.Duration) (result HAState, err error) {
	// Restart is a transient operation, not desired offline state. Clear a stale
	// member suppression before mutation so a failed command can be recovered by
	// the supervisor after this lock is released.
	if err := setHAMemberIntentLocked(config.Name, member.ID, false); err != nil {
		return HAState{}, fmt.Errorf("clear intentional stop before restarting HA member %d: %w", member.ID, err)
	}

	restartNeeded := false
	defer func() {
		if err == nil || !restartNeeded {
			return
		}
		recoveryTimeout := operationTimeout
		if recoveryTimeout > haRecoveryAttemptTimeout {
			recoveryTimeout = haRecoveryAttemptTimeout
		}
		recoveryCtx, cancelRecovery := context.WithTimeout(context.WithoutCancel(recoveryBase), recoveryTimeout)
		defer cancelRecovery()
		recovered, recoveryErr := m.startHAMemberLocked(recoveryCtx, config, member)
		if recoveryErr != nil {
			err = errors.Join(err, fmt.Errorf("best-effort restart recovery for HA member %d failed: %w", member.ID, recoveryErr))
			return
		}
		result = recovered
	}()

	if _, err := m.preflightHAMemberTopology(operationCtx, config, member.ID, false); err != nil {
		return HAState{}, err
	}
	state, err := m.StatusHA(operationCtx, config.Name)
	if err != nil {
		return HAState{}, err
	}
	if !allHAMembersReady(state) {
		return HAState{}, fmt.Errorf("refusing to restart HA member %d: all three node/API pairs must be Ready before mutation (currently %d/3)", member.ID, state.ReadyMembers)
	}
	_, restartNeeded, err = m.stopHAMemberLocked(operationCtx, config, member)
	if err != nil {
		return HAState{}, err
	}
	result, err = m.startHAMemberLocked(operationCtx, config, member)
	if err == nil {
		restartNeeded = false
	}
	return result, err
}

func validateHAMemberOperationInput(name string, id int, timeout time.Duration) error {
	if !dnsLabel.MatchString(name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if id < 1 || id > haMemberCount {
		return fmt.Errorf("HA member ID must be 1, 2, or 3")
	}
	if timeout < 0 {
		return fmt.Errorf("HA member operation timeout must not be negative")
	}
	if timeout > 0 && timeout < time.Second {
		return fmt.Errorf("HA member operation timeout must be at least 1s")
	}
	return nil
}

type haMemberOperationFunc func(context.Context, HAConfig, HAMember, time.Duration) (HAState, error)

// withHAMemberOperation validates only immutable command input before the
// flock. Saved config, member identity and the default timeout are loaded after
// exclusive acquisition so a waiter can never act on a deleted or replaced
// cluster configuration. The operation deadline is always measured from entry,
// including time spent waiting for the lock.
func (m *Manager) withHAMemberOperation(ctx context.Context, name string, id int, timeout time.Duration, operation haMemberOperationFunc) (result HAState, err error) {
	startedAt := time.Now()
	if err := validateHAMemberOperationInput(name, id, timeout); err != nil {
		return HAState{}, err
	}
	lockCtx := ctx
	cancelLock := func() {}
	if timeout > 0 {
		lockCtx, cancelLock = context.WithDeadline(ctx, startedAt.Add(timeout))
	} else {
		lockCtx, cancelLock = context.WithTimeout(ctx, haRecoveryAttemptTimeout)
	}
	lock, err := acquireHAOperationLock(lockCtx, name)
	cancelLock()
	if err != nil {
		return HAState{}, err
	}
	defer func() { err = errors.Join(err, lock.release()) }()
	config, member, operationTimeout, err := loadHAMemberOperationLocked(name, id, timeout)
	if err != nil {
		return HAState{}, err
	}
	deadline := startedAt.Add(operationTimeout)
	if !time.Now().Before(deadline) {
		return HAState{}, fmt.Errorf("HA member operation deadline elapsed while waiting for the operation lock: %w", context.DeadlineExceeded)
	}
	operationCtx, cancelOperation := context.WithDeadline(ctx, deadline)
	defer cancelOperation()
	if err := operationCtx.Err(); err != nil {
		return HAState{}, fmt.Errorf("HA member operation deadline elapsed before mutation: %w", err)
	}
	return operation(operationCtx, config, member, operationTimeout)
}

func loadHAMemberOperationLocked(name string, id int, timeout time.Duration) (HAConfig, HAMember, time.Duration, error) {
	config, err := loadHAConfig(name)
	if err != nil {
		return HAConfig{}, HAMember{}, 0, err
	}
	member := memberByID(config.Members, id)
	if member.ID == 0 {
		return HAConfig{}, HAMember{}, 0, fmt.Errorf("HA member %d is not declared by cluster %q", id, config.Name)
	}
	if timeout == 0 {
		timeout = config.StartupTimeout
	}
	config.StartupTimeout = timeout
	return config, member, timeout, nil
}

func (m *Manager) preflightHAMemberTopology(ctx context.Context, config HAConfig, targetID int, allowMissingTarget bool) (haPreflight, error) {
	preflight, err := m.preflightHA(ctx, config, false)
	if err != nil {
		return haPreflight{}, err
	}
	if !preflight.networkExists {
		return haPreflight{}, fmt.Errorf("HA member maintenance requires the exact APC-owned network %q", config.NetworkName)
	}
	if len(preflight.volumeExists) != len(config.Members) {
		return haPreflight{}, fmt.Errorf("HA member maintenance requires all three exact APC-owned member volumes; found %d", len(preflight.volumeExists))
	}
	for _, member := range config.Members {
		if _, exists := preflight.containerRecord[member.ID]; exists {
			continue
		}
		if allowMissingTarget && member.ID == targetID {
			continue
		}
		return haPreflight{}, fmt.Errorf("HA member maintenance requires exact APC-owned server envelope %q", HAContainerName(config.Name, member.ID))
	}
	return preflight, nil
}

func (m *Manager) stopHAMemberLocked(ctx context.Context, config HAConfig, member HAMember) (HAState, bool, error) {
	preflight, err := m.preflightHAMemberTopology(ctx, config, member.ID, true)
	if err != nil {
		return HAState{}, false, err
	}
	state, err := m.StatusHA(ctx, config.Name)
	if err != nil {
		return HAState{}, false, err
	}
	targetState, found := haMemberStateByID(state, member.ID)
	if !found {
		return HAState{}, false, fmt.Errorf("HA status did not include declared member %d", member.ID)
	}
	record, exists := preflight.containerRecord[member.ID]
	if !exists || strings.EqualFold(record.Status.State, "stopped") {
		if !otherHAMembersReady(state, member.ID) {
			return HAState{}, false, fmt.Errorf("HA member %d is already offline, but the other two node/API pairs are not both Ready", member.ID)
		}
		return state, false, nil
	}
	if !strings.EqualFold(targetState.RuntimeState, "running") || !allHAMembersReady(state) {
		return HAState{}, false, fmt.Errorf("refusing to stop HA member %d: all three node/API pairs must be Ready before mutation (currently %d/3)", member.ID, state.ReadyMembers)
	}
	if _, err := m.validateHAEtcdTopology(ctx, config); err != nil {
		return HAState{}, false, fmt.Errorf("refusing to stop HA member %d without exact healthy embedded-etcd topology: %w", member.ID, err)
	}
	if err := m.runHABounded(ctx, fmt.Sprintf("stop HA server member %d", member.ID), "stop", HAContainerName(config.Name, member.ID)); err != nil {
		// The runtime may have applied the stop even when its client reports an
		// error, so callers performing restart must conservatively recover it.
		return HAState{}, true, err
	}
	stopped, err := m.waitHAMemberStopped(ctx, config, member)
	return stopped, true, err
}

func (m *Manager) startHAMemberLocked(ctx context.Context, config HAConfig, member HAMember) (HAState, error) {
	preflight, err := m.preflightHAMemberTopology(ctx, config, member.ID, true)
	if err != nil {
		return HAState{}, err
	}
	state, err := m.StatusHA(ctx, config.Name)
	if err != nil {
		return HAState{}, err
	}
	if allHAMembersReady(state) {
		return state, nil
	}
	if !otherHAMembersReady(state, member.ID) {
		return HAState{}, fmt.Errorf("refusing to start HA member %d: the other two node/API pairs must both be Ready (currently %d/3 total)", member.ID, state.ReadyMembers)
	}
	if err := validateHACurrentRuntimeIPReservations(config, preflight.containerRecord); err != nil {
		return HAState{}, fmt.Errorf("refusing to start HA member %d with an existing runtime/stable IP collision: %w", member.ID, err)
	}
	record, exists := preflight.containerRecord[member.ID]
	if exists && !strings.EqualFold(record.Status.State, "running") && !strings.EqualFold(record.Status.State, "stopped") {
		return HAState{}, fmt.Errorf("refusing to replace HA member %d while its envelope is %s", member.ID, record.Status.State)
	}
	if !exists || strings.EqualFold(record.Status.State, "stopped") {
		if err := m.reconcileHAMember(ctx, config, member, record); err != nil {
			return HAState{}, fmt.Errorf("reconcile HA member %d: %w", member.ID, err)
		}
	}
	return m.waitHAClusterReady(ctx, config)
}

func (m *Manager) waitHAMemberStopped(ctx context.Context, config HAConfig, member HAMember) (HAState, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastReady := haMemberCount
	for {
		state, err := m.StatusHA(ctx, config.Name)
		if err == nil {
			lastReady = state.ReadyMembers
			target, found := haMemberStateByID(state, member.ID)
			if found && !strings.EqualFold(target.RuntimeState, "running") && otherHAMembersReady(state, member.ID) {
				return state, nil
			}
		} else if ctx.Err() == nil {
			return HAState{}, err
		}
		select {
		case <-ctx.Done():
			return HAState{}, fmt.Errorf("HA member %d did not stop with two Ready node/API pairs; cluster reached %d/3 within %s: %w", member.ID, lastReady, config.StartupTimeout, ctx.Err())
		case <-ticker.C:
		}
	}
}

func haMemberStateByID(state HAState, id int) (HAMemberState, bool) {
	for _, member := range state.Members {
		if member.ID == id {
			return member, true
		}
	}
	return HAMemberState{}, false
}

func allHAMembersReady(state HAState) bool {
	if len(state.Members) != haMemberCount || state.ReadyMembers != haMemberCount {
		return false
	}
	for _, member := range state.Members {
		if !member.NodeReady || !member.APIReady || !strings.EqualFold(member.RuntimeState, "running") {
			return false
		}
	}
	return true
}

func otherHAMembersReady(state HAState, excludedID int) bool {
	ready := 0
	for _, member := range state.Members {
		if member.ID == excludedID {
			continue
		}
		if !member.NodeReady || !member.APIReady || !strings.EqualFold(member.RuntimeState, "running") {
			return false
		}
		ready++
	}
	return ready == haMemberCount-1
}
