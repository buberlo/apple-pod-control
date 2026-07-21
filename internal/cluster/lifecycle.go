package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	haProxySupervisorInitialRetry = time.Second
	haProxySupervisorMaximumRetry = 30 * time.Second
	haRepairInitialBackoff        = 30 * time.Second
	haRepairMaximumBackoff        = 5 * time.Minute
	haSupervisorDefaultInterval   = 15 * time.Second
)

type SuperviseOptions struct {
	Role     string
	Name     string
	Interval time.Duration
	Output   io.Writer
}

// Supervise continuously keeps the Apple container service and one APC node,
// or the complete local HA member set, running. launchd restarts this loop if
// the process itself fails.
func (m *Manager) Supervise(ctx context.Context, options SuperviseOptions) error {
	if ctx == nil {
		return fmt.Errorf("supervisor context is nil")
	}
	if options.Role != "server" && options.Role != "agent" && options.Role != "ha" {
		return fmt.Errorf("role must be server, agent, or ha")
	}
	if !dnsLabel.MatchString(options.Name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if options.Interval == 0 {
		options.Interval = haSupervisorDefaultInterval
	}
	if options.Interval < 5*time.Second {
		return fmt.Errorf("supervisor interval must be at least 5s")
	}
	if options.Output == nil {
		options.Output = io.Discard
	}
	return m.supervise(ctx, options, supervisorRuntime{
		reconcile:         m.reconcileSupervisedNode,
		reconcileHA:       m.reconcileSupervisedHA,
		serveHAProxy:      m.ServeHAProxy,
		proxyInitialRetry: haProxySupervisorInitialRetry,
		proxyMaximumRetry: haProxySupervisorMaximumRetry,
	})
}

type supervisorRuntime struct {
	reconcile         func(context.Context, SuperviseOptions) error
	reconcileHA       func(context.Context, SuperviseOptions, *haSupervisorState) error
	serveHAProxy      func(context.Context, string) error
	proxyInitialRetry time.Duration
	proxyMaximumRetry time.Duration
}

type supervisorLogger struct {
	mu     sync.Mutex
	output io.Writer
}

func (l *supervisorLogger) printf(format string, arguments ...any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := fmt.Fprintf(l.output, "%s "+format+"\n", append([]any{time.Now().UTC().Format(time.RFC3339)}, arguments...)...); err != nil {
		return fmt.Errorf("write supervisor log: %w", err)
	}
	return nil
}

func (m *Manager) supervise(ctx context.Context, options SuperviseOptions, runtime supervisorRuntime) error {
	logger := &supervisorLogger{output: options.Output}
	haState := newHASupervisorState()
	reconcile := func() error {
		if options.Role == "ha" && runtime.reconcileHA != nil {
			return runtime.reconcileHA(ctx, options, haState)
		}
		return runtime.reconcile(ctx, options)
	}
	var stopProxy context.CancelFunc
	var proxyDone <-chan error
	proxyResultRead := false
	if options.Role == "ha" {
		proxyCtx, cancelProxy := context.WithCancel(ctx)
		done := make(chan error, 1)
		stopProxy = cancelProxy
		proxyDone = done
		go func() {
			done <- superviseHAProxy(proxyCtx, options.Name, runtime, logger)
		}()
		defer func() {
			stopProxy()
			if !proxyResultRead {
				<-proxyDone
			}
		}()
	}

	if err := reconcile(); err != nil {
		if logErr := logger.printf("reconcile failed: %v", err); logErr != nil {
			return errors.Join(fmt.Errorf("supervisor reconcile failed: %w", err), logErr)
		}
	}
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case proxyErr := <-proxyDone:
			proxyResultRead = true
			if proxyErr != nil {
				return proxyErr
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("HA API proxy supervisor stopped unexpectedly")
		case <-ticker.C:
			if err := reconcile(); err != nil {
				if logErr := logger.printf("reconcile failed: %v", err); logErr != nil {
					return errors.Join(fmt.Errorf("supervisor reconcile failed: %w", err), logErr)
				}
			}
		}
	}
}

func superviseHAProxy(ctx context.Context, name string, runtime supervisorRuntime, logger *supervisorLogger) error {
	retry := runtime.proxyInitialRetry
	if retry <= 0 {
		retry = haProxySupervisorInitialRetry
	}
	maximumRetry := runtime.proxyMaximumRetry
	if maximumRetry < retry {
		maximumRetry = retry
	}
	for {
		err := runtime.serveHAProxy(ctx, name)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			if logErr := logger.printf("HA API proxy stopped unexpectedly; retrying in %s", retry); logErr != nil {
				return logErr
			}
		} else {
			if logErr := logger.printf("HA API proxy failed: %v; retrying in %s", err, retry); logErr != nil {
				return logErr
			}
		}

		timer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil
		case <-timer.C:
		}
		if retry < maximumRetry {
			if retry > maximumRetry/2 {
				retry = maximumRetry
			} else {
				retry *= 2
			}
		}
	}
}

func (m *Manager) reconcileSupervisedNode(ctx context.Context, options SuperviseOptions) error {
	if err := m.ensureAppleContainerService(ctx); err != nil {
		return err
	}
	if options.Role == "ha" {
		return m.reconcileSupervisedHAAfterService(ctx, options, newHASupervisorState())
	}
	if options.Role == "agent" {
		state, err := m.AgentStatus(ctx, options.Name)
		if err == nil && strings.EqualFold(state.RuntimeState, "running") {
			return nil
		}
		_, startErr := m.StartAgent(ctx, options.Name, 45*time.Second)
		return startErr
	}
	state, err := m.Status(ctx, options.Name)
	if err == nil && strings.EqualFold(state.RuntimeState, "running") && state.NodeReady {
		return nil
	}
	_, startErr := m.Start(ctx, options.Name, 2*time.Minute)
	return startErr
}

func (m *Manager) ensureAppleContainerService(ctx context.Context) error {
	if _, stderr, err := m.runner.Run(ctx, m.binary, "system", "status"); err != nil {
		if _, startStderr, startErr := m.runner.Run(ctx, m.binary, "system", "start"); startErr != nil {
			return errors.Join(commandError("read Apple container service status", stderr, err), commandError("start Apple container service", startStderr, startErr))
		}
	}
	return nil
}

type haRepairBackoff struct {
	failures int
	next     time.Time
}

type haSupervisorState struct {
	now          func() time.Time
	repairs      map[int]haRepairBackoff
	startupUntil map[int]time.Time
}

func newHASupervisorState() *haSupervisorState {
	return &haSupervisorState{
		now:          time.Now,
		repairs:      make(map[int]haRepairBackoff),
		startupUntil: make(map[int]time.Time),
	}
}

func (state *haSupervisorState) repairAllowed(id int) bool {
	return !state.now().Before(state.repairs[id].next)
}

func (state *haSupervisorState) recordRepair(id int, err error) {
	if err == nil {
		delete(state.repairs, id)
		return
	}
	entry := state.repairs[id]
	entry.failures++
	delay := haRepairInitialBackoff
	for attempt := 1; attempt < entry.failures && delay < haRepairMaximumBackoff; attempt++ {
		if delay > haRepairMaximumBackoff/2 {
			delay = haRepairMaximumBackoff
		} else {
			delay *= 2
		}
	}
	entry.next = state.now().Add(delay)
	state.repairs[id] = entry
}

func (state *haSupervisorState) recordStartup(id int, grace time.Duration) {
	if grace <= 0 {
		grace = haSupervisorDefaultInterval
	}
	until := state.now().Add(grace)
	if until.After(state.startupUntil[id]) {
		state.startupUntil[id] = until
	}
}

func (state *haSupervisorState) startupInProgress(id int) bool {
	return state.now().Before(state.startupUntil[id])
}

func (state *haSupervisorState) clearStartup(id int) {
	delete(state.startupUntil, id)
}

func (m *Manager) reconcileSupervisedHA(ctx context.Context, options SuperviseOptions, supervisor *haSupervisorState) error {
	reconcileCtx, cancel := context.WithTimeout(ctx, haSupervisorReconcileTimeout(options.Interval))
	defer cancel()
	if err := m.ensureAppleContainerService(reconcileCtx); err != nil {
		return err
	}
	return m.reconcileSupervisedHAAfterService(reconcileCtx, options, supervisor)
}

func (m *Manager) reconcileSupervisedHAAfterService(ctx context.Context, options SuperviseOptions, supervisor *haSupervisorState) (err error) {
	reconcileCtx, cancel := context.WithTimeout(ctx, haSupervisorReconcileTimeout(options.Interval))
	defer cancel()
	ctx = reconcileCtx
	lock, err := acquireHALifecycleOperationLock(ctx, options.Name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.release()) }()
	config, err := loadHAConfig(options.Name)
	if err != nil {
		return err
	}
	if err := ensureHARecoveryJournalAllowsSupervision(config.Name); err != nil {
		return err
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		return err
	}
	if desired.ClusterState == haDesiredStopped {
		// A crash can occur after durable stop intent is written but before all
		// three stop calls complete. Keep reconciling toward fully stopped while
		// never reconstructing a missing keep-data envelope.
		return m.stopHALocked(ctx, config.Name)
	}
	state, err := m.StatusHA(ctx, config.Name)
	if err != nil {
		return err
	}
	if len(desired.StoppedMembers) == 1 {
		intentionalID := desired.StoppedMembers[0]
		intentional, found := haMemberStateByID(state, intentionalID)
		if !found {
			return fmt.Errorf("HA status omitted intentionally stopped member %d", intentionalID)
		}
		if allHAMembersReady(state) {
			_, _, stopErr := m.stopHAMemberLocked(ctx, config, memberByID(config.Members, intentionalID))
			return stopErr
		}
		if otherHAMembersReady(state, intentionalID) {
			if strings.EqualFold(intentional.RuntimeState, "running") {
				return m.stopIntentionalUnhealthyHAMemberLocked(ctx, config, memberByID(config.Members, intentionalID), state)
			}
			return nil
		}
		return m.reconcileHAWithIntentionalMemberStop(ctx, config, state, intentionalID)
	}
	if allHAMembersReady(state) {
		for _, member := range state.Members {
			supervisor.recordRepair(member.ID, nil)
			supervisor.clearStartup(member.ID)
		}
		return nil
	}
	unready := make([]HAMemberState, 0, haMemberCount)
	for _, member := range state.Members {
		if !member.NodeReady || !member.APIReady || !strings.EqualFold(member.RuntimeState, "running") {
			unready = append(unready, member)
		}
	}
	if len(unready) != 1 {
		for _, member := range unready {
			if !strings.EqualFold(member.RuntimeState, "running") {
				supervisor.recordStartup(member.ID, config.StartupTimeout)
			}
		}
		_, startErr := m.startHALocked(ctx, config.Name, 3*time.Minute)
		return startErr
	}
	targetState := unready[0]
	target := memberByID(config.Members, targetState.ID)
	if !otherHAMembersReady(state, target.ID) {
		return fmt.Errorf("cannot safely reconcile HA member %d: the other two node/API pairs are not Ready", target.ID)
	}
	if !strings.EqualFold(targetState.RuntimeState, "running") {
		supervisor.recordStartup(target.ID, config.StartupTimeout)
		_, startErr := m.startHAMemberLocked(ctx, config, target)
		return startErr
	}
	if supervisor.startupInProgress(target.ID) {
		return nil
	}
	if !supervisor.repairAllowed(target.ID) {
		return nil
	}
	repairErr := m.restartUnhealthyHAMemberLocked(ctx, config, target)
	supervisor.recordRepair(target.ID, repairErr)
	return repairErr
}

func ensureHARecoveryJournalAllowsSupervision(name string) error {
	journal, err := LoadHARecoveryState(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("refusing HA supervision because the protected recovery journal cannot be trusted: %w", err)
	}
	if haRecoveryJournalRuntimeSafe(journal) {
		return nil
	}
	action := fmt.Sprintf("rerun `apc cluster ha restore %s --from %q --yes` to resume or retry recovery", name, journal.SnapshotPath)
	if haRecoveryJournalRequiresRecover(journal) {
		action = fmt.Sprintf("rerun `apc cluster ha recover %s` with the same snapshot package and its independently retained manifest SHA-256", name)
	}
	return fmt.Errorf("refusing HA VM reconciliation while recovery journal is in nonterminal phase %q (recoverySucceeded=%t); %s", journal.Phase, journal.RecoverySucceeded, action)
}

func (m *Manager) stopIntentionalUnhealthyHAMemberLocked(ctx context.Context, config HAConfig, member HAMember, state HAState) error {
	if !otherHAMembersReady(state, member.ID) {
		return fmt.Errorf("refusing to enforce intentional stop for HA member %d without two Ready peers", member.ID)
	}
	preflight, err := m.preflightHAMemberTopology(ctx, config, member.ID, true)
	if err != nil {
		return err
	}
	record, exists := preflight.containerRecord[member.ID]
	if !exists || !strings.EqualFold(record.Status.State, "running") {
		return nil
	}
	if _, err := m.validateHAEtcdRepairQuorum(ctx, config, member.ID); err != nil {
		return fmt.Errorf("refusing to enforce intentional stop for unhealthy HA member %d without a proven two-voter etcd majority: %w", member.ID, err)
	}
	if err := m.runHABounded(ctx, fmt.Sprintf("stop intentionally disabled unhealthy HA server member %d", member.ID), "stop", HAContainerName(config.Name, member.ID)); err != nil {
		return err
	}
	_, err = m.waitHAMemberStopped(ctx, config, member)
	return err
}

func (m *Manager) reconcileHAWithIntentionalMemberStop(ctx context.Context, config HAConfig, state HAState, intentionalID int) error {
	var target *HAMemberState
	for index := range state.Members {
		member := &state.Members[index]
		if member.ID == intentionalID {
			continue
		}
		if !member.NodeReady || !member.APIReady || !strings.EqualFold(member.RuntimeState, "running") {
			if target != nil {
				return fmt.Errorf("multiple HA members are unavailable while member %d is intentionally stopped", intentionalID)
			}
			target = member
		}
	}
	if target == nil {
		return fmt.Errorf("intentionally stopped HA member %d has not reached a stable two-member state", intentionalID)
	}
	if strings.EqualFold(target.RuntimeState, "running") {
		return fmt.Errorf("HA member %d is running but unhealthy while member %d is intentionally stopped; refusing a no-quorum restart", target.ID, intentionalID)
	}
	preflight, err := m.preflightHAMemberTopology(ctx, config, target.ID, true)
	if err != nil {
		return err
	}
	record := preflight.containerRecord[target.ID]
	member := memberByID(config.Members, target.ID)
	if err := m.reconcileHAMember(ctx, config, member, record); err != nil {
		return err
	}
	return m.waitHAMemberReady(ctx, config, member, config.StartupTimeout)
}

func (m *Manager) restartUnhealthyHAMemberLocked(ctx context.Context, config HAConfig, member HAMember) (err error) {
	if _, err := m.preflightHAMemberTopology(ctx, config, member.ID, false); err != nil {
		return err
	}
	state, err := m.StatusHA(ctx, config.Name)
	if err != nil {
		return err
	}
	if !otherHAMembersReady(state, member.ID) {
		return fmt.Errorf("refusing to repair unhealthy HA member %d without two Ready peers", member.ID)
	}
	if _, err := m.validateHAEtcdRepairQuorum(ctx, config, member.ID); err != nil {
		return fmt.Errorf("refusing to repair unhealthy HA member %d without a proven two-voter embedded-etcd majority: %w", member.ID, err)
	}
	restartNeeded := true
	defer func() {
		if err == nil || !restartNeeded {
			return
		}
		recoveryTimeout := config.StartupTimeout
		if recoveryTimeout <= 0 || recoveryTimeout > haRecoveryAttemptTimeout {
			recoveryTimeout = haRecoveryAttemptTimeout
		}
		recoveryCtx, cancelRecovery := context.WithTimeout(ctx, recoveryTimeout)
		defer cancelRecovery()
		_, recoveryErr := m.startHAMemberLocked(recoveryCtx, config, member)
		if recoveryErr != nil {
			err = errors.Join(err, fmt.Errorf("recover unhealthy HA member %d after failed supervisor repair: %w", member.ID, recoveryErr))
		}
	}()
	if err := m.runHABounded(ctx, fmt.Sprintf("stop unhealthy HA server member %d", member.ID), "stop", HAContainerName(config.Name, member.ID)); err != nil {
		return err
	}
	if _, err := m.waitHAMemberStopped(ctx, config, member); err != nil {
		return err
	}
	if _, err := m.startHAMemberLocked(ctx, config, member); err != nil {
		return err
	}
	restartNeeded = false
	return nil
}

func haSupervisorReconcileTimeout(interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = haSupervisorDefaultInterval
	}
	timeout := interval - interval/5
	maximum := haRuntimeOperationTimeout - time.Second
	if maximum <= 0 {
		maximum = haRuntimeOperationTimeout
	}
	if timeout > maximum {
		timeout = maximum
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	return timeout
}

// DeleteServer removes the local server VM envelope. When keepData is false,
// it also removes the APC-owned data volume and local server configuration.
func (m *Manager) DeleteServer(ctx context.Context, name string, keepData bool) error {
	if !dnsLabel.MatchString(name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	files, err := serverConfigurationFiles(name)
	if err != nil {
		return err
	}
	if err := m.deleteOwnedContainer(ctx, ContainerName(name), name, "server"); err != nil {
		return err
	}
	if keepData {
		return nil
	}
	if err := m.deleteOwnedVolume(ctx, ServerVolumeName(name), name, "server"); err != nil {
		return err
	}
	if err := removeExactFiles(files); err != nil {
		return err
	}
	if err := clearCurrentCluster(name); err != nil {
		return err
	}
	return removeEmptyClusterDirectory(name)
}

// DeleteAgent removes the local agent VM envelope. When keepData is false, it
// also removes the APC-owned data volume and the saved local agent config.
func (m *Manager) DeleteAgent(ctx context.Context, name string, keepData bool) error {
	if !dnsLabel.MatchString(name) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if err := m.deleteOwnedContainer(ctx, AgentContainerName(name), name, "agent"); err != nil {
		return err
	}
	if keepData {
		return nil
	}
	if err := m.deleteOwnedVolume(ctx, AgentVolumeName(name), name, "agent"); err != nil {
		return err
	}
	path, err := agentConfigPath(name)
	if err != nil {
		return err
	}
	if err := removeExactFiles([]string{path}); err != nil {
		return err
	}
	return removeEmptyClusterDirectory(name)
}

func (m *Manager) deleteOwnedContainer(ctx context.Context, containerName, clusterName, role string) error {
	record, err := m.inspect(ctx, containerName)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateOwnedContainer(record, clusterName, role); err != nil {
		return err
	}
	if !strings.EqualFold(record.Status.State, "stopped") {
		if _, stderr, stopErr := m.runner.Run(ctx, m.binary, "stop", containerName); stopErr != nil {
			return commandError("stop APC "+role+" node before deletion", stderr, stopErr)
		}
	}
	if _, stderr, deleteErr := m.runner.Run(ctx, m.binary, "delete", containerName); deleteErr != nil {
		return commandError("delete APC "+role+" node", stderr, deleteErr)
	}
	return nil
}

func (m *Manager) inspectVolume(ctx context.Context, name string) (volumeRecord, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "volume", "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return volumeRecord{}, ErrNotFound
		}
		return volumeRecord{}, commandError("inspect K3s data volume", stderr, err)
	}
	var records []volumeRecord
	if err := json.Unmarshal(stdout, &records); err != nil {
		return volumeRecord{}, fmt.Errorf("decode volume inspect output: %w", err)
	}
	if len(records) != 1 {
		return volumeRecord{}, fmt.Errorf("volume inspect returned %d records", len(records))
	}
	return records[0], nil
}

func (m *Manager) deleteOwnedVolume(ctx context.Context, volumeName, clusterName, role string) error {
	record, err := m.inspectVolume(ctx, volumeName)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateOwnedVolume(record, volumeName, clusterName, role); err != nil {
		return err
	}
	if _, stderr, deleteErr := m.runner.Run(ctx, m.binary, "volume", "delete", volumeName); deleteErr != nil {
		return commandError("delete APC "+role+" data volume", stderr, deleteErr)
	}
	return nil
}

func validateOwnedVolume(record volumeRecord, volumeName, clusterName, role string) error {
	labels := record.Configuration.Labels
	if record.Configuration.Name != "" && record.Configuration.Name != volumeName {
		return fmt.Errorf("volume inspect returned unexpected volume %q", record.Configuration.Name)
	}
	if labels["apc.dev/managed"] != "true" || labels["apc.dev/cluster"] != clusterName || labels["apc.dev/role"] != role {
		return fmt.Errorf("volume %q exists but is not the expected APC %s volume", volumeName, role)
	}
	return nil
}

func serverConfigurationFiles(name string) ([]string, error) {
	configPath, err := clusterConfigPath(name)
	if err != nil {
		return nil, err
	}
	kubeconfigPath, err := KubeconfigPath(name)
	if err != nil {
		return nil, err
	}
	config, loadErr := loadClusterConfig(name)
	switch {
	case loadErr == nil:
		kubeconfigPath = config.KubeconfigPath
	case errors.Is(loadErr, os.ErrNotExist):
	default:
		return nil, loadErr
	}
	return []string{configPath, kubeconfigPath}, nil
}

func removeExactFiles(paths []string) error {
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		clean := filepath.Clean(path)
		if _, duplicate := seen[clean]; duplicate {
			continue
		}
		seen[clean] = struct{}{}
		if err := os.Remove(clean); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove APC configuration file %q: %w", clean, err)
		}
	}
	return nil
}

func clearCurrentCluster(name string) error {
	current, err := CurrentCluster()
	if err != nil {
		return nil
	}
	if current != name {
		return nil
	}
	path, err := currentClusterPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear current cluster: %w", err)
	}
	return nil
}

func removeEmptyClusterDirectory(name string) error {
	configPath, err := clusterConfigPath(name)
	if err != nil {
		return err
	}
	err = os.Remove(filepath.Dir(configPath))
	if err == nil || errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTEMPTY) {
		return nil
	}
	return fmt.Errorf("remove empty APC cluster directory: %w", err)
}
