package cluster

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type contextBlockingRunner struct {
	started chan struct{}
	calls   atomic.Int32
}

func (runner *contextBlockingRunner) Run(ctx context.Context, _ string, _ ...string) ([]byte, []byte, error) {
	runner.calls.Add(1)
	select {
	case runner.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func TestHASupervisorRetriesProxyWhileReconcilingAndCancelsCleanly(t *testing.T) {
	manager := NewManager("container")
	var reconciles atomic.Int32
	var proxyAttempts atomic.Int32
	var activeProxyCalls atomic.Int32
	var maximumActiveProxyCalls atomic.Int32
	runtime := supervisorRuntime{
		reconcile: func(context.Context, SuperviseOptions) error {
			reconciles.Add(1)
			return nil
		},
		serveHAProxy: func(ctx context.Context, name string) error {
			if name != "ha-test" {
				t.Errorf("proxy cluster = %q", name)
			}
			active := activeProxyCalls.Add(1)
			defer activeProxyCalls.Add(-1)
			for {
				maximum := maximumActiveProxyCalls.Load()
				if active <= maximum || maximumActiveProxyCalls.CompareAndSwap(maximum, active) {
					break
				}
			}
			attempt := proxyAttempts.Add(1)
			if attempt <= 2 {
				return errors.New("proxy unavailable")
			}
			<-ctx.Done()
			return nil
		},
		proxyInitialRetry: 5 * time.Millisecond,
		proxyMaximumRetry: 10 * time.Millisecond,
	}
	var output bytes.Buffer
	options := SuperviseOptions{Role: "ha", Name: "ha-test", Interval: 5 * time.Millisecond, Output: &output}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.supervise(ctx, options, runtime) }()

	deadline := time.Now().Add(2 * time.Second)
	for proxyAttempts.Load() < 3 || reconciles.Load() < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("supervisor did not retry and reconcile: proxy=%d reconcile=%d", proxyAttempts.Load(), reconciles.Load())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("supervisor cancellation error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HA supervisor did not stop after cancellation")
	}

	if activeProxyCalls.Load() != 0 {
		t.Fatalf("active proxy calls after cancellation = %d", activeProxyCalls.Load())
	}
	if maximumActiveProxyCalls.Load() != 1 {
		t.Fatalf("concurrent proxy calls = %d, want 1", maximumActiveProxyCalls.Load())
	}
	logOutput := output.String()
	if strings.Count(logOutput, "HA API proxy failed: proxy unavailable") != 2 ||
		!strings.Contains(logOutput, "retrying in 5ms") ||
		!strings.Contains(logOutput, "retrying in 10ms") {
		t.Fatalf("unexpected bounded retry log: %s", logOutput)
	}
}

func TestHASupervisorRespectsPersistedClusterStopAcrossTicks(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.states[2] = "stopped"
	runner.ready[2] = false
	runner.states[3] = "stopped"
	runner.ready[3] = false
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	options := SuperviseOptions{Role: "ha", Name: config.Name}
	for tick := 0; tick < 2; tick++ {
		if err := manager.reconcileSupervisedHAAfterService(context.Background(), options, newHASupervisorState()); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
	}
	want := "stop:" + HAContainerName(config.Name, 1)
	if len(runner.mutations) != 1 || runner.mutations[0] != want {
		t.Fatalf("interrupted cluster stop mutations = %#v, want %q", runner.mutations, want)
	}
}

func TestHASupervisorFailsClosedOnNonterminalRecoveryJournal(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	journal := HARecoveryState{
		APIVersion:      haSnapshotAPIVersion,
		Kind:            haRecoveryStateKind,
		Cluster:         config.Name,
		ClusterIdentity: strings.Repeat("a", 64),
		SnapshotPath:    "/private/tmp/ha-lab.snapshot",
		Phase:           "resetting-member-1",
		StartedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}
	if err := saveHARecoveryState(journal); err != nil {
		t.Fatal(err)
	}
	err := manager.reconcileSupervisedHAAfterService(context.Background(), SuperviseOptions{Role: "ha", Name: config.Name}, newHASupervisorState())
	if err == nil || !strings.Contains(err.Error(), "nonterminal phase \"resetting-member-1\"") || !strings.Contains(err.Error(), "apc cluster ha restore") {
		t.Fatalf("journal gate error = %v", err)
	}
	if len(runner.calls) != 0 || len(runner.mutations) != 0 {
		t.Fatalf("nonterminal recovery journal allowed runtime access: calls=%#v mutations=%#v", runner.calls, runner.mutations)
	}
}

func TestHASupervisorPreservesIntentionalMemberStopAcrossTicks(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.states[2] = "stopped"
	runner.ready[2] = false
	if err := setHAMemberIntentLocked(config.Name, 2, true); err != nil {
		t.Fatal(err)
	}
	options := SuperviseOptions{Role: "ha", Name: config.Name}
	for tick := 0; tick < 2; tick++ {
		// A fresh state models a supervisor process restart; durable intent must
		// still suppress automatic member reconstruction.
		if err := manager.reconcileSupervisedHAAfterService(context.Background(), options, newHASupervisorState()); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
	}
	if len(runner.mutations) != 0 {
		t.Fatalf("intentionally stopped member was reconciled: %#v", runner.mutations)
	}
}

func TestHASupervisorRepairsUnintentionalStoppedMember(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.states[2] = "stopped"
	runner.ready[2] = false
	if err := manager.reconcileSupervisedHAAfterService(context.Background(), SuperviseOptions{Role: "ha", Name: config.Name}, newHASupervisorState()); err != nil {
		t.Fatal(err)
	}
	target := HAContainerName(config.Name, 2)
	want := []string{"delete:" + target, "run:" + target}
	if strings.Join(runner.mutations, "|") != strings.Join(want, "|") {
		t.Fatalf("repair mutations = %#v, want %#v", runner.mutations, want)
	}
}

func TestHASupervisorRepairsRunningUnhealthyMemberWithoutProbingTargetEtcd(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.ready[2] = false
	runner.etcdErrorMember = 2
	if err := manager.reconcileSupervisedHAAfterService(context.Background(), SuperviseOptions{Role: "ha", Name: config.Name}, newHASupervisorState()); err != nil {
		t.Fatal(err)
	}
	target := HAContainerName(config.Name, 2)
	want := []string{"stop:" + target, "delete:" + target, "run:" + target}
	if strings.Join(runner.mutations, "|") != strings.Join(want, "|") {
		t.Fatalf("repair mutations = %#v, want %#v", runner.mutations, want)
	}
	if runner.etcdProbes[1] != 1 || runner.etcdProbes[3] != 1 || runner.etcdProbes[2] != 0 {
		t.Fatalf("repair etcd probes = %#v", runner.etcdProbes)
	}
}

func TestHASupervisorEnforcesIntentionalStopForRunningUnhealthyMember(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.ready[2] = false
	runner.etcdErrorMember = 2
	if err := setHAMemberIntentLocked(config.Name, 2, true); err != nil {
		t.Fatal(err)
	}
	if err := manager.reconcileSupervisedHAAfterService(context.Background(), SuperviseOptions{Role: "ha", Name: config.Name}, newHASupervisorState()); err != nil {
		t.Fatal(err)
	}
	want := "stop:" + HAContainerName(config.Name, 2)
	if len(runner.mutations) != 1 || runner.mutations[0] != want {
		t.Fatalf("intentional stop mutations = %#v, want %q", runner.mutations, want)
	}
	if runner.etcdProbes[2] != 0 {
		t.Fatalf("unhealthy target etcd was probed: %#v", runner.etcdProbes)
	}
}

func TestHASupervisorBacksOffDivergentEtcdRepairAcrossTicks(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.ready[2] = false
	runner.etcdOutput = func(memberID int) []byte {
		mutate := (func(string) string)(nil)
		if memberID == 3 {
			mutate = func(value string) string {
				return strings.Replace(value, fakeHAEtcdServerIDs[1], "1111111111111111", 1)
			}
		}
		return fakeHAEtcdProbeOutputWith(memberID, mutate)
	}
	supervisor := newHASupervisorState()
	options := SuperviseOptions{Role: "ha", Name: config.Name}
	firstErr := manager.reconcileSupervisedHAAfterService(context.Background(), options, supervisor)
	if firstErr == nil || !strings.Contains(firstErr.Error(), "disagree on target") {
		t.Fatalf("first repair error = %v", firstErr)
	}
	firstProbes := runner.etcdProbes[1] + runner.etcdProbes[3]
	if err := manager.reconcileSupervisedHAAfterService(context.Background(), options, supervisor); err != nil {
		t.Fatalf("backoff tick returned error: %v", err)
	}
	if probes := runner.etcdProbes[1] + runner.etcdProbes[3]; probes != firstProbes {
		t.Fatalf("backoff re-probed etcd: before=%d after=%d", firstProbes, probes)
	}
	if len(runner.mutations) != 0 {
		t.Fatalf("divergent quorum was mutated: %#v", runner.mutations)
	}
}

func TestHASupervisorReconcileDeadlineReleasesOperationLock(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &contextBlockingRunner{started: make(chan struct{}, 1)}
	manager := NewManager("container")
	manager.runner = runner
	options := SuperviseOptions{Role: "ha", Name: config.Name, Interval: 100 * time.Millisecond}
	done := make(chan error, 1)
	startedAt := time.Now()
	go func() {
		done <- manager.reconcileSupervisedHAAfterService(context.Background(), options, newHASupervisorState())
	}()
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not reach the blocking runtime read")
	}

	lockCtx, cancelLock := context.WithTimeout(context.Background(), time.Second)
	defer cancelLock()
	lock, err := acquireHAOperationLock(lockCtx, config.Name)
	if err != nil {
		t.Fatalf("supervisor did not release HA lock after its deadline: %v", err)
	}
	if err := lock.release(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("bounded supervisor error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded supervisor did not return after releasing its lock")
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("supervisor deadline took %s", elapsed)
	}
	if runner.calls.Load() == 0 {
		t.Fatal("blocking runner was not invoked")
	}
}

func TestHASupervisorStartupGracePreventsRestartOfBootingMember(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.states[2] = "stopped"
	runner.ready[2] = false
	runner.readyAfterRun = false
	now := time.Now().UTC()
	supervisor := newHASupervisorState()
	supervisor.now = func() time.Time { return now }
	options := SuperviseOptions{Role: "ha", Name: config.Name}
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 25*time.Millisecond)
	firstErr := manager.reconcileSupervisedHAAfterService(firstCtx, options, supervisor)
	cancelFirst()
	if firstErr == nil || !errors.Is(firstErr, context.DeadlineExceeded) {
		t.Fatalf("initial bounded start error = %v", firstErr)
	}
	target := HAContainerName(config.Name, 2)
	initialMutations := []string{"delete:" + target, "run:" + target}
	if strings.Join(runner.mutations, "|") != strings.Join(initialMutations, "|") || runner.states[2] != "running" || runner.ready[2] {
		t.Fatalf("initial boot state: mutations=%#v runtime=%q ready=%v", runner.mutations, runner.states[2], runner.ready[2])
	}

	if err := manager.reconcileSupervisedHAAfterService(context.Background(), options, supervisor); err != nil {
		t.Fatalf("startup grace tick = %v", err)
	}
	if strings.Join(runner.mutations, "|") != strings.Join(initialMutations, "|") {
		t.Fatalf("booting member was restarted inside grace: %#v", runner.mutations)
	}

	now = now.Add(config.StartupTimeout + time.Second)
	runner.readyAfterRun = true
	if err := manager.reconcileSupervisedHAAfterService(context.Background(), options, supervisor); err != nil {
		t.Fatalf("post-grace repair = %v", err)
	}
	want := append(initialMutations, "stop:"+target, "delete:"+target, "run:"+target)
	if strings.Join(runner.mutations, "|") != strings.Join(want, "|") {
		t.Fatalf("post-grace mutations = %#v, want %#v", runner.mutations, want)
	}
}
