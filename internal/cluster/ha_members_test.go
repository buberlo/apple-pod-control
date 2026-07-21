package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type haMemberLifecycleRunner struct {
	t                     *testing.T
	config                HAConfig
	states                map[int]string
	ready                 map[int]bool
	legacy                map[int]bool
	readyAfterRun         bool
	expireOnProbeAfterRun func()
	foreignMember         int
	failInspectAfterStop  bool
	postStopInspectFailed bool
	etcdOutput            func(int) []byte
	etcdErrorMember       int
	etcdProbes            map[int]int
	calls                 [][]string
	mutations             []string
}

// manualDeadlineContext lets lifecycle tests expire an operation at an exact
// reconciliation phase without relying on scheduler-sensitive wall-clock sleeps.
type manualDeadlineContext struct {
	context.Context
	done chan struct{}
}

func newManualDeadlineContext() *manualDeadlineContext {
	return &manualDeadlineContext{Context: context.Background(), done: make(chan struct{})}
}

func (ctx *manualDeadlineContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *manualDeadlineContext) Err() error {
	select {
	case <-ctx.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (ctx *manualDeadlineContext) expire() {
	close(ctx.done)
}

func newHAMemberLifecycleFixture(t *testing.T) (*Manager, *haMemberLifecycleRunner, HAConfig) {
	t.Helper()
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haMemberLifecycleRunner{
		t:             t,
		config:        config,
		states:        map[int]string{1: "running", 2: "running", 3: "running"},
		ready:         map[int]bool{1: true, 2: true, 3: true},
		legacy:        map[int]bool{1: false, 2: false, 3: false},
		readyAfterRun: true,
		etcdProbes:    make(map[int]int),
	}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, member HAMember) bool {
		return runner.states[member.ID] == "running" && runner.ready[member.ID]
	}
	return manager, runner, config
}

func (runner *haMemberLifecycleRunner) Run(_ context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	runner.calls = append(runner.calls, append([]string(nil), arguments...))
	switch {
	case len(arguments) == 3 && arguments[0] == "network" && arguments[1] == "inspect":
		var record haNetworkInspect
		record.Configuration.Name = runner.config.NetworkName
		record.Configuration.IPv4Subnet = runner.config.Subnet
		record.Configuration.Labels = map[string]string{"apc.dev/managed": "true", "apc.dev/cluster": runner.config.Name}
		return runner.marshal(record), nil, nil
	case len(arguments) == 3 && arguments[0] == "volume" && arguments[1] == "inspect":
		member := memberForHAVolume(runner.t, runner.config, arguments[2])
		var record haVolumeInspect
		record.Configuration.Name = arguments[2]
		record.Configuration.Labels = map[string]string{
			"apc.dev/managed": "true", "apc.dev/cluster": runner.config.Name,
			"apc.dev/role": "server", "apc.dev/member": strconv.Itoa(member.ID),
		}
		record.Configuration.Options = map[string]string{"size": runner.config.VolumeSize}
		return runner.marshal(record), nil, nil
	case len(arguments) == 2 && arguments[0] == "inspect":
		if runner.failInspectAfterStop && len(runner.mutations) > 0 && strings.HasPrefix(runner.mutations[len(runner.mutations)-1], "stop:") && !runner.postStopInspectFailed {
			runner.postStopInspectFailed = true
			return nil, []byte("injected post-stop inspect failure"), errors.New("exit 1")
		}
		member := memberForHAContainer(runner.t, runner.config, arguments[1])
		if runner.states[member.ID] == "missing" {
			return nil, []byte("not found"), errors.New("exit 1")
		}
		record := configuredHAContainer(runner.config, member, runner.states[member.ID])
		if runner.legacy[member.ID] {
			record.Configuration.InitProcess.Arguments = legacyHAInitArguments(runner.config, member)
		}
		if runner.foreignMember == member.ID {
			record.Configuration.Labels["apc.dev/cluster"] = "foreign"
		}
		return runner.marshal(record), nil, nil
	case len(arguments) == 5 && arguments[0] == "exec" && arguments[2] == "/bin/sh" && arguments[3] == "-c" && arguments[4] == haEtcdLocalProbeScript:
		member := memberForHAContainer(runner.t, runner.config, arguments[1])
		runner.etcdProbes[member.ID]++
		if runner.etcdErrorMember == member.ID {
			return nil, []byte("injected etcd probe failure"), errors.New("exit 1")
		}
		if runner.etcdOutput != nil {
			return runner.etcdOutput(member.ID), nil, nil
		}
		return fakeHAEtcdProbeOutput(member.ID), nil, nil
	case len(arguments) >= 8 && arguments[0] == "exec" && arguments[2] == "kubectl" && arguments[3] == "get" && arguments[4] == "node":
		if runner.expireOnProbeAfterRun != nil && len(runner.mutations) > 0 && strings.HasPrefix(runner.mutations[len(runner.mutations)-1], "run:") {
			expire := runner.expireOnProbeAfterRun
			runner.expireOnProbeAfterRun = nil
			expire()
		}
		member := memberForHAContainer(runner.t, runner.config, arguments[1])
		condition := "False"
		if runner.ready[member.ID] {
			condition = "True"
		}
		return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":%q}],"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"}}}`, member.NodeName, condition)), nil, nil
	case len(arguments) == 2 && arguments[0] == "stop":
		member := memberForHAContainer(runner.t, runner.config, arguments[1])
		runner.mutations = append(runner.mutations, "stop:"+arguments[1])
		runner.states[member.ID] = "stopped"
		runner.ready[member.ID] = false
		return nil, nil, nil
	case len(arguments) == 2 && arguments[0] == "delete":
		member := memberForHAContainer(runner.t, runner.config, arguments[1])
		runner.mutations = append(runner.mutations, "delete:"+arguments[1])
		runner.states[member.ID] = "missing"
		runner.ready[member.ID] = false
		return nil, nil, nil
	case len(arguments) > 0 && arguments[0] == "run":
		name := argumentAfter(arguments, "--name")
		member := memberForHAContainer(runner.t, runner.config, name)
		runner.mutations = append(runner.mutations, "run:"+name)
		runner.states[member.ID] = "running"
		runner.ready[member.ID] = runner.readyAfterRun
		runner.legacy[member.ID] = false
		return nil, nil, nil
	default:
		runner.t.Fatalf("unexpected command: %#v", arguments)
		return nil, nil, errors.New("unexpected command")
	}
}

func (runner *haMemberLifecycleRunner) marshal(record any) []byte {
	runner.t.Helper()
	data, err := json.Marshal([]any{record})
	if err != nil {
		runner.t.Fatal(err)
	}
	return data
}

func argumentAfter(arguments []string, flag string) string {
	for index := range arguments {
		if arguments[index] == flag && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	return ""
}

func TestStopHAMemberPreservesHealthyQuorum(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	state, err := manager.StopHAMember(context.Background(), config.Name, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wantMutations := []string{"stop:" + HAContainerName(config.Name, 2)}
	if !reflect.DeepEqual(runner.mutations, wantMutations) {
		t.Fatalf("mutations = %#v, want %#v", runner.mutations, wantMutations)
	}
	member, _ := haMemberStateByID(state, 2)
	if state.ReadyMembers != 2 || !state.Healthy || member.RuntimeState != "stopped" {
		t.Fatalf("unexpected stopped state: %+v", state)
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.StoppedMembers) != 1 || desired.StoppedMembers[0] != 2 {
		t.Fatalf("stopped member intent = %+v", desired)
	}
}

func TestStopHAMemberRefusesSecondOutageBeforeMutation(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.states[1] = "stopped"
	runner.ready[1] = false
	state, err := manager.StopHAMember(context.Background(), config.Name, 2, time.Second)
	if err == nil || !strings.Contains(err.Error(), "currently 2/3") {
		t.Fatalf("state = %+v, error = %v", state, err)
	}
	if len(runner.mutations) != 0 {
		t.Fatalf("mutation happened without three Ready members: %#v", runner.mutations)
	}
	desired, loadErr := loadHADesiredState(config.Name)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if !haMemberIntentionallyStopped(desired, 2) {
		t.Fatalf("operator stop intent was lost after failed reconciliation: %+v", desired)
	}
}

func TestStartHAMemberReplacesOnlyTargetEnvelope(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     string
		mutations []string
	}{
		{name: "stopped", state: "stopped", mutations: []string{"delete:", "run:"}},
		{name: "missing", state: "missing", mutations: []string{"run:"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, runner, config := newHAMemberLifecycleFixture(t)
			if err := setHAMemberIntentLocked(config.Name, 2, true); err != nil {
				t.Fatal(err)
			}
			target := HAContainerName(config.Name, 2)
			runner.states[2] = test.state
			runner.ready[2] = false
			state, err := manager.StartHAMember(context.Background(), config.Name, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]string, len(test.mutations))
			for index, operation := range test.mutations {
				want[index] = operation + target
			}
			if !reflect.DeepEqual(runner.mutations, want) {
				t.Fatalf("mutations = %#v, want %#v", runner.mutations, want)
			}
			if state.ReadyMembers != 3 || !state.Healthy {
				t.Fatalf("unexpected recovered state: %+v", state)
			}
			desired, err := loadHADesiredState(config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if len(desired.StoppedMembers) != 0 {
				t.Fatalf("start left stale member suppression: %+v", desired)
			}
		})
	}
}

func TestRestartHAMemberMutatesOnlyTargetAndReturnsThreeReady(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	if err := setHAMemberIntentLocked(config.Name, 2, true); err != nil {
		t.Fatal(err)
	}
	target := HAContainerName(config.Name, 2)
	state, err := manager.RestartHAMember(context.Background(), config.Name, 2, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"stop:" + target, "delete:" + target, "run:" + target}
	if !reflect.DeepEqual(runner.mutations, want) {
		t.Fatalf("mutations = %#v, want %#v", runner.mutations, want)
	}
	if state.ReadyMembers != 3 || !state.Healthy {
		t.Fatalf("unexpected restarted state: %+v", state)
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(desired.StoppedMembers) != 0 {
		t.Fatalf("restart left stale member suppression: %+v", desired)
	}
}

func TestReconcileHAMemberRollingMigratesExactLegacyEnvelope(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	member := config.Members[1]
	target := HAContainerName(config.Name, member.ID)
	runner.legacy[member.ID] = true
	record := configuredHAContainer(config, member, "running")
	record.Configuration.InitProcess.Arguments = legacyHAInitArguments(config, member)

	if err := manager.reconcileHAMember(context.Background(), config, member, record); err != nil {
		t.Fatal(err)
	}
	want := []string{"stop:" + target, "delete:" + target, "run:" + target}
	if !reflect.DeepEqual(runner.mutations, want) {
		t.Fatalf("legacy migration mutations = %#v, want %#v", runner.mutations, want)
	}
	if runner.legacy[member.ID] || runner.states[member.ID] != "running" || !runner.ready[member.ID] {
		t.Fatalf("legacy member was not replaced by a Ready guarded envelope: legacy=%v state=%q ready=%v", runner.legacy[member.ID], runner.states[member.ID], runner.ready[member.ID])
	}
}

func TestRestartHAMemberBestEffortRecoversAfterPostStopFailure(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.failInspectAfterStop = true
	target := HAContainerName(config.Name, 2)
	state, err := manager.RestartHAMember(context.Background(), config.Name, 2, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "injected post-stop inspect failure") {
		t.Fatalf("restart post-stop failure = %v", err)
	}
	want := []string{"stop:" + target, "delete:" + target, "run:" + target}
	if !reflect.DeepEqual(runner.mutations, want) {
		t.Fatalf("best-effort recovery mutations = %#v, want %#v", runner.mutations, want)
	}
	if runner.states[2] != "running" || !runner.ready[2] || state.ReadyMembers != 3 || !state.Healthy {
		t.Fatalf("best-effort restart did not restore three Ready members: state=%+v runtime=%q ready=%v", state, runner.states[2], runner.ready[2])
	}
}

func TestHAMemberLifecycleRejectsInvalidMemberWithoutRuntimeCalls(t *testing.T) {
	setHAConfigHome(t)
	runner := &haMemberLifecycleRunner{t: t}
	manager := NewManager("container")
	manager.runner = runner
	if _, err := manager.StopHAMember(context.Background(), "ha-lab", 4, time.Second); err == nil || !strings.Contains(err.Error(), "1, 2, or 3") {
		t.Fatalf("invalid member error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("invalid member caused runtime calls: %#v", runner.calls)
	}
}

func TestHAMemberLifecycleRejectsForeignTargetBeforeMutation(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.foreignMember = 2
	state, err := manager.StopHAMember(context.Background(), config.Name, 2, time.Second)
	if err == nil || !strings.Contains(err.Error(), "not the expected APC") {
		t.Fatalf("state = %+v, error = %v", state, err)
	}
	if len(runner.mutations) != 0 {
		t.Fatalf("foreign target was mutated: %#v", runner.mutations)
	}
}

func TestStartHAMemberReturnsBoundedReadinessTimeout(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	target := HAContainerName(config.Name, 2)
	runner.states[2] = "stopped"
	runner.ready[2] = false
	runner.readyAfterRun = false
	ctx := newManualDeadlineContext()
	runner.expireOnProbeAfterRun = ctx.expire
	state, err := manager.StartHAMember(ctx, config.Name, 2, time.Second)
	if err == nil || !strings.Contains(err.Error(), "reached only 2 of 3") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("state = %+v, error = %v", state, err)
	}
	want := []string{"delete:" + target, "run:" + target}
	if !reflect.DeepEqual(runner.mutations, want) {
		t.Fatalf("mutations = %#v, want %#v", runner.mutations, want)
	}
}

func TestHAMemberLifecycleIdempotenceDoesNotMutate(t *testing.T) {
	t.Run("start ready member", func(t *testing.T) {
		manager, runner, config := newHAMemberLifecycleFixture(t)
		state, err := manager.StartHAMember(context.Background(), config.Name, 2, time.Second)
		if err != nil || state.ReadyMembers != 3 {
			t.Fatalf("state = %+v, error = %v", state, err)
		}
		if len(runner.mutations) != 0 {
			t.Fatalf("ready member was mutated: %#v", runner.mutations)
		}
	})
	t.Run("stop offline member", func(t *testing.T) {
		manager, runner, config := newHAMemberLifecycleFixture(t)
		runner.states[2] = "stopped"
		runner.ready[2] = false
		state, err := manager.StopHAMember(context.Background(), config.Name, 2, time.Second)
		if err != nil || state.ReadyMembers != 2 {
			t.Fatalf("state = %+v, error = %v", state, err)
		}
		if len(runner.mutations) != 0 {
			t.Fatalf("offline member was mutated: %#v", runner.mutations)
		}
	})
}

func TestHAMemberOperationsDoNotUseConfigDeletedWhileWaitingForLock(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(context.Context, *Manager, string) error
	}{
		{
			name: "stop",
			invoke: func(ctx context.Context, manager *Manager, name string) error {
				_, err := manager.StopHAMember(ctx, name, 2, 2*time.Second)
				return err
			},
		},
		{
			name: "start",
			invoke: func(ctx context.Context, manager *Manager, name string) error {
				_, err := manager.StartHAMember(ctx, name, 2, 2*time.Second)
				return err
			},
		},
		{
			name: "restart",
			invoke: func(ctx context.Context, manager *Manager, name string) error {
				_, err := manager.RestartHAMember(ctx, name, 2, 2*time.Second)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			config := liveHAConfig(t)
			if err := saveHAConfig(config); err != nil {
				t.Fatal(err)
			}
			held, err := acquireHAOperationLock(context.Background(), config.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer held.release() //nolint:errcheck -- explicit release below is asserted
			runner := &haTestRunner{handler: func([]string) ([]byte, []byte, error) {
				return nil, []byte("unexpected runtime access"), errors.New("unexpected runtime access")
			}}
			manager := NewManager("container")
			manager.runner = runner
			done := make(chan error, 1)
			go func() { done <- test.invoke(context.Background(), manager, config.Name) }()

			time.Sleep(3 * haOperationLockPoll)
			configPath, err := HAConfigPath(config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(configPath); err != nil {
				t.Fatal(err)
			}
			if err := held.release(); err != nil {
				t.Fatal(err)
			}
			err = <-done
			if err == nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("operation after config deletion error = %v", err)
			}
			if len(runner.calls) != 0 {
				t.Fatalf("deleted cluster reached runtime: %#v", runner.calls)
			}
			desiredPath, err := haDesiredStatePath(config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(desiredPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("deleted cluster gained orphan desired state: %v", err)
			}
		})
	}
}

func TestStopHAMemberReloadsChangedConfigAfterBlockedLock(t *testing.T) {
	manager, runner, oldConfig := newHAMemberLifecycleFixture(t)
	newConfig := oldConfig
	newConfig.Members = append([]HAMember(nil), oldConfig.Members...)
	newConfig.Members[1].NodeName = "replacement-member-2"
	runner.config = newConfig
	held, err := acquireHAOperationLock(context.Background(), oldConfig.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer held.release() //nolint:errcheck -- explicit release below is asserted
	type operationResult struct {
		state HAState
		err   error
	}
	done := make(chan operationResult, 1)
	go func() {
		state, err := manager.StopHAMember(context.Background(), oldConfig.Name, 2, 2*time.Second)
		done <- operationResult{state: state, err: err}
	}()

	time.Sleep(3 * haOperationLockPoll)
	if err := saveHAConfig(newConfig); err != nil {
		t.Fatal(err)
	}
	if err := held.release(); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	member, found := haMemberStateByID(result.state, 2)
	if !found || member.NodeName != newConfig.Members[1].NodeName {
		t.Fatalf("operation used stale member identity: %+v", result.state)
	}
	desired, err := loadHADesiredState(newConfig.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !haMemberIntentionallyStopped(desired, 2) {
		t.Fatalf("changed config stop intent = %+v", desired)
	}
}

func TestHAMemberExplicitTimeoutIncludesTimeWaitingForLock(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	runner.states[2] = "stopped"
	runner.ready[2] = false
	runner.readyAfterRun = false
	held, err := acquireHAOperationLock(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer held.release() //nolint:errcheck -- explicit release below is asserted
	type operationResult struct {
		state HAState
		err   error
	}
	startedAt := time.Now()
	done := make(chan operationResult, 1)
	go func() {
		state, err := manager.StartHAMember(context.Background(), config.Name, 2, time.Second)
		done <- operationResult{state: state, err: err}
	}()
	time.Sleep(650 * time.Millisecond)
	if err := held.release(); err != nil {
		t.Fatal(err)
	}
	result := <-done
	elapsed := time.Since(startedAt)
	if result.err == nil || !strings.Contains(result.err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("bounded member start error = %v", result.err)
	}
	if elapsed < 850*time.Millisecond || elapsed > 1400*time.Millisecond {
		t.Fatalf("explicit 1s timeout completed in %s; lock wait was not charged to one total deadline", elapsed)
	}
	target := HAContainerName(config.Name, 2)
	want := []string{"delete:" + target, "run:" + target}
	if !reflect.DeepEqual(runner.mutations, want) {
		t.Fatalf("bounded start mutations = %#v, want %#v", runner.mutations, want)
	}
}

func TestHAMemberDefaultTimeoutExhaustedByLockWaitDoesNotMutate(t *testing.T) {
	manager, runner, config := newHAMemberLifecycleFixture(t)
	config.StartupTimeout = time.Second
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	held, err := acquireHAOperationLock(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer held.release() //nolint:errcheck -- explicit release below is asserted
	done := make(chan error, 1)
	go func() {
		_, err := manager.StopHAMember(context.Background(), config.Name, 2, 0)
		done <- err
	}()
	time.Sleep(1100 * time.Millisecond)
	if err := held.release(); err != nil {
		t.Fatal(err)
	}
	err = <-done
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exhausted saved timeout error = %v", err)
	}
	if len(runner.calls) != 0 || len(runner.mutations) != 0 {
		t.Fatalf("expired operation reached runtime: calls=%#v mutations=%#v", runner.calls, runner.mutations)
	}
	desiredPath, err := haDesiredStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(desiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired operation created desired state: %v", err)
	}
}
