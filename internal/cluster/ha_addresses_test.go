package cluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunHAServerEnvelopeRetriesReservedRuntimeIPv4AndRemovesOnlyExactEnvelope(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	member := config.Members[0]
	runner := &haAddressRetryRunner{
		t:           t,
		config:      config,
		member:      member,
		allocations: []string{config.Members[2].StableIP, "192.168.96.50"},
	}
	manager := NewManager("container")
	manager.runner = runner

	if err := manager.runHAServerEnvelope(context.Background(), config, member, "start test HA server", time.Second); err != nil {
		t.Fatal(err)
	}
	if runner.runCount != 2 || runner.stopCount != 1 || runner.deleteCount != 1 || !runner.exists || runner.address != "192.168.96.50" {
		t.Fatalf("runs=%d stops=%d deletes=%d exists=%v address=%q calls=%#v", runner.runCount, runner.stopCount, runner.deleteCount, runner.exists, runner.address, runner.calls)
	}
	if !hasOrderedHAAddressCalls(runner.calls,
		[]string{"run"},
		[]string{"inspect", HAContainerName(config.Name, member.ID)},
		[]string{"stop", HAContainerName(config.Name, member.ID)},
		[]string{"inspect", HAContainerName(config.Name, member.ID)},
		[]string{"delete", HAContainerName(config.Name, member.ID)},
		[]string{"run"},
	) {
		t.Fatalf("collision retry order was not deterministic: %#v", runner.calls)
	}
}

func TestRunHAServerEnvelopeExhaustsBoundedRetriesAndLeavesNoConflictingEnvelope(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	member := config.Members[0]
	runner := &haAddressRetryRunner{
		t:      t,
		config: config,
		member: member,
		allocations: []string{
			config.Members[1].StableIP,
			config.Members[2].StableIP,
			config.Members[1].StableIP,
		},
	}
	manager := NewManager("container")
	manager.runner = runner

	err := manager.runHAServerEnvelope(context.Background(), config, member, "start test HA server", time.Second)
	if err == nil || !strings.Contains(err.Error(), "after 3 attempts") || !strings.Contains(err.Error(), "does not support fixed IPv4") {
		t.Fatalf("bounded retry error = %v", err)
	}
	if runner.runCount != haRuntimeAddressRetryLimit || runner.stopCount != haRuntimeAddressRetryLimit || runner.deleteCount != haRuntimeAddressRetryLimit || runner.exists {
		t.Fatalf("runs=%d stops=%d deletes=%d exists=%v calls=%#v", runner.runCount, runner.stopCount, runner.deleteCount, runner.exists, runner.calls)
	}
}

func TestValidateHACurrentRuntimeIPReservationsReportsBothOwnersAndRecovery(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	records := map[int]haContainerInspect{}
	for _, member := range config.Members {
		records[member.ID] = configuredHAContainer(config, member, "running")
	}
	record := records[1]
	record.Status.Networks[0].IPv4Address = config.Members[2].StableIP + "/24"
	records[1] = record

	err := validateHACurrentRuntimeIPReservations(config, records)
	for _, expected := range []string{"member 1", config.Members[2].StableIP, "member 3", "validated HA restore"} {
		if err == nil || !strings.Contains(err.Error(), expected) {
			t.Fatalf("reservation error %q does not contain %q", err, expected)
		}
	}
}

func TestRunHAServerEnvelopeRunPhaseIsContextBounded(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	manager := NewManager("container")
	manager.runner = haRunPhaseBlockingRunner{}

	started := time.Now()
	err := manager.runHAServerEnvelope(context.Background(), config, config.Members[0], "start bounded HA server", 20*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("bounded run error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded run took %s", elapsed)
	}
}

func TestRunHAServerEnvelopeRefusesExistingPeerPrimaryOverlapBeforeRun(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	target := config.Members[2]
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		if len(arguments) != 2 || arguments[0] != "inspect" {
			t.Fatalf("runtime mutated after peer collision preflight: %#v", arguments)
		}
		member := memberForHAContainer(t, config, arguments[1])
		if member.ID == target.ID {
			return nil, []byte("container not found"), errors.New("exit 1")
		}
		record := configuredHAContainer(config, member, "running")
		if member.ID == 1 {
			record.Status.Networks[0].IPv4Address = target.StableIP + "/24"
		}
		return marshalHAInspect(t, record), nil, nil
	}}
	manager := NewManager("container")
	manager.runner = runner

	err := manager.runHAServerEnvelope(context.Background(), config, target, "start guarded HA server", time.Second)
	if err == nil || !strings.Contains(err.Error(), "before creating member 3") || !strings.Contains(err.Error(), "member 1") {
		t.Fatalf("peer collision preflight error = %v", err)
	}
	for _, call := range runner.calls {
		if len(call) > 0 && call[0] == "run" {
			t.Fatalf("peer collision preflight launched target: %#v", runner.calls)
		}
	}
}

type haAddressRetryRunner struct {
	t           *testing.T
	config      HAConfig
	member      HAMember
	allocations []string
	calls       [][]string
	runCount    int
	stopCount   int
	deleteCount int
	exists      bool
	state       string
	address     string
}

func (runner *haAddressRetryRunner) Run(_ context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	call := append([]string(nil), arguments...)
	runner.calls = append(runner.calls, call)
	name := HAContainerName(runner.config.Name, runner.member.ID)
	switch {
	case len(call) > 0 && call[0] == "run":
		if runner.exists {
			runner.t.Fatalf("run attempted while envelope exists: %#v", runner.calls)
		}
		if runner.runCount >= len(runner.allocations) {
			runner.t.Fatalf("unexpected run %d: %#v", runner.runCount+1, runner.calls)
		}
		runner.address = runner.allocations[runner.runCount]
		runner.runCount++
		runner.exists = true
		runner.state = "running"
		return nil, nil, nil
	case reflect.DeepEqual(call, []string{"inspect", name}):
		if !runner.exists {
			return nil, []byte("container not found"), errors.New("exit 1")
		}
		record := configuredHAContainer(runner.config, runner.member, runner.state)
		record.Status.Networks[0].IPv4Address = runner.address + "/24"
		return marshalHAInspect(runner.t, record), nil, nil
	case len(call) == 2 && call[0] == "inspect":
		return nil, []byte("container not found"), errors.New("exit 1")
	case reflect.DeepEqual(call, []string{"stop", name}):
		if !runner.exists || runner.state != "running" {
			runner.t.Fatalf("invalid stop: exists=%v state=%q", runner.exists, runner.state)
		}
		runner.stopCount++
		runner.state = "stopped"
		return nil, nil, nil
	case reflect.DeepEqual(call, []string{"delete", name}):
		if !runner.exists || runner.state != "stopped" {
			runner.t.Fatalf("invalid delete: exists=%v state=%q", runner.exists, runner.state)
		}
		runner.deleteCount++
		runner.exists = false
		runner.state = ""
		return nil, nil, nil
	default:
		runner.t.Fatalf("unexpected HA address retry command: %#v", call)
		return nil, []byte("unexpected command"), fmt.Errorf("unexpected command")
	}
}

type haRunPhaseBlockingRunner struct{}

func (haRunPhaseBlockingRunner) Run(ctx context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	if len(arguments) == 2 && arguments[0] == "inspect" {
		return nil, []byte("container not found"), errors.New("exit 1")
	}
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func hasOrderedHAAddressCalls(calls [][]string, prefixes ...[]string) bool {
	position := -1
	for _, prefix := range prefixes {
		found := -1
		for index := position + 1; index < len(calls); index++ {
			if len(calls[index]) >= len(prefix) && reflect.DeepEqual(calls[index][:len(prefix)], prefix) {
				found = index
				break
			}
		}
		if found < 0 {
			return false
		}
		position = found
	}
	return true
}
