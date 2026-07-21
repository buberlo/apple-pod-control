package cluster

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestImageImportTargetsReturnsAllValidatedHAMembersInOrder(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	config.Members[0], config.Members[2] = config.Members[2], config.Members[0]
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := normalizeHAConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager("container")
	manager.runner = newHAOwnedResourceRunner(t, runtimeConfig)

	targets, err := manager.ImageImportTargets(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		HAContainerName(config.Name, 1),
		HAContainerName(config.Name, 2),
		HAContainerName(config.Name, 3),
	}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets = %#v, want %#v", targets, want)
	}
}

func TestImageImportTargetsRejectsStoppedHAMember(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := newHAOwnedResourceRunner(t, config)
	ownedHandler := runner.handler
	runner.handler = func(arguments []string) ([]byte, []byte, error) {
		if len(arguments) == 2 && arguments[0] == "inspect" && arguments[1] == HAContainerName(config.Name, 2) {
			return marshalHAInspect(t, configuredHAContainer(config, config.Members[1], "stopped")), nil, nil
		}
		return ownedHandler(arguments)
	}
	manager := NewManager("container")
	manager.runner = runner

	targets, err := manager.ImageImportTargets(context.Background(), config.Name)
	if err == nil || !strings.Contains(err.Error(), "member 2") || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("targets = %#v, error = %v", targets, err)
	}
	if len(targets) != 0 {
		t.Fatalf("partial targets returned: %#v", targets)
	}
}

func TestImageImportTargetsRejectsForeignHAEnvelope(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := newHAOwnedResourceRunner(t, config)
	ownedHandler := runner.handler
	runner.handler = func(arguments []string) ([]byte, []byte, error) {
		if len(arguments) == 2 && arguments[0] == "inspect" && arguments[1] == HAContainerName(config.Name, 2) {
			record := configuredHAContainer(config, config.Members[1], "running")
			record.Configuration.Labels["apc.dev/cluster"] = "foreign"
			return marshalHAInspect(t, record), nil, nil
		}
		return ownedHandler(arguments)
	}
	manager := NewManager("container")
	manager.runner = runner

	targets, err := manager.ImageImportTargets(context.Background(), config.Name)
	if err == nil || !strings.Contains(err.Error(), HAContainerName(config.Name, 2)) || !strings.Contains(err.Error(), "not the expected APC") {
		t.Fatalf("targets = %#v, error = %v", targets, err)
	}
	if len(targets) != 0 {
		t.Fatalf("partial targets returned: %#v", targets)
	}
}

func TestImageImportTargetsRequireEveryHAEnvelope(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := newHAOwnedResourceRunner(t, config)
	ownedHandler := runner.handler
	runner.handler = func(arguments []string) ([]byte, []byte, error) {
		if len(arguments) == 2 && arguments[0] == "inspect" && arguments[1] == HAContainerName(config.Name, 3) {
			return nil, []byte("not found"), errors.New("exit 1")
		}
		return ownedHandler(arguments)
	}
	manager := NewManager("container")
	manager.runner = runner

	targets, err := manager.ImageImportTargets(context.Background(), config.Name)
	if err == nil || !strings.Contains(err.Error(), "all 3") || !strings.Contains(err.Error(), "found 2") {
		t.Fatalf("targets = %#v, error = %v", targets, err)
	}
	if len(targets) != 0 {
		t.Fatalf("partial targets returned: %#v", targets)
	}
}

func TestImageImportTargetsRetainsLegacySingleServerResolution(t *testing.T) {
	setHAConfigHome(t)
	manager := NewManager("container")
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		t.Fatalf("legacy target resolution executed runtime command: %#v", arguments)
		return nil, nil, errors.New("unexpected command")
	}}
	manager.runner = runner

	targets, err := manager.ImageImportTargets(context.Background(), "home")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{ContainerName("home")}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets = %#v, want %#v", targets, want)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("legacy resolution made runtime calls: %#v", runner.calls)
	}
}
