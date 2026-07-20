package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestDefaultHAConfigMatchesValidationLabIdentity(t *testing.T) {
	setHAConfigHome(t)
	config, err := DefaultHAConfig("ha-lab")
	if err != nil {
		t.Fatal(err)
	}
	if config.NetworkName != "apc-ha-lab" || config.Subnet != "192.168.96.0/24" {
		t.Fatalf("unexpected network defaults: %+v", config)
	}
	if filepath.Base(config.TokenFile) != "server-token" || filepath.Base(config.KubeconfigPath) != "kubeconfig" {
		t.Fatalf("unexpected protected paths: token=%q kubeconfig=%q", config.TokenFile, config.KubeconfigPath)
	}
	wantNodes := []string{"apc-ha-1", "apc-ha-2", "apc-ha-3"}
	wantIPs := []string{"192.168.96.241", "192.168.96.242", "192.168.96.243"}
	for index, member := range config.Members {
		if member.ID != index+1 || member.NodeName != wantNodes[index] || member.StableIP != wantIPs[index] || member.HostAPIPort != 17443+index {
			t.Fatalf("member %d = %+v", index, member)
		}
	}
}

func TestHAServerRunArgumentsUseNativeARM64StableSourceRouteAndProtectedTokenFile(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	seedArguments := HAServerRunArguments(config, config.Members[0])
	joinedArguments := HAServerRunArguments(config, config.Members[1])

	for _, required := range []string{
		"--arch", "arm64", DefaultK3sImage,
		"apc-ha-lab,mac=02:ac:96:00:00:01,mtu=1280",
		"127.0.0.1:17443:6443/tcp",
		"apc.dev/member=1",
		"--cluster-init", "--token-file", haTokenMountPath,
		"ip address add 192.168.96.11/24 dev eth0 2>/dev/null || true; ip route replace 192.168.96.0/24 dev eth0 src 192.168.96.11; exec /bin/k3s \"$@\"",
	} {
		if !contains(seedArguments, required) {
			t.Fatalf("seed arguments do not contain %q: %#v", required, seedArguments)
		}
	}
	if contains(joinedArguments, "--cluster-init") || !contains(joinedArguments, "https://192.168.96.11:6443") {
		t.Fatalf("joiner arguments are not seed-directed: %#v", joinedArguments)
	}
	if strings.Contains(strings.Join(seedArguments, " "), "test-secret-value") {
		t.Fatal("server token value leaked into process arguments")
	}
}

func TestNormalizeHAConfigRejectsDuplicateMemberIdentity(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	config.Members[2].HostAPIPort = config.Members[1].HostAPIPort
	if _, err := normalizeHAConfig(config); err == nil || !strings.Contains(err.Error(), "ports must be unique") {
		t.Fatalf("duplicate port error = %v", err)
	}
	config = liveHAConfig(t)
	config.Members[2].StableIP = "192.168.96.1"
	if _, err := normalizeHAConfig(config); err == nil || !strings.Contains(err.Error(), "usable address") {
		t.Fatalf("gateway address error = %v", err)
	}
}

func TestNormalizeHAConfigRejectsProtectedPathCollisions(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	config.KubeconfigPath = config.TokenFile
	if _, err := normalizeHAConfig(config); err == nil || !strings.Contains(err.Error(), "pairwise distinct") {
		t.Fatalf("token/kubeconfig collision error = %v", err)
	}
	config = liveHAConfig(t)
	config.KubeconfigPath, _ = HAConfigPath(config.Name)
	if _, err := normalizeHAConfig(config); err == nil || !strings.Contains(err.Error(), "pairwise distinct") {
		t.Fatalf("config/kubeconfig collision error = %v", err)
	}
}

func TestHARejectsLegacyClusterNameCollision(t *testing.T) {
	setHAConfigHome(t)
	legacy, err := normalizeConfig(Config{Name: "ha-lab"})
	if err != nil {
		t.Fatal(err)
	}
	if err := saveClusterConfig(legacy); err != nil {
		t.Fatal(err)
	}
	if err := checkHALegacyCollision("ha-lab"); err == nil || !strings.Contains(err.Error(), "legacy APC state") {
		t.Fatalf("collision error = %v", err)
	}
}

func TestResolvedKubeconfigPathUsesSavedHAConfig(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	config.KubeconfigPath = filepath.Join(filepath.Dir(config.KubeconfigPath), "custom-ha-kubeconfig")
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateFile(config.KubeconfigPath, []byte("apiVersion: v1\n")); err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolvedKubeconfigPath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != config.KubeconfigPath {
		t.Fatalf("resolved = %q, want %q", resolved, config.KubeconfigPath)
	}
}

func TestStatusHAReportsAllReadyMembersWithoutExposingToken(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) > 0 && arguments[0] == "--kubeconfig":
			return []byte("ok\n"), nil, nil
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) >= 7 && arguments[0] == "exec":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}],"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"}}}`, member.NodeName)), nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }
	state, err := manager.StatusHA(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadyMembers != 3 || state.Quorum != 2 || !state.Healthy || len(state.Members) != 3 {
		t.Fatalf("unexpected state: %+v", state)
	}
	serialized, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(serialized), "tokenFile") || strings.Contains(string(serialized), "test-secret-value") {
		t.Fatalf("status contains token metadata: %s", serialized)
	}
}

func TestStatusHARequiresPublishedHostAPI(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) > 0 && arguments[0] == "--kubeconfig":
			return []byte("ok\n"), nil, nil
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) >= 7 && arguments[0] == "exec":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`, member.NodeName)), nil, nil
		default:
			return nil, []byte("unexpected"), errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return false }
	state, err := manager.StatusHA(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadyMembers != 0 || state.Healthy {
		t.Fatalf("unpublished APIs were reported healthy: %+v", state)
	}
	for _, member := range state.Members {
		if member.APIReady || !member.NodeReady {
			t.Fatalf("unexpected split readiness for member: %+v", member)
		}
	}
}

func TestPrepareKubeconfigFailsOverToSecondReadyAPI(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) > 0 && arguments[0] == "--kubeconfig":
			return []byte("ok\n"), nil, nil
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			state := "running"
			if member.ID == 1 {
				state = "stopped"
			}
			return marshalHAInspect(t, configuredHAContainer(config, member, state)), nil, nil
		case len(arguments) >= 7 && arguments[0] == "exec" && arguments[2] == "kubectl":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`, member.NodeName)), nil, nil
		case len(arguments) == 4 && arguments[0] == "exec" && arguments[2] == "/bin/cat":
			return []byte("apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443\n  name: default\n"), nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, member HAMember) bool { return member.ID == 2 }
	path, err := manager.PrepareKubeconfig(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "server: https://127.0.0.1:17444") {
		t.Fatalf("kubeconfig did not select member 2: %s", data)
	}
}

func TestBundledKubectlUsesReadyHAMember(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			state := "running"
			if member.ID == 1 {
				state = "stopped"
			}
			return marshalHAInspect(t, configuredHAContainer(config, member, state)), nil, nil
		case len(arguments) >= 8 && arguments[0] == "exec" && arguments[3] == "get" && arguments[4] == "node":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`, member.NodeName)), nil, nil
		case reflect.DeepEqual(arguments, []string{"exec", HAContainerName(config.Name, 2), "kubectl", "get", "pods"}):
			return []byte("pods\n"), nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	stdout, _, err := manager.Kubectl(context.Background(), config.Name, "get", "pods")
	if err != nil {
		t.Fatal(err)
	}
	if string(stdout) != "pods\n" {
		t.Fatalf("stdout = %q", stdout)
	}
}

func TestStopHAValidatesAllOwnershipThenStopsInReverseOrder(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := newHAOwnedResourceRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	if err := manager.StopHA(context.Background(), config.Name); err != nil {
		t.Fatal(err)
	}
	var stopped []string
	for _, call := range runner.calls {
		if len(call) == 2 && call[0] == "stop" {
			stopped = append(stopped, call[1])
		}
	}
	want := []string{
		HAContainerName(config.Name, 3),
		HAContainerName(config.Name, 2),
		HAContainerName(config.Name, 1),
	}
	if !reflect.DeepEqual(stopped, want) {
		t.Fatalf("stopped = %#v, want %#v", stopped, want)
	}
}

func TestStopHAPreflightRejectsWrongMemberBeforeMutation(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := newHAOwnedResourceRunner(t, config)
	original := runner.handler
	runner.handler = func(arguments []string) ([]byte, []byte, error) {
		if len(arguments) == 2 && arguments[0] == "inspect" && arguments[1] == HAContainerName(config.Name, 2) {
			record := configuredHAContainer(config, config.Members[1], "running")
			record.Configuration.Labels["apc.dev/member"] = "9"
			return marshalHAInspect(t, record), nil, nil
		}
		return original(arguments)
	}
	manager := NewManager("container")
	manager.runner = runner
	err := manager.StopHA(context.Background(), config.Name)
	if err == nil || !strings.Contains(err.Error(), "member 2") {
		t.Fatalf("ownership error = %v", err)
	}
	for _, call := range runner.calls {
		if len(call) > 0 && call[0] == "stop" {
			t.Fatalf("mutation happened before complete preflight: %#v", runner.calls)
		}
	}
}

func TestDeleteHACleansUpPartialOwnedVolumeState(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 3 && arguments[0] == "network" && arguments[1] == "inspect":
			return nil, []byte("not found"), errors.New("exit 1")
		case len(arguments) == 3 && arguments[0] == "volume" && arguments[1] == "inspect" && arguments[2] == HAVolumeName(config.Name, 1):
			var record haVolumeInspect
			record.Configuration.Name = arguments[2]
			record.Configuration.Labels = map[string]string{
				"apc.dev/managed": "true", "apc.dev/cluster": config.Name,
				"apc.dev/role": "server", "apc.dev/member": "1",
			}
			record.Configuration.Options = map[string]string{"size": config.VolumeSize}
			return marshalHAInspect(t, record), nil, nil
		case len(arguments) == 3 && arguments[0] == "volume" && arguments[1] == "inspect":
			return nil, []byte("not found"), errors.New("exit 1")
		case len(arguments) == 2 && arguments[0] == "inspect":
			return nil, []byte("not found"), errors.New("exit 1")
		case len(arguments) == 3 && arguments[0] == "volume" && arguments[1] == "delete":
			return nil, nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	if err := manager.DeleteHA(context.Background(), config.Name, false); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) == 0 || !reflect.DeepEqual(runner.calls[len(runner.calls)-1], []string{"volume", "delete", HAVolumeName(config.Name, 1)}) {
		t.Fatalf("partial owned volume was not deleted: %#v", runner.calls)
	}
}

func TestEnsureHATokenCreatesPrivateRandomToken(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, haTokenFilename)
	if err := ensureHAToken(path, true); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || len(strings.TrimSpace(string(data))) != 64 {
		t.Fatalf("token mode=%o length=%d", info.Mode().Perm(), len(strings.TrimSpace(string(data))))
	}
}

type haTestRunner struct {
	calls   [][]string
	handler func([]string) ([]byte, []byte, error)
}

func (runner *haTestRunner) Run(_ context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	call := append([]string(nil), arguments...)
	runner.calls = append(runner.calls, call)
	return runner.handler(call)
}

func setHAConfigHome(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
}

func liveHAConfig(t *testing.T) HAConfig {
	t.Helper()
	config, err := DefaultHAConfig("ha-lab")
	if err != nil {
		t.Fatal(err)
	}
	for index := range config.Members {
		config.Members[index].StableIP = "192.168.96." + strconv.Itoa(11+index)
	}
	config, err = normalizeHAConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func configuredHAContainer(config HAConfig, member HAMember, state string) haContainerInspect {
	var record haContainerInspect
	record.Configuration.Labels = map[string]string{
		"apc.dev/managed": "true",
		"apc.dev/cluster": config.Name,
		"apc.dev/role":    "server",
		"apc.dev/member":  strconv.Itoa(member.ID),
	}
	record.Configuration.Image.Reference = config.Image
	record.Configuration.InitProcess.Arguments = haInitArguments(config, member)
	record.Configuration.Networks = make([]struct {
		Network string `json:"network"`
		Options struct {
			MACAddress string `json:"macAddress"`
			MTU        int    `json:"mtu"`
		} `json:"options"`
	}, 1)
	record.Configuration.Networks[0].Network = config.NetworkName
	record.Configuration.Networks[0].Options.MACAddress = member.MAC
	record.Configuration.Networks[0].Options.MTU = 1280
	record.Configuration.PublishedPorts = make([]struct {
		ContainerPort int    `json:"containerPort"`
		HostAddress   string `json:"hostAddress"`
		HostPort      int    `json:"hostPort"`
		Proto         string `json:"proto"`
	}, 1)
	record.Configuration.PublishedPorts[0].ContainerPort = 6443
	record.Configuration.PublishedPorts[0].HostAddress = config.ListenAddress
	record.Configuration.PublishedPorts[0].HostPort = member.HostAPIPort
	record.Configuration.PublishedPorts[0].Proto = "tcp"
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
	}, 2)
	record.Configuration.Mounts[0].Destination = "/var/lib/rancher/k3s"
	record.Configuration.Mounts[0].Type.Volume = &struct {
		Name string `json:"name"`
	}{Name: HAVolumeName(config.Name, member.ID)}
	record.Configuration.Mounts[1].Destination = "/run/secrets/apc"
	record.Configuration.Mounts[1].Source = filepath.Dir(config.TokenFile)
	record.Configuration.Mounts[1].Options = []string{"ro"}
	record.Configuration.Mounts[1].Type.VirtioFS = &struct{}{}
	memory, _ := parseHAByteSize(config.Memory)
	record.Configuration.Resources.CPUs = config.CPUs
	record.Configuration.Resources.MemoryInBytes = int64(memory)
	record.Status.State = state
	record.Status.Networks = make([]struct {
		IPv4Address string `json:"ipv4Address"`
	}, 1)
	record.Status.Networks[0].IPv4Address = member.StableIP + "/24"
	return record
}

func newHAOwnedResourceRunner(t *testing.T, config HAConfig) *haTestRunner {
	t.Helper()
	runner := &haTestRunner{}
	runner.handler = func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 3 && arguments[0] == "network" && arguments[1] == "inspect":
			var record haNetworkInspect
			record.Configuration.Name = config.NetworkName
			record.Configuration.IPv4Subnet = config.Subnet
			record.Configuration.Labels = map[string]string{"apc.dev/managed": "true", "apc.dev/cluster": config.Name}
			return marshalHAInspect(t, record), nil, nil
		case len(arguments) == 3 && arguments[0] == "volume" && arguments[1] == "inspect":
			member := memberForHAVolume(t, config, arguments[2])
			var record haVolumeInspect
			record.Configuration.Name = arguments[2]
			record.Configuration.Labels = map[string]string{
				"apc.dev/managed": "true", "apc.dev/cluster": config.Name,
				"apc.dev/role": "server", "apc.dev/member": strconv.Itoa(member.ID),
			}
			record.Configuration.Options = map[string]string{"size": config.VolumeSize}
			return marshalHAInspect(t, record), nil, nil
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) == 2 && arguments[0] == "stop":
			return nil, nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}
	return runner
}

func memberForHAContainer(t *testing.T, config HAConfig, name string) HAMember {
	t.Helper()
	for _, member := range config.Members {
		if HAContainerName(config.Name, member.ID) == name {
			return member
		}
	}
	t.Fatalf("unknown HA container %q", name)
	return HAMember{}
}

func memberForHAVolume(t *testing.T, config HAConfig, name string) HAMember {
	t.Helper()
	for _, member := range config.Members {
		if HAVolumeName(config.Name, member.ID) == name {
			return member
		}
	}
	t.Fatalf("unknown HA volume %q", name)
	return HAMember{}
}

func marshalHAInspect(t *testing.T, record any) []byte {
	t.Helper()
	data, err := json.Marshal([]any{record})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
