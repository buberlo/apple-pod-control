package cluster

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestServerRunArgumentsUsePinnedARM64ImageAndVXLAN(t *testing.T) {
	config := Config{Name: "home", NodeName: "macbook", ListenAddress: "0.0.0.0", AdvertiseAddress: "192.0.2.10", CPUs: 4, Memory: "4G"}
	arguments := ServerRunArguments(config)
	for _, required := range []string{
		"--arch", "arm64", DefaultK3sImage, "--flannel-backend", "vxlan",
		"0.0.0.0:16443:16443/tcp", "0.0.0.0:8472:8472/udp", "0.0.0.0:10250:10250/tcp",
		"--https-listen-port", "16443",
		"--node-external-ip", "192.0.2.10", "--flannel-external-ip",
		"default,mac=" + DeterministicMAC("home", "server") + ",mtu=1280",
		"--entrypoint", "/bin/sh", dynamicNodeIPScript,
		"--disable-network-policy",
		ServerVolumeName("home") + ":/var/lib/rancher/k3s",
	} {
		if !contains(arguments, required) {
			t.Fatalf("arguments do not contain %q: %#v", required, arguments)
		}
	}
	if contains(arguments, "wireguard-native") {
		t.Fatalf("wireguard must not be selected: %#v", arguments)
	}
}

func TestNormalizeConfigRejectsPublicListenerWithoutAdvertiseAddress(t *testing.T) {
	_, err := normalizeConfig(Config{Name: "home", ListenAddress: "0.0.0.0"})
	if err == nil || !strings.Contains(err.Error(), "advertise address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAgentRunArgumentsMountProtectedTokenWithoutPuttingItInArguments(t *testing.T) {
	config := AgentConfig{
		Name: "home", NodeName: "mac-mini", ServerURL: "https://192.0.2.10:16443",
		TokenFile: "/private/token", AdvertiseAddress: "192.0.2.20",
	}
	arguments := AgentRunArguments(config)
	for _, required := range []string{
		"0.0.0.0:8472:8472/udp", "0.0.0.0:10250:10250/tcp",
		"type=bind,source=/private,target=/run/secrets/apc,readonly",
		"--token-file", agentTokenMountPath, "--node-external-ip", "192.0.2.20",
		"default,mac=" + DeterministicMAC("home", "agent") + ",mtu=1280",
		"--entrypoint", "/bin/sh", agentNodeIPScript,
		AgentVolumeName("home") + ":/var/lib/rancher/k3s",
	} {
		if !contains(arguments, required) {
			t.Fatalf("arguments do not contain %q: %#v", required, arguments)
		}
	}
}

func TestAgentEntrypointPersistsK3sNodeIdentity(t *testing.T) {
	for _, required := range []string{"/var/lib/rancher/k3s/apc-node-identity", "ln -s", "/etc/rancher/node"} {
		if !strings.Contains(agentNodeIPScript, required) {
			t.Fatalf("agent entrypoint is missing %q: %s", required, agentNodeIPScript)
		}
	}
}

func TestDeterministicMACIsStableLocallyAdministeredAndUnicast(t *testing.T) {
	first := DeterministicMAC("home", "server")
	if first != DeterministicMAC("home", "server") {
		t.Fatal("MAC is not deterministic")
	}
	if first == DeterministicMAC("home", "agent") {
		t.Fatal("server and agent MACs must differ")
	}
	parsed, err := net.ParseMAC(first)
	if err != nil {
		t.Fatal(err)
	}
	if parsed[0]&0x01 != 0 || parsed[0]&0x02 == 0 {
		t.Fatalf("MAC %s is not locally administered unicast", first)
	}
}

func TestCurrentAgentBootLogsDropsPreviousSuccessfulBoot(t *testing.T) {
	logs := "Starting k3s agent\nRunning flannel backend.\nStarting k3s agent\nwaiting for server"
	current := currentAgentBootLogs(logs)
	if strings.Contains(current, "Running flannel backend.") || !strings.Contains(current, "waiting for server") {
		t.Fatalf("unexpected current boot logs: %q", current)
	}
}

func TestWaitReadyRequiresKubernetesInternalIPToMatchCurrentVM(t *testing.T) {
	inspect := func(address string) []byte {
		return []byte(`[{"configuration":{"labels":{}},"status":{"state":"running","networks":[{"ipv4Address":"` + address + `/24"}]}}]`)
	}
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: []byte("True;192.168.64.8")},
		{stdout: inspect("192.168.64.9")},
		{stdout: []byte("True;192.168.64.9")},
		{stdout: inspect("192.168.64.9")},
	}}
	manager := NewManager("container")
	manager.runner = runner

	if err := manager.waitReady(context.Background(), "node", "macbook", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("calls = %d, want 4; stale Kubernetes IP was accepted", len(runner.calls))
	}
}

func TestValidatePrivateTokenFileRejectsGroupReadableFile(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "token")
	if err := os.WriteFile(path, []byte("test-token\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateTokenFile(path); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateTokenFile(path); err != nil {
		t.Fatalf("mode 0600 rejected: %v", err)
	}
}

func TestReplaceKubeconfigServer(t *testing.T) {
	runner := &scriptedRunner{responses: []runnerResponse{{stdout: []byte(`apiVersion: v1
clusters:
  - cluster:
      server: https://127.0.0.1:6443
    name: default
`)}}}
	manager := NewManager("container")
	manager.runner = runner
	output, err := manager.readKubeconfig(context.Background(), "node", "https://127.0.0.1:16443")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "server: https://127.0.0.1:16443") {
		t.Fatalf("unexpected kubeconfig: %s", output)
	}
}

func TestKubectlOnlyUsesOwnedRunningContainer(t *testing.T) {
	inspect := `[{
  "configuration":{"labels":{"apc.dev/managed":"true","apc.dev/cluster":"home","apc.dev/role":"server"}},
  "status":{"state":"running","networks":[]}
}]`
	runner := &scriptedRunner{responses: []runnerResponse{{stdout: []byte(inspect)}, {stdout: []byte("ok")}}}
	manager := NewManager("container")
	manager.runner = runner
	stdout, _, err := manager.Kubectl(context.Background(), "home", "get", "nodes")
	if err != nil {
		t.Fatal(err)
	}
	if string(stdout) != "ok" {
		t.Fatalf("stdout = %q", stdout)
	}
	want := []string{"exec", "apc-k3s-home-server", "kubectl", "get", "nodes"}
	if !reflect.DeepEqual(runner.calls[1], want) {
		t.Fatalf("call = %#v, want %#v", runner.calls[1], want)
	}
}

func TestInspectMapsNotFound(t *testing.T) {
	manager := NewManager("container")
	manager.runner = &scriptedRunner{responses: []runnerResponse{{stderr: []byte("container not found"), err: errors.New("exit 1")}}}
	_, err := manager.Status(context.Background(), "home")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestStatusSelectsNodeMatchingServerVMAddress(t *testing.T) {
	inspect := `[{"configuration":{"labels":{"apc.dev/managed":"true","apc.dev/cluster":"home","apc.dev/role":"server"},"publishedPorts":[]},"status":{"state":"running","networks":[{"ipv4Address":"192.168.64.9/24"}]}}]`
	nodes := `{"items":[{"metadata":{"name":"agent-first"},"status":{"nodeInfo":{"kubeletVersion":"wrong"},"conditions":[{"type":"Ready","status":"False"}],"addresses":[{"type":"InternalIP","address":"192.168.64.8"}]}},{"metadata":{"name":"server-second"},"status":{"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"},"conditions":[{"type":"Ready","status":"True"}],"addresses":[{"type":"InternalIP","address":"192.168.64.9"}]}}]}`
	manager := NewManager("container")
	manager.runner = &scriptedRunner{responses: []runnerResponse{{stdout: []byte(inspect)}, {stdout: []byte(nodes)}}}

	state, err := manager.Status(context.Background(), "home")
	if err != nil {
		t.Fatal(err)
	}
	if state.NodeName != "server-second" || !state.NodeReady || state.K3sVersion != "v1.36.2+k3s1" {
		t.Fatalf("unexpected server state: %#v", state)
	}
}

func TestCurrentClusterRoundTripAndListing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))

	for _, name := range []string{"zeta", "alpha"} {
		path, err := KubeconfigPath(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("apiVersion: v1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := SetCurrentCluster("zeta"); err != nil {
		t.Fatal(err)
	}
	current, err := CurrentCluster()
	if err != nil || current != "zeta" {
		t.Fatalf("current = %q, err = %v", current, err)
	}
	clusters, err := ListClusters()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clusters, []string{"alpha", "zeta"}) {
		t.Fatalf("clusters = %#v", clusters)
	}
}

func TestUpdatedAddressOnExistingSubnetFollowsDHCPChange(t *testing.T) {
	oldAddress := mustCIDR(t, "192.168.50.10/24")
	newAddress := mustCIDR(t, "192.168.50.11/24")
	loopback := mustCIDR(t, "127.0.0.1/8")

	updated, changed := updatedAddressOnExistingSubnet("192.168.50.10", []net.Addr{loopback, newAddress})
	if !changed || updated != "192.168.50.11" {
		t.Fatalf("updated = %q, changed = %t", updated, changed)
	}
	unchanged, changed := updatedAddressOnExistingSubnet("192.168.50.10", []net.Addr{oldAddress, newAddress})
	if changed || unchanged != "192.168.50.10" {
		t.Fatalf("unchanged = %q, changed = %t", unchanged, changed)
	}
}

func TestAgentConfigRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	config, err := normalizeAgentConfig(AgentConfig{
		Name: "home", NodeName: "mini", ServerURL: "https://192.0.2.10:16443",
		TokenFile: filepath.Join(home, "token"), AdvertiseAddress: "192.0.2.20",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := saveAgentConfig(config); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadAgentConfig("home")
	if err != nil {
		t.Fatal(err)
	}
	if !sameAgentRuntimeConfig(config, loaded) {
		t.Fatalf("loaded config differs: %#v", loaded)
	}
}

func mustCIDR(t *testing.T, value string) *net.IPNet {
	t.Helper()
	ip, network, err := net.ParseCIDR(value)
	if err != nil {
		t.Fatal(err)
	}
	network.IP = ip
	return network
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

type runnerResponse struct {
	stdout []byte
	stderr []byte
	err    error
}

type scriptedRunner struct {
	responses []runnerResponse
	calls     [][]string
}

func (r *scriptedRunner) Run(_ context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	r.calls = append(r.calls, append([]string(nil), arguments...))
	response := r.responses[len(r.calls)-1]
	return response.stdout, response.stderr, response.err
}
