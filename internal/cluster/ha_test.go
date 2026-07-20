package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
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
		"PRIMARY_IP=$(ip -o -4 addr show dev eth0 scope global | awk 'NR == 1 {split($4, address, \"/\"); print address[1]}'); case \"$PRIMARY_IP\" in 192.168.96.12|192.168.96.13) echo \"APC_HA_RUNTIME_IP_COLLISION=$PRIMARY_IP\" >&2; exit 78;; esac; ip address add 192.168.96.11/24 dev eth0 2>/dev/null || true; ip route replace 192.168.96.0/24 dev eth0 src 192.168.96.11; exec /bin/k3s \"$@\"",
	} {
		if !contains(seedArguments, required) {
			t.Fatalf("seed arguments do not contain %q: %#v", required, seedArguments)
		}
	}
	if contains(joinedArguments, "--cluster-init") || !contains(joinedArguments, "https://192.168.96.11:6443") {
		t.Fatalf("joiner arguments are not seed-directed: %#v", joinedArguments)
	}
	for _, argument := range seedArguments {
		if strings.Contains(argument, ",ip=") || argument == "--ip" || argument == "--ip-address" {
			t.Fatalf("apple/container 1.0 has no fixed-IPv4 run option, got %#v", seedArguments)
		}
	}
	if strings.Contains(strings.Join(seedArguments, " "), "test-secret-value") {
		t.Fatal("server token value leaked into process arguments")
	}
}

func TestValidateHAContainerAcceptsOnlyExactPreviousInitForMigration(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	member := config.Members[0]
	record := configuredHAContainer(config, member, "running")
	record.Configuration.InitProcess.Arguments = legacyHAInitArguments(config, member)

	if err := validateHAContainer(record, config, member); err != nil {
		t.Fatalf("exact previous envelope was rejected: %v", err)
	}
	if !haContainerUsesLegacyInit(record, config, member) {
		t.Fatal("exact previous envelope was not classified for migration")
	}
	record.Configuration.InitProcess.Arguments[1] += "; true"
	if err := validateHAContainer(record, config, member); err == nil || !strings.Contains(err.Error(), "init process") {
		t.Fatalf("modified previous envelope validation error = %v", err)
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
	kubeconfig := validHAKubeconfig(t, "https://127.0.0.1:6443")
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
			return kubeconfig, nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	var probes []string
	path, err := manager.prepareKubeconfig(context.Background(), config.Name, func(_ context.Context, endpoint string, tlsConfig *tls.Config) bool {
		probes = append(probes, endpoint)
		return endpoint == config.Members[1].apiEndpoint(config.ListenAddress)
	})
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
	proxyEndpoint, err := haProxyEndpoint(config)
	if err != nil {
		t.Fatal(err)
	}
	wantProbes := []string{proxyEndpoint, config.Members[1].apiEndpoint(config.ListenAddress)}
	if !reflect.DeepEqual(probes, wantProbes) {
		t.Fatalf("probes = %#v, want %#v", probes, wantProbes)
	}
}

func TestPrepareKubeconfigFallsBackDirectlyWhenProxyDerivationFails(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	for index := range config.Members {
		config.Members[index].HostAPIPort = 1024 + index
	}
	if _, err := haProxyEndpoint(config); err == nil {
		t.Fatal("test configuration unexpectedly has a valid derived proxy endpoint")
	}
	kubeconfig := validHAKubeconfig(t, config.Members[0].apiEndpoint(config.ListenAddress))
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) >= 7 && arguments[0] == "exec" && arguments[2] == "kubectl":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`, member.NodeName)), nil, nil
		case len(arguments) == 4 && arguments[0] == "exec" && arguments[2] == "/bin/cat":
			return kubeconfig, nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	var probes []string
	path, err := manager.prepareHAKubeconfigFromConfig(context.Background(), config, func(_ context.Context, endpoint string, _ *tls.Config) bool {
		probes = append(probes, endpoint)
		return endpoint == config.Members[0].apiEndpoint(config.ListenAddress)
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != config.KubeconfigPath {
		t.Fatalf("path = %q, want %q", path, config.KubeconfigPath)
	}
	want := []string{config.Members[0].apiEndpoint(config.ListenAddress)}
	if !reflect.DeepEqual(probes, want) {
		t.Fatalf("proxy derivation failure probes = %#v, want direct %#v", probes, want)
	}
}

func TestPrepareKubeconfigPrefersAuthenticatedStableProxy(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	kubeconfig := validHAKubeconfig(t, "https://127.0.0.1:6443")
	if err := os.WriteFile(config.KubeconfigPath, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) >= 7 && arguments[0] == "exec" && arguments[2] == "kubectl":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`, member.NodeName)), nil, nil
		case len(arguments) == 4 && arguments[0] == "exec" && arguments[2] == "/bin/cat":
			return kubeconfig, nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	proxyEndpoint, err := haProxyEndpoint(config)
	if err != nil {
		t.Fatal(err)
	}
	var probes []string
	path, err := manager.prepareKubeconfig(context.Background(), config.Name, func(_ context.Context, endpoint string, tlsConfig *tls.Config) bool {
		probes = append(probes, endpoint)
		if tlsConfig == nil || tlsConfig.RootCAs == nil || len(tlsConfig.Certificates) != 1 {
			t.Fatal("proxy probe did not receive authenticated client TLS credentials")
		}
		return endpoint == proxyEndpoint
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(probes, []string{proxyEndpoint}) {
		t.Fatalf("probes = %#v", probes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "server: "+proxyEndpoint) {
		t.Fatalf("kubeconfig did not select stable proxy: %s", data)
	}
	if _, err := loadHAClientTLSConfigData(data); err != nil {
		t.Fatalf("proxy rewrite did not preserve HA credentials: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("kubeconfig mode = %o, want 600", info.Mode().Perm())
	}
}

func validHAKubeconfig(t *testing.T, endpoint string) []byte {
	t.Helper()
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	certificate := server.TLS.Certificates[0]
	keyDER, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	encode := base64.StdEncoding.EncodeToString
	return []byte(fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: %s
    server: %s
  name: default
contexts: []
current-context: default
kind: Config
users:
- name: default
  user:
    client-certificate-data: %s
    client-key-data: %s
`, encode(certificatePEM), endpoint, encode(certificatePEM), encode(keyPEM)))
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

func TestWritePrivateFileAtomicReportsDirectorySyncFailureAfterRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "state.json")
	syncFailure := errors.New("injected directory sync failure")
	var syncedDirectory string
	err := writePrivateFileAtomicWithDirectorySync(path, []byte("durable candidate\n"), func(directory string) error {
		syncedDirectory = directory
		return syncFailure
	})
	if !errors.Is(err, syncFailure) || !strings.Contains(err.Error(), "sync private file directory") {
		t.Fatalf("directory sync error = %v", err)
	}
	if syncedDirectory != filepath.Dir(path) {
		t.Fatalf("synced directory = %q, want %q", syncedDirectory, filepath.Dir(path))
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(data) != "durable candidate\n" {
		t.Fatalf("renamed file = %q", data)
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

func TestValidateHAContainerRejectsUnexpectedServerEnvelope(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	member := config.Members[0]
	tests := []struct {
		name   string
		mutate func(*haContainerInspect)
		want   string
	}{
		{
			name: "extra bind mount",
			mutate: func(record *haContainerInspect) {
				extra := record.Configuration.Mounts[1]
				extra.Destination = "/unexpected"
				record.Configuration.Mounts = append(record.Configuration.Mounts, extra)
			},
			want: "expected exactly two",
		},
		{
			name: "extra network",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Networks = append(record.Configuration.Networks, record.Configuration.Networks[0])
			},
			want: "expected exactly one",
		},
		{
			name: "extra port",
			mutate: func(record *haContainerInspect) {
				record.Configuration.PublishedPorts = append(record.Configuration.PublishedPorts, record.Configuration.PublishedPorts[0])
			},
			want: "exactly one API port",
		},
		{
			name: "published socket",
			mutate: func(record *haContainerInspect) {
				record.Configuration.PublishedSockets = []json.RawMessage{json.RawMessage(`{"hostPath":"/tmp/foreign.sock"}`)}
			},
			want: "no sockets",
		},
		{
			name: "wrong entrypoint",
			mutate: func(record *haContainerInspect) {
				record.Configuration.InitProcess.Executable = "/bin/bash"
			},
			want: "init process",
		},
		{
			name: "wrong platform",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Platform.Architecture = "amd64"
			},
			want: "linux/arm64",
		},
		{
			name: "unexpected added capability",
			mutate: func(record *haContainerInspect) {
				record.Configuration.CapAdd = []string{"ALL", "NET_ADMIN"}
			},
			want: "cap-add ALL",
		},
		{
			name: "unexpected dropped capability",
			mutate: func(record *haContainerInspect) {
				record.Configuration.CapDrop = []string{"SYS_ADMIN"}
			},
			want: "no dropped capabilities",
		},
		{
			name: "non-root user",
			mutate: func(record *haContainerInspect) {
				record.Configuration.InitProcess.User.ID.UID = 1000
			},
			want: "root, non-TTY",
		},
		{
			name: "terminal",
			mutate: func(record *haContainerInspect) {
				record.Configuration.InitProcess.Terminal = true
			},
			want: "non-TTY",
		},
		{
			name: "nested virtualization",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Virtualization = true
			},
			want: "unexpected runtime feature",
		},
		{
			name: "rosetta",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Rosetta = true
			},
			want: "unexpected runtime feature",
		},
		{
			name: "ssh",
			mutate: func(record *haContainerInspect) {
				record.Configuration.SSH = true
			},
			want: "unexpected runtime feature",
		},
		{
			name: "read-only root filesystem",
			mutate: func(record *haContainerInspect) {
				record.Configuration.ReadOnly = true
			},
			want: "unexpected runtime feature",
		},
		{
			name: "init wrapper",
			mutate: func(record *haContainerInspect) {
				record.Configuration.UseInit = true
			},
			want: "unexpected runtime feature",
		},
		{
			name: "sysctl",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Sysctls["net.ipv4.ip_forward"] = "1"
			},
			want: "unexpected runtime feature",
		},
		{
			name: "wrong resources",
			mutate: func(record *haContainerInspect) {
				record.Configuration.Resources.CPUs++
			},
			want: "CPU and memory",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := configuredHAContainer(config, member, "running")
			test.mutate(&record)
			err := validateHAContainer(record, config, member)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestReconcileHAMemberRejectsUnexpectedEnvelopeBeforeDelete(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	member := config.Members[0]
	record := configuredHAContainer(config, member, "stopped")
	record.Configuration.Networks = append(record.Configuration.Networks, record.Configuration.Networks[0])
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		t.Fatalf("unexpected mutation: %#v", arguments)
		return nil, nil, errors.New("unexpected mutation")
	}}
	manager := NewManager("container")
	manager.runner = runner
	if err := manager.reconcileHAMember(context.Background(), config, member, record); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("reconcile error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("reconcile mutated an untrusted container: %#v", runner.calls)
	}
}

func TestHALifecycleOperationsWaitForHeldLockBeforeRuntimeMutation(t *testing.T) {
	tests := []struct {
		name       string
		needsState bool
		invoke     func(context.Context, *Manager, HAConfig) error
	}{
		{
			name: "create",
			invoke: func(ctx context.Context, manager *Manager, config HAConfig) error {
				_, err := manager.CreateHA(ctx, config)
				return err
			},
		},
		{
			name:       "start",
			needsState: true,
			invoke: func(ctx context.Context, manager *Manager, config HAConfig) error {
				_, err := manager.StartHA(ctx, config.Name, time.Second)
				return err
			},
		},
		{
			name:       "stop",
			needsState: true,
			invoke: func(ctx context.Context, manager *Manager, config HAConfig) error {
				return manager.StopHA(ctx, config.Name)
			},
		},
		{
			name:       "delete",
			needsState: true,
			invoke: func(ctx context.Context, manager *Manager, config HAConfig) error {
				return manager.DeleteHA(ctx, config.Name, false)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			config := liveHAConfig(t)
			if test.needsState {
				if err := saveHAConfig(config); err != nil {
					t.Fatal(err)
				}
			}
			held, err := acquireHAOperationLock(context.Background(), config.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := held.release(); err != nil {
					t.Error(err)
				}
			}()

			runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
				t.Fatalf("runtime mutation or inspection occurred while HA lock was held: %#v", arguments)
				return nil, nil, errors.New("unexpected runtime operation")
			}}
			manager := NewManager("container")
			manager.runner = runner
			operationCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err = test.invoke(operationCtx, manager, config)
			if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "operation lock") {
				t.Fatalf("held-lock error = %v", err)
			}
			if len(runner.calls) != 0 {
				t.Fatalf("runtime was touched while HA lock was held: %#v", runner.calls)
			}
		})
	}
}

func TestCreateHAFreshLockDirectoryAndStartWithoutRecursiveLock(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	configPath, err := HAConfigPath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Dir(configPath)
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh HA directory unexpectedly exists: %v", err)
	}
	runner := newHACreateLifecycleRunner(t, config)
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	state, err := manager.CreateHA(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadyMembers != haMemberCount || !state.Healthy {
		t.Fatalf("fresh HA state = %+v", state)
	}
	desired, err := loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredRunning {
		t.Fatalf("fresh create desired state = %+v", desired)
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("fresh HA directory mode = %v", directoryInfo.Mode())
	}
	lockInfo, err := os.Lstat(filepath.Join(directory, haOperationLockFilename))
	if err != nil {
		t.Fatal(err)
	}
	if !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0o600 {
		t.Fatalf("HA lifecycle lock mode = %v", lockInfo.Mode())
	}

	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	state, err = manager.CreateHA(context.Background(), config)
	if err != nil {
		t.Fatalf("idempotent create from Stopped intent failed: %v", err)
	}
	desired, err = loadHADesiredState(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if desired.ClusterState != haDesiredRunning || len(desired.StoppedMembers) != 0 {
		t.Fatalf("idempotent create desired state = %+v, want Running", desired)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	state, err = manager.StartHA(startCtx, config.Name, time.Second)
	if err != nil {
		t.Fatalf("start with one lifecycle lock acquisition failed: %v", err)
	}
	if state.ReadyMembers != haMemberCount || !state.Healthy {
		t.Fatalf("started HA state = %+v", state)
	}
}

func TestDeleteHACleansUpPartialOwnedVolumeState(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	if err := markHAClusterStoppedLocked(config.Name); err != nil {
		t.Fatal(err)
	}
	recoveryPath, err := HARecoveryStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePrivateFileAtomic(recoveryPath, []byte("stale recovery journal\n")); err != nil {
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
	desiredPath, err := haDesiredStatePath(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{recoveryPath, desiredPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("full HA delete left stale local state %q: %v", filepath.Base(path), err)
		}
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
	record.Configuration.Platform.Architecture = "arm64"
	record.Configuration.Platform.OS = "linux"
	record.Configuration.Labels = map[string]string{
		"apc.dev/managed": "true",
		"apc.dev/cluster": config.Name,
		"apc.dev/role":    "server",
		"apc.dev/member":  strconv.Itoa(member.ID),
	}
	record.Configuration.Image.Reference = config.Image
	record.Configuration.InitProcess.Arguments = haInitArguments(config, member)
	record.Configuration.InitProcess.Executable = "/bin/sh"
	record.Configuration.InitProcess.User.ID = &haContainerUserID{}
	record.Configuration.CapAdd = []string{"ALL"}
	record.Configuration.CapDrop = []string{}
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
		Count         int    `json:"count"`
		HostAddress   string `json:"hostAddress"`
		HostPort      int    `json:"hostPort"`
		Proto         string `json:"proto"`
	}, 1)
	record.Configuration.PublishedPorts[0].ContainerPort = 6443
	record.Configuration.PublishedPorts[0].Count = 1
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
	record.Configuration.Sysctls = map[string]string{}
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

func newHACreateLifecycleRunner(t *testing.T, config HAConfig) *haTestRunner {
	t.Helper()
	networkExists := false
	volumes := map[int]bool{}
	containers := map[int]string{}
	runner := &haTestRunner{}
	runner.handler = func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 3 && arguments[0] == "network" && arguments[1] == "inspect":
			if !networkExists {
				return nil, []byte("not found"), errors.New("exit 1")
			}
			var record haNetworkInspect
			record.Configuration.Name = config.NetworkName
			record.Configuration.IPv4Subnet = config.Subnet
			record.Configuration.Labels = map[string]string{"apc.dev/managed": "true", "apc.dev/cluster": config.Name}
			return marshalHAInspect(t, record), nil, nil
		case len(arguments) == 3 && arguments[0] == "volume" && arguments[1] == "inspect":
			member := memberForHAVolume(t, config, arguments[2])
			if !volumes[member.ID] {
				return nil, []byte("not found"), errors.New("exit 1")
			}
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
			state, exists := containers[member.ID]
			if !exists {
				return nil, []byte("not found"), errors.New("exit 1")
			}
			return marshalHAInspect(t, configuredHAContainer(config, member, state)), nil, nil
		case len(arguments) >= 2 && arguments[0] == "network" && arguments[1] == "create":
			networkExists = true
			return nil, nil, nil
		case len(arguments) >= 2 && arguments[0] == "volume" && arguments[1] == "create":
			member := memberForHAVolume(t, config, arguments[len(arguments)-1])
			volumes[member.ID] = true
			return nil, nil, nil
		case len(arguments) > 0 && arguments[0] == "run":
			name := haTestFlagValue(arguments, "--name")
			member := memberForHAContainer(t, config, name)
			containers[member.ID] = "running"
			return nil, nil, nil
		case len(arguments) >= 7 && arguments[0] == "exec" && arguments[2] == "kubectl":
			member := memberForHAContainer(t, config, arguments[1])
			return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":"True"}],"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"}}}`, member.NodeName)), nil, nil
		case len(arguments) == 4 && arguments[0] == "exec" && arguments[2] == "/bin/cat":
			return []byte("apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443\n  name: default\n"), nil, nil
		default:
			t.Fatalf("unexpected fresh HA lifecycle command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}
	return runner
}

func haTestFlagValue(arguments []string, flag string) string {
	for index := 0; index+1 < len(arguments); index++ {
		if arguments[index] == flag {
			return arguments[index+1]
		}
	}
	return ""
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
