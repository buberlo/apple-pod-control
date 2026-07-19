package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultK3sVersion   = "v1.36.2+k3s1"
	DefaultK3sImage     = "docker.io/rancher/k3s@sha256:6a47cea22c4b834d4ba72c89d291696b79ebe406251f90b446e4dff03513dd87"
	DefaultAPIPort      = 16443
	DefaultVXLANPort    = 8472
	DefaultKubeletPort  = 10250
	defaultVolumeSize   = "8G"
	agentTokenMountDir  = "/run/secrets/apc"
	agentTokenMountPath = agentTokenMountDir + "/agent-token"
	dynamicNodeIPScript = `NODE_IP=$(hostname -i); NODE_IP=${NODE_IP%% *}; NODE_CIDR=$(ip -o -4 addr show dev eth0 scope global | awk 'NR == 1 {print $4}'); PREFIX=${NODE_CIDR#*/}; EFFECTIVE_NODE_IP=${APC_STABLE_NODE_IP:-$NODE_IP}; if [ "$EFFECTIVE_NODE_IP" != "$NODE_IP" ]; then ip address add "$EFFECTIVE_NODE_IP/$PREFIX" dev eth0 2>/dev/null || true; fi; exec /bin/k3s "$@" --node-ip "$EFFECTIVE_NODE_IP"`
	agentNodeIPScript   = `mkdir -p /var/lib/rancher/k3s/apc-node-identity /etc/rancher; if [ ! -L /etc/rancher/node ]; then rm -rf /etc/rancher/node; ln -s /var/lib/rancher/k3s/apc-node-identity /etc/rancher/node; fi; NODE_IP=$(hostname -i); NODE_IP=${NODE_IP%% *}; NODE_CIDR=$(ip -o -4 addr show dev eth0 scope global | awk 'NR == 1 {print $4}'); PREFIX=${NODE_CIDR#*/}; STORED_NODE_IP=""; if [ -f /var/lib/rancher/k3s/agent/kubelet.kubeconfig ]; then STORED_NODE_IP=$(/bin/kubectl --request-timeout=5s --kubeconfig /var/lib/rancher/k3s/agent/kubelet.kubeconfig --server "$APC_SERVER_URL" get node "$APC_NODE_NAME" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true); fi; EFFECTIVE_NODE_IP=${STORED_NODE_IP:-$APC_PREVIOUS_NODE_IP}; EFFECTIVE_NODE_IP=${EFFECTIVE_NODE_IP:-$NODE_IP}; if [ "$EFFECTIVE_NODE_IP" != "$NODE_IP" ]; then ip address add "$EFFECTIVE_NODE_IP/$PREFIX" dev eth0 2>/dev/null || true; fi; exec /bin/k3s "$@" --node-ip "$EFFECTIVE_NODE_IP"`
)

var (
	ErrNotFound         = errors.New("cluster not found")
	ErrNoCurrentCluster = errors.New("no current APC cluster")
)

var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

type Config struct {
	Name                string
	NodeName            string
	Image               string
	CPUs                int
	Memory              string
	ListenAddress       string
	AdvertiseAddress    string
	APIPort             int
	VXLANPort           int
	KubeletPort         int
	StartupTimeout      time.Duration
	KubeconfigPath      string
	DisableTraefik      bool
	EnableNetworkPolicy bool
	StableNodeIP        string
}

type AgentConfig struct {
	Name             string
	NodeName         string
	ServerURL        string
	TokenFile        string
	Image            string
	CPUs             int
	Memory           string
	ListenAddress    string
	AdvertiseAddress string
	VXLANPort        int
	KubeletPort      int
	StartupTimeout   time.Duration
	PreviousNodeIP   string
}

type State struct {
	Name         string `json:"name" yaml:"name"`
	Container    string `json:"container" yaml:"container"`
	RuntimeState string `json:"runtimeState" yaml:"runtimeState"`
	Address      string `json:"address,omitempty" yaml:"address,omitempty"`
	APIEndpoint  string `json:"apiEndpoint" yaml:"apiEndpoint"`
	NodeName     string `json:"nodeName,omitempty" yaml:"nodeName,omitempty"`
	NodeReady    bool   `json:"nodeReady" yaml:"nodeReady"`
	K3sVersion   string `json:"k3sVersion,omitempty" yaml:"k3sVersion,omitempty"`
	Kubeconfig   string `json:"kubeconfig,omitempty" yaml:"kubeconfig,omitempty"`
	InternalIP   string `json:"internalIP,omitempty" yaml:"internalIP,omitempty"`
}

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, []byte, error)
}

type streamCommandRunner interface {
	RunIO(context.Context, string, []string, io.Reader, io.Writer, io.Writer) error
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, binary string, arguments ...string) ([]byte, []byte, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func (execRunner) RunIO(ctx context.Context, binary string, arguments []string, stdin io.Reader, stdout, stderr io.Writer) error {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	return command.Run()
}

type Manager struct {
	binary  string
	runner  commandRunner
	stream  streamCommandRunner
	dialTCP func(context.Context, string) error
}

func NewManager(binary string) *Manager {
	if binary == "" {
		binary = "container"
	}
	if binary == "container" {
		if resolved, err := exec.LookPath(binary); err == nil {
			binary = resolved
		} else if info, statErr := os.Stat("/usr/local/bin/container"); statErr == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			binary = "/usr/local/bin/container"
		}
	}
	return &Manager{
		binary: binary,
		runner: execRunner{},
		stream: execRunner{},
		dialTCP: func(ctx context.Context, address string) error {
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if err != nil {
				return err
			}
			return connection.Close()
		},
	}
}

func (m *Manager) Create(ctx context.Context, config Config) (State, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return State{}, err
	}
	containerName := ContainerName(config.Name)
	if err := m.ensureVolume(ctx, ServerVolumeName(config.Name), config.Name, "server"); err != nil {
		return State{}, err
	}
	record, inspectErr := m.inspect(ctx, containerName)
	switch {
	case inspectErr == nil:
		if err := validateOwnedContainer(record, config.Name, "server"); err != nil {
			return State{}, err
		}
		if strings.EqualFold(record.Status.State, "stopped") {
			if stored, loadErr := loadClusterConfig(config.Name); loadErr == nil {
				config = stored
			}
			if err := m.deleteStoppedContainer(ctx, containerName); err != nil {
				return State{}, err
			}
			if _, stderr, runErr := m.runner.Run(ctx, m.binary, ServerRunArguments(config)...); runErr != nil {
				return State{}, commandError("recreate K3s node", stderr, runErr)
			}
		}
	case errors.Is(inspectErr, ErrNotFound):
		arguments := ServerRunArguments(config)
		if _, stderr, runErr := m.runner.Run(ctx, m.binary, arguments...); runErr != nil {
			return State{}, commandError("create K3s node", stderr, runErr)
		}
	default:
		return State{}, inspectErr
	}

	if err := m.waitReady(ctx, containerName, config.NodeName, config.StableNodeIP, config.StartupTimeout); err != nil {
		return State{}, err
	}
	kubeconfig, err := m.readKubeconfig(ctx, containerName, config.APIEndpoint())
	if err != nil {
		return State{}, err
	}
	if err := writePrivateFile(config.KubeconfigPath, kubeconfig); err != nil {
		return State{}, err
	}
	if err := saveClusterConfig(config); err != nil {
		return State{}, err
	}
	if err := SetCurrentCluster(config.Name); err != nil {
		return State{}, err
	}
	state, err := m.Status(ctx, config.Name)
	if err != nil {
		return State{}, err
	}
	state.Kubeconfig = config.KubeconfigPath
	if config.StableNodeIP == "" && state.InternalIP != "" {
		config.StableNodeIP = state.InternalIP
		if err := saveClusterConfig(config); err != nil {
			return State{}, err
		}
	}
	return state, nil
}

func (m *Manager) Join(ctx context.Context, config AgentConfig) (State, error) {
	config, err := normalizeAgentConfig(config)
	if err != nil {
		return State{}, err
	}
	if err := validatePrivateTokenFile(config.TokenFile); err != nil {
		return State{}, err
	}
	if err := m.ensureVolume(ctx, AgentVolumeName(config.Name), config.Name, "agent"); err != nil {
		return State{}, err
	}
	containerName := AgentContainerName(config.Name)
	record, inspectErr := m.inspect(ctx, containerName)
	switch {
	case inspectErr == nil:
		if err := validateOwnedContainer(record, config.Name, "agent"); err != nil {
			return State{}, err
		}
		recreate := strings.EqualFold(record.Status.State, "stopped")
		if stored, loadErr := loadAgentConfig(config.Name); loadErr != nil || !sameAgentRuntimeConfig(stored, config) {
			recreate = true
		}
		if recreate && strings.EqualFold(record.Status.State, "running") {
			if _, stderr, stopErr := m.runner.Run(ctx, m.binary, "stop", containerName); stopErr != nil {
				return State{}, commandError("stop K3s agent for configuration update", stderr, stopErr)
			}
		}
		if recreate {
			if err := m.deleteStoppedContainer(ctx, containerName); err != nil {
				return State{}, err
			}
			if _, stderr, runErr := m.runner.Run(ctx, m.binary, AgentRunArguments(config)...); runErr != nil {
				return State{}, commandError("recreate K3s agent", stderr, runErr)
			}
		}
	case errors.Is(inspectErr, ErrNotFound):
		if _, stderr, runErr := m.runner.Run(ctx, m.binary, AgentRunArguments(config)...); runErr != nil {
			return State{}, commandError("create K3s agent", stderr, runErr)
		}
	default:
		return State{}, inspectErr
	}
	if err := m.waitAgentConnected(ctx, containerName, config.StartupTimeout); err != nil {
		return State{}, err
	}
	state, err := m.AgentStatus(ctx, config.Name)
	if err != nil {
		return State{}, err
	}
	if config.PreviousNodeIP == "" {
		config.PreviousNodeIP = state.Address
	}
	if err := saveAgentConfig(config); err != nil {
		return State{}, err
	}
	state.NodeName = config.NodeName
	return state, nil
}

func (m *Manager) StopAgent(ctx context.Context, name string) error {
	record, err := m.inspect(ctx, AgentContainerName(name))
	if err != nil {
		return err
	}
	if err := validateOwnedContainer(record, name, "agent"); err != nil {
		return err
	}
	if strings.EqualFold(record.Status.State, "stopped") {
		return nil
	}
	_, stderr, err := m.runner.Run(ctx, m.binary, "stop", AgentContainerName(name))
	if err != nil {
		return commandError("stop K3s agent", stderr, err)
	}
	return nil
}

func (m *Manager) StartAgent(ctx context.Context, name string, timeout time.Duration) (State, error) {
	config, err := loadAgentConfig(name)
	if err != nil {
		return State{}, err
	}
	if timeout > 0 {
		config.StartupTimeout = timeout
	}
	config, addressChanged, err := refreshAgentAdvertiseAddress(config)
	if err != nil {
		return State{}, err
	}
	if err := validatePrivateTokenFile(config.TokenFile); err != nil {
		return State{}, err
	}
	record, err := m.inspect(ctx, AgentContainerName(name))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return State{}, err
	}
	if err == nil {
		if err := validateOwnedContainer(record, name, "agent"); err != nil {
			return State{}, err
		}
	}
	if addressChanged && strings.EqualFold(record.Status.State, "running") {
		if _, stderr, stopErr := m.runner.Run(ctx, m.binary, "stop", AgentContainerName(name)); stopErr != nil {
			return State{}, commandError("stop K3s agent for LAN address update", stderr, stopErr)
		}
		record.Status.State = "stopped"
	}
	if errors.Is(err, ErrNotFound) || strings.EqualFold(record.Status.State, "stopped") {
		if err := m.ensureVolume(ctx, AgentVolumeName(name), name, "agent"); err != nil {
			return State{}, err
		}
		if !errors.Is(err, ErrNotFound) {
			if err := m.deleteStoppedContainer(ctx, AgentContainerName(name)); err != nil {
				return State{}, err
			}
		}
		if _, stderr, runErr := m.runner.Run(ctx, m.binary, AgentRunArguments(config)...); runErr != nil {
			return State{}, commandError("recreate K3s agent", stderr, runErr)
		}
	}
	if err := m.waitAgentConnected(ctx, AgentContainerName(name), config.StartupTimeout); err != nil {
		return State{}, err
	}
	state, err := m.AgentStatus(ctx, name)
	if err != nil {
		return State{}, err
	}
	if config.PreviousNodeIP == "" {
		config.PreviousNodeIP = state.Address
	}
	if err := saveAgentConfig(config); err != nil {
		return State{}, err
	}
	state.NodeName = config.NodeName
	return state, nil
}

func (m *Manager) Status(ctx context.Context, name string) (State, error) {
	if !dnsLabel.MatchString(name) {
		return State{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	containerName := ContainerName(name)
	record, err := m.inspect(ctx, containerName)
	if err != nil {
		return State{}, err
	}
	if err := validateOwnedContainer(record, name, "server"); err != nil {
		return State{}, err
	}
	state := State{Name: name, Container: containerName, RuntimeState: record.Status.State}
	if len(record.Status.Networks) > 0 {
		state.Address = strings.Split(record.Status.Networks[0].IPv4Address, "/")[0]
	}
	state.APIEndpoint = apiEndpointFromRecord(record)
	if !strings.EqualFold(record.Status.State, "running") {
		return state, nil
	}
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "exec", containerName, "kubectl", "get", "node", "-o", "json")
	if err != nil {
		return state, commandError("read Kubernetes node status", stderr, err)
	}
	var nodes struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				NodeInfo struct {
					KubeletVersion string `json:"kubeletVersion"`
				} `json:"nodeInfo"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
				Addresses []struct {
					Type    string `json:"type"`
					Address string `json:"address"`
				} `json:"addresses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(stdout, &nodes); err != nil {
		return state, fmt.Errorf("decode Kubernetes node status: %w", err)
	}
	if len(nodes.Items) > 0 {
		selected := &nodes.Items[0]
		expectedNodeName := ""
		if config, configErr := loadClusterConfig(name); configErr == nil {
			expectedNodeName = config.NodeName
		}
		for index := range nodes.Items {
			if expectedNodeName != "" && nodes.Items[index].Metadata.Name == expectedNodeName {
				selected = &nodes.Items[index]
				break
			}
			for _, address := range nodes.Items[index].Status.Addresses {
				if expectedNodeName == "" && address.Type == "InternalIP" && address.Address == state.Address {
					selected = &nodes.Items[index]
				}
			}
		}
		state.NodeName = selected.Metadata.Name
		state.K3sVersion = selected.Status.NodeInfo.KubeletVersion
		for _, condition := range selected.Status.Conditions {
			if condition.Type == "Ready" {
				state.NodeReady = condition.Status == "True"
				break
			}
		}
		for _, address := range selected.Status.Addresses {
			if address.Type == "InternalIP" {
				state.InternalIP = address.Address
				break
			}
		}
	}
	if path, pathErr := KubeconfigPath(name); pathErr == nil {
		state.Kubeconfig = path
	}
	return state, nil
}

func (m *Manager) AgentStatus(ctx context.Context, name string) (State, error) {
	if !dnsLabel.MatchString(name) {
		return State{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	containerName := AgentContainerName(name)
	record, err := m.inspect(ctx, containerName)
	if err != nil {
		return State{}, err
	}
	if err := validateOwnedContainer(record, name, "agent"); err != nil {
		return State{}, err
	}
	state := State{Name: name, Container: containerName, RuntimeState: record.Status.State}
	if len(record.Status.Networks) > 0 {
		state.Address = strings.Split(record.Status.Networks[0].IPv4Address, "/")[0]
	}
	return state, nil
}

func (m *Manager) Kubectl(ctx context.Context, name string, arguments ...string) ([]byte, []byte, error) {
	record, err := m.inspect(ctx, ContainerName(name))
	if err != nil {
		return nil, nil, err
	}
	if err := validateOwnedContainer(record, name, "server"); err != nil {
		return nil, nil, err
	}
	if !strings.EqualFold(record.Status.State, "running") {
		return nil, nil, fmt.Errorf("cluster %q is %s", name, record.Status.State)
	}
	commandArguments := append([]string{"exec", ContainerName(name), "kubectl"}, arguments...)
	return m.runner.Run(ctx, m.binary, commandArguments...)
}

func (m *Manager) Stop(ctx context.Context, name string) error {
	record, err := m.inspect(ctx, ContainerName(name))
	if err != nil {
		return err
	}
	if err := validateOwnedContainer(record, name, "server"); err != nil {
		return err
	}
	if strings.EqualFold(record.Status.State, "stopped") {
		return nil
	}
	_, stderr, err := m.runner.Run(ctx, m.binary, "stop", ContainerName(name))
	if err != nil {
		return commandError("stop K3s node", stderr, err)
	}
	return nil
}

func (m *Manager) Start(ctx context.Context, name string, timeout time.Duration) (State, error) {
	config, err := loadClusterConfig(name)
	if err != nil {
		return State{}, err
	}
	config, addressChanged, err := refreshAdvertiseAddress(config)
	if err != nil {
		return State{}, err
	}
	record, err := m.inspect(ctx, ContainerName(name))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return State{}, err
	}
	if err == nil {
		if err := validateOwnedContainer(record, name, "server"); err != nil {
			return State{}, err
		}
	}
	if addressChanged && strings.EqualFold(record.Status.State, "running") {
		if _, stderr, stopErr := m.runner.Run(ctx, m.binary, "stop", ContainerName(name)); stopErr != nil {
			return State{}, commandError("stop K3s node for LAN address update", stderr, stopErr)
		}
		record.Status.State = "stopped"
	}
	if errors.Is(err, ErrNotFound) || strings.EqualFold(record.Status.State, "stopped") {
		if err := m.ensureVolume(ctx, ServerVolumeName(name), name, "server"); err != nil {
			return State{}, err
		}
		if !errors.Is(err, ErrNotFound) {
			if err := m.deleteStoppedContainer(ctx, ContainerName(name)); err != nil {
				return State{}, err
			}
		}
		if _, stderr, runErr := m.runner.Run(ctx, m.binary, ServerRunArguments(config)...); runErr != nil {
			return State{}, commandError("recreate K3s node", stderr, runErr)
		}
	}
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	if err := m.waitReady(ctx, ContainerName(name), config.NodeName, config.StableNodeIP, timeout); err != nil {
		return State{}, err
	}
	kubeconfig, err := m.readKubeconfig(ctx, ContainerName(name), config.APIEndpoint())
	if err != nil {
		return State{}, err
	}
	if err := writePrivateFile(config.KubeconfigPath, kubeconfig); err != nil {
		return State{}, err
	}
	if err := saveClusterConfig(config); err != nil {
		return State{}, err
	}
	state, err := m.Status(ctx, name)
	if err != nil {
		return State{}, err
	}
	if err := SetCurrentCluster(name); err != nil {
		return State{}, err
	}
	if config.StableNodeIP == "" && state.InternalIP != "" {
		config.StableNodeIP = state.InternalIP
		if err := saveClusterConfig(config); err != nil {
			return State{}, err
		}
	}
	return state, nil
}

func refreshAdvertiseAddress(config Config) (Config, bool, error) {
	if config.AdvertiseAddress == "" {
		return config, false, nil
	}
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return Config{}, false, fmt.Errorf("list host network addresses: %w", err)
	}
	updated, changed := updatedAddressOnExistingSubnet(config.AdvertiseAddress, addresses)
	if changed {
		config.AdvertiseAddress = updated
	}
	return config, changed, nil
}

func refreshAgentAdvertiseAddress(config AgentConfig) (AgentConfig, bool, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return AgentConfig{}, false, fmt.Errorf("list host network addresses: %w", err)
	}
	updated, changed := updatedAddressOnExistingSubnet(config.AdvertiseAddress, addresses)
	if changed {
		config.AdvertiseAddress = updated
	}
	return config, changed, nil
}

func updatedAddressOnExistingSubnet(previous string, addresses []net.Addr) (string, bool) {
	previousIP := net.ParseIP(previous)
	if previousIP == nil {
		return previous, false
	}
	for _, address := range addresses {
		ip, _, ok := addressIPNet(address)
		if ok && ip.Equal(previousIP) {
			return previous, false
		}
	}
	for _, address := range addresses {
		ip, network, ok := addressIPNet(address)
		if !ok || ip.IsLoopback() || (previousIP.To4() == nil) != (ip.To4() == nil) {
			continue
		}
		if network.Contains(previousIP) {
			return ip.String(), true
		}
	}
	return previous, false
}

func addressIPNet(address net.Addr) (net.IP, *net.IPNet, bool) {
	switch value := address.(type) {
	case *net.IPNet:
		return value.IP, value, true
	case *net.IPAddr:
		bits := 128
		if value.IP.To4() != nil {
			bits = 32
		}
		return value.IP, &net.IPNet{IP: value.IP, Mask: net.CIDRMask(bits, bits)}, true
	default:
		ip, network, err := net.ParseCIDR(address.String())
		return ip, network, err == nil
	}
}

func (m *Manager) WriteAgentToken(ctx context.Context, name, path string) (string, error) {
	if !dnsLabel.MatchString(name) {
		return "", fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if path == "" {
		var err error
		path, err = AgentTokenPath(name)
		if err != nil {
			return "", err
		}
	}
	record, err := m.inspect(ctx, ContainerName(name))
	if err != nil {
		return "", err
	}
	if err := validateOwnedContainer(record, name, "server"); err != nil {
		return "", err
	}
	if !strings.EqualFold(record.Status.State, "running") {
		return "", fmt.Errorf("cluster %q is %s", name, record.Status.State)
	}
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "exec", ContainerName(name), "/bin/cat", "/var/lib/rancher/k3s/server/agent-token")
	if err != nil {
		return "", commandError("read K3s agent token", stderr, err)
	}
	token := strings.TrimSpace(string(stdout))
	if token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return "", fmt.Errorf("K3s returned an invalid agent token")
	}
	if err := writePrivateFile(path, []byte(token+"\n")); err != nil {
		return "", fmt.Errorf("write agent token: %w", err)
	}
	return path, nil
}

func ContainerName(clusterName string) string {
	return "apc-k3s-" + clusterName + "-server"
}

func AgentContainerName(clusterName string) string {
	return "apc-k3s-" + clusterName + "-agent"
}

func ServerVolumeName(clusterName string) string {
	return "apc-k3s-" + clusterName + "-server-data"
}

func AgentVolumeName(clusterName string) string {
	return "apc-k3s-" + clusterName + "-agent-data"
}

func KubeconfigPath(clusterName string) (string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(configDirectory, "apc", "clusters", clusterName, "kubeconfig"), nil
}

func ResolvedKubeconfigPath(clusterName string) (string, error) {
	if !dnsLabel.MatchString(clusterName) {
		return "", fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	config, err := loadClusterConfig(clusterName)
	if err == nil {
		return config.KubeconfigPath, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return KubeconfigPath(clusterName)
}

func SetCurrentCluster(clusterName string) error {
	if !dnsLabel.MatchString(clusterName) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	kubeconfig, err := ResolvedKubeconfigPath(clusterName)
	if err != nil {
		return err
	}
	info, err := os.Stat(kubeconfig)
	if err != nil {
		return fmt.Errorf("read kubeconfig for cluster %q: %w", clusterName, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("kubeconfig for cluster %q is not a regular file", clusterName)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("kubeconfig for cluster %q must have mode 0600 or stricter", clusterName)
	}
	path, err := currentClusterPath()
	if err != nil {
		return err
	}
	if err := writePrivateFile(path, []byte(clusterName+"\n")); err != nil {
		return fmt.Errorf("save current cluster: %w", err)
	}
	return nil
}

func CurrentCluster() (string, error) {
	path, err := currentClusterPath()
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNoCurrentCluster
	}
	if err != nil {
		return "", fmt.Errorf("read current cluster: %w", err)
	}
	name := strings.TrimSpace(string(data))
	if !dnsLabel.MatchString(name) {
		return "", fmt.Errorf("current cluster file contains an invalid name")
	}
	return name, nil
}

func ListClusters() ([]string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user configuration directory: %w", err)
	}
	root := filepath.Join(configDirectory, "apc", "clusters")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return []string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list APC clusters: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !dnsLabel.MatchString(entry.Name()) {
			continue
		}
		path, pathErr := ResolvedKubeconfigPath(entry.Name())
		if pathErr != nil {
			continue
		}
		if info, statErr := os.Stat(path); statErr == nil && info.Mode().IsRegular() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func AgentTokenPath(clusterName string) (string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(configDirectory, "apc", "clusters", clusterName, "agent-token"), nil
}

func agentConfigPath(clusterName string) (string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(configDirectory, "apc", "clusters", clusterName, "agent.json"), nil
}

func clusterConfigPath(clusterName string) (string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(configDirectory, "apc", "clusters", clusterName, "cluster.json"), nil
}

func currentClusterPath() (string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(configDirectory, "apc", "current-cluster"), nil
}

func ServerRunArguments(config Config) []string {
	config, _ = normalizeConfig(config)
	arguments := []string{
		"run", "--detach", "--name", ContainerName(config.Name),
		"--arch", "arm64", "--cpus", strconv.Itoa(config.CPUs), "--memory", config.Memory,
		"--cap-add", "ALL",
		"--network", "default,mac=" + DeterministicMAC(config.Name, "server") + ",mtu=1280",
		"--entrypoint", "/bin/sh",
		"--volume", ServerVolumeName(config.Name) + ":/var/lib/rancher/k3s",
		"--env", "APC_STABLE_NODE_IP=" + config.StableNodeIP,
		"--publish", fmt.Sprintf("%s:%d:%d/tcp", config.ListenAddress, config.APIPort, config.APIPort),
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster=" + config.Name,
		"--label", "apc.dev/role=server",
		"--label", "apc.dev/api-port=" + strconv.Itoa(config.APIPort),
		"--progress", "plain",
	}
	if config.AdvertiseAddress != "" {
		arguments = append(arguments,
			"--publish", fmt.Sprintf("%s:%d:8472/udp", config.ListenAddress, config.VXLANPort),
			"--publish", fmt.Sprintf("%s:%d:10250/tcp", config.ListenAddress, config.KubeletPort),
		)
	}
	arguments = append(arguments, config.Image,
		"-c", dynamicNodeIPScript, "apc-k3s",
		"server",
		"--https-listen-port", strconv.Itoa(config.APIPort),
		"--node-name", config.NodeName,
		"--write-kubeconfig-mode", "600",
		"--tls-san", config.APIAddress(),
		"--flannel-backend", "vxlan",
	)
	if !config.EnableNetworkPolicy {
		arguments = append(arguments, "--disable-network-policy")
	}
	if config.AdvertiseAddress != "" {
		arguments = append(arguments,
			"--node-external-ip", config.AdvertiseAddress,
			"--flannel-external-ip",
		)
	}
	if config.DisableTraefik {
		arguments = append(arguments, "--disable", "traefik", "--disable", "servicelb")
	}
	return arguments
}

func AgentRunArguments(config AgentConfig) []string {
	config, _ = normalizeAgentConfig(config)
	return []string{
		"run", "--detach", "--name", AgentContainerName(config.Name),
		"--arch", "arm64", "--cpus", strconv.Itoa(config.CPUs), "--memory", config.Memory,
		"--cap-add", "ALL",
		"--network", "default,mac=" + DeterministicMAC(config.Name, "agent") + ",mtu=1280",
		"--entrypoint", "/bin/sh",
		"--volume", AgentVolumeName(config.Name) + ":/var/lib/rancher/k3s",
		"--publish", fmt.Sprintf("%s:%d:8472/udp", config.ListenAddress, config.VXLANPort),
		"--publish", fmt.Sprintf("%s:%d:10250/tcp", config.ListenAddress, config.KubeletPort),
		"--mount", fmt.Sprintf("type=bind,source=%s,target=%s,readonly", filepath.Dir(config.TokenFile), agentTokenMountDir),
		"--env", "APC_PREVIOUS_NODE_IP=" + config.PreviousNodeIP,
		"--env", "APC_SERVER_URL=" + config.ServerURL,
		"--env", "APC_NODE_NAME=" + config.NodeName,
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster=" + config.Name,
		"--label", "apc.dev/role=agent",
		"--progress", "plain",
		config.Image,
		"-c", agentNodeIPScript, "apc-k3s",
		"agent",
		"--server", config.ServerURL,
		"--token-file", agentTokenMountPath,
		"--node-name", config.NodeName,
		"--node-external-ip", config.AdvertiseAddress,
	}
}

func normalizeConfig(config Config) (Config, error) {
	if config.Name == "" {
		config.Name = "spike"
	}
	if !dnsLabel.MatchString(config.Name) {
		return Config{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if config.NodeName == "" {
		config.NodeName = "apc-" + config.Name + "-server"
	}
	if !dnsLabel.MatchString(config.NodeName) {
		return Config{}, fmt.Errorf("node name must be a lowercase DNS label")
	}
	if config.Image == "" {
		config.Image = DefaultK3sImage
	}
	if config.CPUs == 0 {
		config.CPUs = 4
	}
	if config.CPUs < 2 {
		return Config{}, fmt.Errorf("K3s server requires at least 2 CPUs")
	}
	if config.Memory == "" {
		config.Memory = "4G"
	}
	if config.ListenAddress == "" {
		config.ListenAddress = "127.0.0.1"
	}
	if net.ParseIP(config.ListenAddress) == nil {
		return Config{}, fmt.Errorf("listen address must be an IP address")
	}
	if config.AdvertiseAddress != "" && net.ParseIP(config.AdvertiseAddress) == nil {
		return Config{}, fmt.Errorf("advertise address must be an IP address")
	}
	if config.StableNodeIP != "" && net.ParseIP(config.StableNodeIP) == nil {
		return Config{}, fmt.Errorf("stable node IP must be a valid IP address")
	}
	if config.ListenAddress == "0.0.0.0" && config.AdvertiseAddress == "" {
		return Config{}, fmt.Errorf("advertise address is required when listening on all interfaces")
	}
	if config.APIPort == 0 {
		config.APIPort = DefaultAPIPort
	}
	if config.VXLANPort == 0 {
		config.VXLANPort = DefaultVXLANPort
	}
	if config.KubeletPort == 0 {
		config.KubeletPort = DefaultKubeletPort
	}
	for name, port := range map[string]int{"API": config.APIPort, "VXLAN": config.VXLANPort, "kubelet": config.KubeletPort} {
		if port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("%s port must be between 1 and 65535", name)
		}
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 2 * time.Minute
	}
	if config.KubeconfigPath == "" {
		path, err := KubeconfigPath(config.Name)
		if err != nil {
			return Config{}, err
		}
		config.KubeconfigPath = path
	}
	return config, nil
}

func DeterministicMAC(clusterName, role string) string {
	digest := sha256.Sum256([]byte("apc/" + clusterName + "/" + role))
	digest[0] = (digest[0] | 0x02) & 0xfe
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", digest[0], digest[1], digest[2], digest[3], digest[4], digest[5])
}

func normalizeAgentConfig(config AgentConfig) (AgentConfig, error) {
	if config.Name == "" {
		config.Name = "lan-spike"
	}
	if !dnsLabel.MatchString(config.Name) {
		return AgentConfig{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if config.NodeName == "" {
		config.NodeName = "apc-" + config.Name + "-agent"
	}
	if !dnsLabel.MatchString(config.NodeName) {
		return AgentConfig{}, fmt.Errorf("node name must be a lowercase DNS label")
	}
	if config.Image == "" {
		config.Image = DefaultK3sImage
	}
	if config.CPUs == 0 {
		config.CPUs = 2
	}
	if config.CPUs < 1 {
		return AgentConfig{}, fmt.Errorf("K3s agent requires at least 1 CPU")
	}
	if config.Memory == "" {
		config.Memory = "2G"
	}
	if config.ListenAddress == "" {
		config.ListenAddress = "0.0.0.0"
	}
	if net.ParseIP(config.ListenAddress) == nil {
		return AgentConfig{}, fmt.Errorf("listen address must be an IP address")
	}
	if config.AdvertiseAddress == "" || net.ParseIP(config.AdvertiseAddress) == nil {
		return AgentConfig{}, fmt.Errorf("advertise address must be a valid LAN IP address")
	}
	if config.PreviousNodeIP != "" && net.ParseIP(config.PreviousNodeIP) == nil {
		return AgentConfig{}, fmt.Errorf("previous node IP must be a valid IP address")
	}
	if config.ServerURL == "" {
		return AgentConfig{}, fmt.Errorf("server URL is required")
	}
	serverURL, err := neturl.Parse(config.ServerURL)
	if err != nil || serverURL.Scheme != "https" || serverURL.Host == "" {
		return AgentConfig{}, fmt.Errorf("server URL must be an https URL")
	}
	if config.TokenFile == "" {
		return AgentConfig{}, fmt.Errorf("token file is required")
	}
	absoluteTokenPath, err := filepath.Abs(config.TokenFile)
	if err != nil {
		return AgentConfig{}, fmt.Errorf("resolve token file path: %w", err)
	}
	config.TokenFile = absoluteTokenPath
	if config.VXLANPort == 0 {
		config.VXLANPort = DefaultVXLANPort
	}
	if config.KubeletPort == 0 {
		config.KubeletPort = DefaultKubeletPort
	}
	for name, port := range map[string]int{"VXLAN": config.VXLANPort, "kubelet": config.KubeletPort} {
		if port < 1 || port > 65535 {
			return AgentConfig{}, fmt.Errorf("%s port must be between 1 and 65535", name)
		}
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 45 * time.Second
	}
	return config, nil
}

func (c Config) APIAddress() string {
	if c.AdvertiseAddress != "" {
		return c.AdvertiseAddress
	}
	return c.ListenAddress
}

func (c Config) APIEndpoint() string {
	return "https://" + net.JoinHostPort(c.APIAddress(), strconv.Itoa(c.APIPort))
}

type inspectRecord struct {
	Configuration struct {
		Labels         map[string]string `json:"labels"`
		PublishedPorts []struct {
			ContainerPort int    `json:"containerPort"`
			HostAddress   string `json:"hostAddress"`
			HostPort      int    `json:"hostPort"`
			Proto         string `json:"proto"`
		} `json:"publishedPorts"`
	} `json:"configuration"`
	Status struct {
		State    string `json:"state"`
		Networks []struct {
			IPv4Address string `json:"ipv4Address"`
		} `json:"networks"`
	} `json:"status"`
}

type volumeRecord struct {
	Configuration struct {
		Labels map[string]string `json:"labels"`
		Name   string            `json:"name"`
	} `json:"configuration"`
}

func (m *Manager) inspect(ctx context.Context, name string) (inspectRecord, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return inspectRecord{}, ErrNotFound
		}
		return inspectRecord{}, commandError("inspect K3s node", stderr, err)
	}
	var records []inspectRecord
	if err := json.Unmarshal(stdout, &records); err != nil {
		return inspectRecord{}, fmt.Errorf("decode container inspect output: %w", err)
	}
	if len(records) != 1 {
		return inspectRecord{}, fmt.Errorf("container inspect returned %d records", len(records))
	}
	return records[0], nil
}

func (m *Manager) ensureVolume(ctx context.Context, name, clusterName, role string) error {
	record, inspectErr := m.inspectVolume(ctx, name)
	if inspectErr == nil {
		if err := validateOwnedVolume(record, name, clusterName, role); err != nil {
			return err
		}
		return nil
	}
	if !errors.Is(inspectErr, ErrNotFound) {
		return inspectErr
	}
	_, stderr, err := m.runner.Run(ctx, m.binary,
		"volume", "create",
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster="+clusterName,
		"--label", "apc.dev/role="+role,
		"-s", defaultVolumeSize,
		name,
	)
	if err != nil {
		return commandError("create K3s data volume", stderr, err)
	}
	return nil
}

func (m *Manager) deleteStoppedContainer(ctx context.Context, name string) error {
	_, stderr, err := m.runner.Run(ctx, m.binary, "delete", name)
	if err != nil {
		return commandError("delete stopped K3s node", stderr, err)
	}
	return nil
}

func validateOwnedContainer(record inspectRecord, clusterName, role string) error {
	labels := record.Configuration.Labels
	if labels["apc.dev/managed"] != "true" || labels["apc.dev/cluster"] != clusterName || labels["apc.dev/role"] != role {
		containerName := ContainerName(clusterName)
		switch role {
		case "agent":
			containerName = AgentContainerName(clusterName)
		case "backup":
			containerName = BackupContainerName(clusterName)
		}
		return fmt.Errorf("container %q exists but is not the expected APC %s node", containerName, role)
	}
	return nil
}

func (m *Manager) waitReady(ctx context.Context, containerName, nodeName, expectedNodeIP string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		arguments := []string{"exec", containerName, "kubectl", "get", "nodes", "-o", "jsonpath={.items[0].status.conditions[?(@.type==\"Ready\")].status};{.items[0].status.addresses[?(@.type==\"InternalIP\")].address}"}
		if nodeName != "" {
			arguments = []string{"exec", containerName, "kubectl", "get", "node", nodeName, "-o", "jsonpath={.status.conditions[?(@.type==\"Ready\")].status};{.status.addresses[?(@.type==\"InternalIP\")].address}"}
		}
		stdout, _, err := m.runner.Run(waitCtx, m.binary, arguments...)
		if err == nil {
			parts := strings.Split(strings.TrimSpace(string(stdout)), ";")
			if len(parts) == 2 && parts[0] == "True" {
				if expectedNodeIP != "" {
					if parts[1] == expectedNodeIP {
						return nil
					}
				} else {
					record, inspectErr := m.inspect(waitCtx, containerName)
					if inspectErr == nil && len(record.Status.Networks) > 0 {
						currentIP := strings.Split(record.Status.Networks[0].IPv4Address, "/")[0]
						if parts[1] == currentIP {
							return nil
						}
					}
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("K3s node did not become Ready within %s: %w", timeout, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) waitAgentConnected(ctx context.Context, containerName string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		record, inspectErr := m.inspect(waitCtx, containerName)
		if inspectErr == nil && strings.EqualFold(record.Status.State, "stopped") {
			stdout, stderr, _ := m.runner.Run(waitCtx, m.binary, "logs", containerName)
			return fmt.Errorf("K3s agent stopped during bootstrap: %s", tail(string(stdout)+string(stderr), 12))
		}
		if inspectErr == nil && strings.EqualFold(record.Status.State, "running") {
			stdout, stderr, logsErr := m.runner.Run(waitCtx, m.binary, "logs", containerName)
			logs := currentAgentBootLogs(string(stdout) + string(stderr))
			if strings.Contains(logs, "Node password rejected") {
				return fmt.Errorf("K3s rejected the agent identity for a duplicate node name; delete the stale Kubernetes Node and retry the join")
			}
			if logsErr == nil && (strings.Contains(logs, "Node controller sync successful") || strings.Contains(logs, "Running flannel backend.")) {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("K3s agent did not connect within %s: %w", timeout, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) readKubeconfig(ctx context.Context, containerName, endpoint string) ([]byte, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "exec", containerName, "/bin/cat", "/etc/rancher/k3s/k3s.yaml")
	if err != nil {
		return nil, commandError("read kubeconfig", stderr, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(stdout, &document); err != nil {
		return nil, fmt.Errorf("decode kubeconfig: %w", err)
	}
	if !replaceScalar(&document, "server", endpoint) {
		return nil, fmt.Errorf("kubeconfig does not contain a server endpoint")
	}
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return nil, fmt.Errorf("encode kubeconfig: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("close kubeconfig encoder: %w", err)
	}
	return output.Bytes(), nil
}

func replaceScalar(node *yaml.Node, key, value string) bool {
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Value == key && node.Content[index+1].Kind == yaml.ScalarNode {
				node.Content[index+1].Value = value
				return true
			}
		}
	}
	for _, child := range node.Content {
		if replaceScalar(child, key, value) {
			return true
		}
	}
	return false
}

func writePrivateFile(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create private file directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure private file directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write private file: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure private file: %w", err)
	}
	return nil
}

func saveClusterConfig(config Config) error {
	path, err := clusterConfigPath(config.Name)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("encode cluster configuration: %w", err)
	}
	data = append(data, '\n')
	if err := writePrivateFile(path, data); err != nil {
		return fmt.Errorf("save cluster configuration: %w", err)
	}
	return nil
}

func saveAgentConfig(config AgentConfig) error {
	path, err := agentConfigPath(config.Name)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("encode agent configuration: %w", err)
	}
	data = append(data, '\n')
	if err := writePrivateFile(path, data); err != nil {
		return fmt.Errorf("save agent configuration: %w", err)
	}
	return nil
}

func loadClusterConfig(name string) (Config, error) {
	path, err := clusterConfigPath(name)
	if err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read cluster configuration: %w", err)
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("decode cluster configuration: %w", err)
	}
	config, err = normalizeConfig(config)
	if err != nil {
		return Config{}, fmt.Errorf("validate cluster configuration: %w", err)
	}
	return config, nil
}

func loadAgentConfig(name string) (AgentConfig, error) {
	path, err := agentConfigPath(name)
	if err != nil {
		return AgentConfig{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return AgentConfig{}, fmt.Errorf("read agent configuration: %w", err)
	}
	var config AgentConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return AgentConfig{}, fmt.Errorf("decode agent configuration: %w", err)
	}
	config, err = normalizeAgentConfig(config)
	if err != nil {
		return AgentConfig{}, fmt.Errorf("validate agent configuration: %w", err)
	}
	return config, nil
}

func sameAgentRuntimeConfig(left, right AgentConfig) bool {
	return left.Name == right.Name &&
		left.NodeName == right.NodeName &&
		left.ServerURL == right.ServerURL &&
		left.TokenFile == right.TokenFile &&
		left.Image == right.Image &&
		left.CPUs == right.CPUs &&
		left.Memory == right.Memory &&
		left.ListenAddress == right.ListenAddress &&
		left.AdvertiseAddress == right.AdvertiseAddress &&
		left.VXLANPort == right.VXLANPort &&
		left.KubeletPort == right.KubeletPort
}

func validatePrivateTokenFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("token file must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("token file permissions must be 0600 or stricter")
	}
	directoryInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("read token directory: %w", err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("token directory permissions must be 0700 or stricter")
	}
	return nil
}

func tail(value string, lines int) string {
	parts := strings.Split(strings.TrimSpace(value), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}

func currentAgentBootLogs(value string) string {
	const marker = "Starting k3s agent"
	if index := strings.LastIndex(value, marker); index >= 0 {
		return value[index:]
	}
	return value
}

func apiEndpointFromRecord(record inspectRecord) string {
	apiPort, _ := strconv.Atoi(record.Configuration.Labels["apc.dev/api-port"])
	for _, port := range record.Configuration.PublishedPorts {
		if (port.ContainerPort == apiPort || (apiPort == 0 && port.ContainerPort == 6443)) && strings.EqualFold(port.Proto, "tcp") {
			address := port.HostAddress
			if address == "0.0.0.0" || address == "::" || address == "" {
				address = "127.0.0.1"
			}
			return "https://" + net.JoinHostPort(address, strconv.Itoa(port.HostPort))
		}
	}
	return ""
}

func commandError(operation string, stderr []byte, err error) error {
	detail := strings.TrimSpace(string(stderr))
	if detail == "" {
		detail = err.Error()
	}
	return fmt.Errorf("%s: %s", operation, detail)
}

func isNotFound(stderr []byte) bool {
	value := strings.ToLower(string(stderr))
	return strings.Contains(value, "not found") || strings.Contains(value, "does not exist")
}
