package cluster

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	haMemberCount             = 3
	haDefaultSubnet           = "192.168.96.0/24"
	haDefaultStableIPStart    = "192.168.96.241"
	haDefaultAPIPortBase      = 17443
	haTokenFilename           = "server-token"
	haTokenMountPath          = "/run/secrets/apc/server-token"
	haRuntimeOperationTimeout = 30 * time.Second
)

// HAConfig describes one local, three-server K3s embedded-etcd cluster. It
// stores only the path to the server token; the token value is never serialized.
type HAConfig struct {
	Name           string        `json:"name" yaml:"name"`
	NetworkName    string        `json:"networkName" yaml:"networkName"`
	Subnet         string        `json:"subnet" yaml:"subnet"`
	Image          string        `json:"image" yaml:"image"`
	ListenAddress  string        `json:"listenAddress" yaml:"listenAddress"`
	CPUs           int           `json:"cpus" yaml:"cpus"`
	Memory         string        `json:"memory" yaml:"memory"`
	VolumeSize     string        `json:"volumeSize" yaml:"volumeSize"`
	StartupTimeout time.Duration `json:"startupTimeout" yaml:"startupTimeout"`
	KubeconfigPath string        `json:"kubeconfigPath" yaml:"kubeconfigPath"`
	TokenFile      string        `json:"tokenFile" yaml:"tokenFile"`
	DisableTraefik bool          `json:"disableTraefik" yaml:"disableTraefik"`
	Members        []HAMember    `json:"members" yaml:"members"`
}

type HAMember struct {
	ID          int    `json:"id" yaml:"id"`
	NodeName    string `json:"nodeName" yaml:"nodeName"`
	StableIP    string `json:"stableIP" yaml:"stableIP"`
	MAC         string `json:"mac" yaml:"mac"`
	HostAPIPort int    `json:"hostAPIPort" yaml:"hostAPIPort"`
}

type HAMemberState struct {
	ID           int    `json:"id" yaml:"id"`
	NodeName     string `json:"nodeName" yaml:"nodeName"`
	Container    string `json:"container" yaml:"container"`
	RuntimeState string `json:"runtimeState" yaml:"runtimeState"`
	StableIP     string `json:"stableIP" yaml:"stableIP"`
	VMAddress    string `json:"vmAddress,omitempty" yaml:"vmAddress,omitempty"`
	APIEndpoint  string `json:"apiEndpoint" yaml:"apiEndpoint"`
	APIReady     bool   `json:"apiReady" yaml:"apiReady"`
	NodeReady    bool   `json:"nodeReady" yaml:"nodeReady"`
	K3sVersion   string `json:"k3sVersion,omitempty" yaml:"k3sVersion,omitempty"`
}

type HAState struct {
	Name         string          `json:"name" yaml:"name"`
	NetworkName  string          `json:"networkName" yaml:"networkName"`
	Subnet       string          `json:"subnet" yaml:"subnet"`
	Kubeconfig   string          `json:"kubeconfig,omitempty" yaml:"kubeconfig,omitempty"`
	Quorum       int             `json:"quorum" yaml:"quorum"`
	ReadyMembers int             `json:"readyMembers" yaml:"readyMembers"`
	Healthy      bool            `json:"healthy" yaml:"healthy"`
	Members      []HAMemberState `json:"members" yaml:"members"`
}

func DefaultHAConfig(name string) (HAConfig, error) {
	if name == "" {
		name = "ha-lab"
	}
	if !dnsLabel.MatchString(name) {
		return HAConfig{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	kubeconfig, err := KubeconfigPath(name)
	if err != nil {
		return HAConfig{}, err
	}
	tokenFile, err := haTokenPath(name)
	if err != nil {
		return HAConfig{}, err
	}
	address, _ := netip.ParseAddr(haDefaultStableIPStart)
	members := make([]HAMember, 0, haMemberCount)
	for id := 1; id <= haMemberCount; id++ {
		members = append(members, HAMember{
			ID:          id,
			NodeName:    defaultHANodeName(name, id),
			StableIP:    address.String(),
			MAC:         fmt.Sprintf("02:ac:96:00:00:%02x", id),
			HostAPIPort: haDefaultAPIPortBase + id - 1,
		})
		address = address.Next()
	}
	return HAConfig{
		Name:           name,
		NetworkName:    HANetworkName(name),
		Subnet:         haDefaultSubnet,
		Image:          DefaultK3sImage,
		ListenAddress:  "127.0.0.1",
		CPUs:           2,
		Memory:         "2G",
		VolumeSize:     "8G",
		StartupTimeout: 3 * time.Minute,
		KubeconfigPath: kubeconfig,
		TokenFile:      tokenFile,
		DisableTraefik: true,
		Members:        members,
	}, nil
}

func defaultHANodeName(name string, memberID int) string {
	// Preserve the names used by the manually bootstrapped validation cluster.
	if name == "ha-lab" {
		return fmt.Sprintf("apc-ha-%d", memberID)
	}
	return fmt.Sprintf("apc-%s-%d", name, memberID)
}

func HAContainerName(name string, memberID int) string {
	return fmt.Sprintf("apc-k3s-%s-server-%d", name, memberID)
}

func HAVolumeName(name string, memberID int) string {
	return HAContainerName(name, memberID) + "-data"
}

func HANetworkName(name string) string {
	suffix := strings.TrimPrefix(name, "ha-")
	return "apc-ha-" + suffix
}

func HAConfigPath(name string) (string, error) {
	if !dnsLabel.MatchString(name) {
		return "", fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user configuration directory: %w", err)
	}
	return filepath.Join(root, "apc", "clusters", name, "ha.json"), nil
}

func haTokenPath(name string) (string, error) {
	configPath, err := HAConfigPath(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(configPath), haTokenFilename), nil
}

func normalizeHAConfig(config HAConfig) (HAConfig, error) {
	defaults, err := DefaultHAConfig(config.Name)
	if err != nil {
		return HAConfig{}, err
	}
	if config.Name == "" {
		config.Name = defaults.Name
	}
	if config.NetworkName == "" {
		config.NetworkName = defaults.NetworkName
	}
	if !dnsLabel.MatchString(config.NetworkName) {
		return HAConfig{}, fmt.Errorf("HA network name must be a lowercase DNS label")
	}
	if config.Subnet == "" {
		config.Subnet = defaults.Subnet
	}
	prefix, err := netip.ParsePrefix(config.Subnet)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 30 {
		return HAConfig{}, fmt.Errorf("HA subnet must be a usable IPv4 prefix")
	}
	prefix = prefix.Masked()
	config.Subnet = prefix.String()
	if config.Image == "" {
		config.Image = defaults.Image
	}
	if config.ListenAddress == "" {
		config.ListenAddress = defaults.ListenAddress
	}
	if net.ParseIP(config.ListenAddress) == nil {
		return HAConfig{}, fmt.Errorf("HA listen address must be an IP address")
	}
	if config.CPUs == 0 {
		config.CPUs = defaults.CPUs
	}
	if config.CPUs < 2 {
		return HAConfig{}, fmt.Errorf("each K3s HA server requires at least 2 CPUs")
	}
	if config.Memory == "" {
		config.Memory = defaults.Memory
	}
	if _, err := parseHAByteSize(config.Memory); err != nil {
		return HAConfig{}, fmt.Errorf("invalid HA memory size: %w", err)
	}
	if config.VolumeSize == "" {
		config.VolumeSize = defaults.VolumeSize
	}
	if _, err := parseHAByteSize(config.VolumeSize); err != nil {
		return HAConfig{}, fmt.Errorf("invalid HA volume size: %w", err)
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = defaults.StartupTimeout
	}
	if config.StartupTimeout < time.Second {
		return HAConfig{}, fmt.Errorf("HA startup timeout must be at least 1s")
	}
	if config.KubeconfigPath == "" {
		config.KubeconfigPath = defaults.KubeconfigPath
	}
	if config.TokenFile == "" {
		config.TokenFile = defaults.TokenFile
	}
	config.KubeconfigPath, err = filepath.Abs(config.KubeconfigPath)
	if err != nil {
		return HAConfig{}, fmt.Errorf("resolve HA kubeconfig path: %w", err)
	}
	config.TokenFile, err = filepath.Abs(config.TokenFile)
	if err != nil {
		return HAConfig{}, fmt.Errorf("resolve HA token path: %w", err)
	}
	if filepath.Base(config.TokenFile) != haTokenFilename {
		return HAConfig{}, fmt.Errorf("HA token file must be named %q", haTokenFilename)
	}
	configPath, err := HAConfigPath(config.Name)
	if err != nil {
		return HAConfig{}, err
	}
	paths := []string{filepath.Clean(configPath), filepath.Clean(config.KubeconfigPath), filepath.Clean(config.TokenFile)}
	if paths[0] == paths[1] || paths[0] == paths[2] || paths[1] == paths[2] {
		return HAConfig{}, fmt.Errorf("HA config, kubeconfig and token paths must be pairwise distinct")
	}
	if len(config.Members) == 0 {
		config.Members = defaults.Members
	}
	if len(config.Members) != haMemberCount {
		return HAConfig{}, fmt.Errorf("embedded-etcd HA requires exactly three servers in this release")
	}
	config.Members = append([]HAMember(nil), config.Members...)
	sort.Slice(config.Members, func(i, j int) bool { return config.Members[i].ID < config.Members[j].ID })
	seenIDs := map[int]bool{}
	seenNodes := map[string]bool{}
	seenIPs := map[netip.Addr]bool{}
	seenMACs := map[string]bool{}
	seenPorts := map[int]bool{}
	lastAddress := lastIPv4Address(prefix)
	for index := range config.Members {
		member := &config.Members[index]
		if member.ID < 1 || member.ID > haMemberCount || seenIDs[member.ID] {
			return HAConfig{}, fmt.Errorf("HA member IDs must be unique values 1, 2, and 3")
		}
		seenIDs[member.ID] = true
		if member.NodeName == "" {
			member.NodeName = defaultHANodeName(config.Name, member.ID)
		}
		if !dnsLabel.MatchString(member.NodeName) || seenNodes[member.NodeName] {
			return HAConfig{}, fmt.Errorf("HA node names must be unique lowercase DNS labels")
		}
		seenNodes[member.NodeName] = true
		address, parseErr := netip.ParseAddr(member.StableIP)
		if parseErr != nil || !address.Is4() || !prefix.Contains(address) || address == prefix.Addr() || address == prefix.Addr().Next() || address == lastAddress || seenIPs[address] {
			return HAConfig{}, fmt.Errorf("HA member %d stable IP must be a unique usable address in %s", member.ID, prefix)
		}
		seenIPs[address] = true
		parsedMAC, parseErr := net.ParseMAC(member.MAC)
		normalizedMAC := strings.ToLower(member.MAC)
		if parseErr != nil || len(parsedMAC) != 6 || parsedMAC[0]&0x01 != 0 || parsedMAC[0]&0x02 == 0 || seenMACs[normalizedMAC] {
			return HAConfig{}, fmt.Errorf("HA member %d MAC must be a unique locally administered unicast address", member.ID)
		}
		member.MAC = normalizedMAC
		seenMACs[normalizedMAC] = true
		if member.HostAPIPort < 1 || member.HostAPIPort > 65535 || seenPorts[member.HostAPIPort] {
			return HAConfig{}, fmt.Errorf("HA member API ports must be unique values between 1 and 65535")
		}
		seenPorts[member.HostAPIPort] = true
	}
	return config, nil
}

func lastIPv4Address(prefix netip.Prefix) netip.Addr {
	address := prefix.Masked().Addr().As4()
	hostBits := 32 - prefix.Bits()
	value := uint32(address[0])<<24 | uint32(address[1])<<16 | uint32(address[2])<<8 | uint32(address[3])
	value |= uint32(1<<hostBits) - 1
	return netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
}

func parseHAByteSize(value string) (uint64, error) {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0, fmt.Errorf("size is empty")
	}
	multiplier := uint64(1)
	if suffix := value[len(value)-1]; suffix < '0' || suffix > '9' {
		switch suffix {
		case 'K':
			multiplier = 1 << 10
		case 'M':
			multiplier = 1 << 20
		case 'G':
			multiplier = 1 << 30
		case 'T':
			multiplier = 1 << 40
		case 'P':
			multiplier = 1 << 50
		default:
			return 0, fmt.Errorf("unsupported size suffix %q", suffix)
		}
		value = value[:len(value)-1]
	}
	number, err := strconv.ParseUint(value, 10, 64)
	if err != nil || number == 0 {
		return 0, fmt.Errorf("size must be a positive integer")
	}
	if number > ^uint64(0)/multiplier {
		return 0, fmt.Errorf("size overflows")
	}
	return number * multiplier, nil
}

func HAServerRunArguments(config HAConfig, member HAMember) []string {
	config, _ = normalizeHAConfig(config)
	member = memberByID(config.Members, member.ID)
	arguments := []string{
		"run", "--detach", "--name", HAContainerName(config.Name, member.ID),
		"--arch", "arm64", "--cpus", strconv.Itoa(config.CPUs), "--memory", config.Memory,
		"--cap-add", "ALL",
		"--network", fmt.Sprintf("%s,mac=%s,mtu=1280", config.NetworkName, member.MAC),
		"--entrypoint", "/bin/sh",
		"--volume", HAVolumeName(config.Name, member.ID) + ":/var/lib/rancher/k3s",
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/run/secrets/apc,readonly", filepath.Dir(config.TokenFile)),
		"--publish", fmt.Sprintf("%s:%d:6443/tcp", config.ListenAddress, member.HostAPIPort),
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/cluster=" + config.Name,
		"--label", "apc.dev/role=server",
		"--label", "apc.dev/member=" + strconv.Itoa(member.ID),
		"--progress", "plain",
		config.Image,
	}
	return append(arguments, haInitArguments(config, member)...)
}

func haInitArguments(config HAConfig, member HAMember) []string {
	prefix, _ := netip.ParsePrefix(config.Subnet)
	script := fmt.Sprintf("ip address add %s/%d dev eth0 2>/dev/null || true; ip route replace %s dev eth0 src %s; exec /bin/k3s \"$@\"", member.StableIP, prefix.Bits(), config.Subnet, member.StableIP)
	arguments := []string{"-c", script, "apc-k3s", "server"}
	if member.ID == 1 {
		arguments = append(arguments, "--cluster-init")
	} else {
		seed := memberByID(config.Members, 1)
		arguments = append(arguments, "--server", "https://"+net.JoinHostPort(seed.StableIP, "6443"))
	}
	arguments = append(arguments,
		"--token-file", haTokenMountPath,
		"--node-name", member.NodeName,
		"--node-ip", member.StableIP,
		"--advertise-address", member.StableIP,
		"--write-kubeconfig-mode", "600",
		"--flannel-backend", "vxlan",
		"--tls-san", config.ListenAddress,
	)
	for _, peer := range config.Members {
		arguments = append(arguments, "--tls-san", peer.StableIP)
	}
	if config.DisableTraefik {
		arguments = append(arguments, "--disable", "traefik", "--disable", "servicelb")
	}
	return arguments
}

func memberByID(members []HAMember, id int) HAMember {
	for _, member := range members {
		if member.ID == id {
			return member
		}
	}
	return HAMember{ID: id}
}

func (member HAMember) apiEndpoint(listenAddress string) string {
	if listenAddress == "" || listenAddress == "0.0.0.0" || listenAddress == "::" {
		listenAddress = "127.0.0.1"
	}
	return "https://" + net.JoinHostPort(listenAddress, strconv.Itoa(member.HostAPIPort))
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

type haContainerInspect struct {
	Configuration struct {
		Labels map[string]string `json:"labels"`
		Image  struct {
			Reference string `json:"reference"`
		} `json:"image"`
		InitProcess struct {
			Arguments []string `json:"arguments"`
		} `json:"initProcess"`
		Networks []struct {
			Network string `json:"network"`
			Options struct {
				MACAddress string `json:"macAddress"`
				MTU        int    `json:"mtu"`
			} `json:"options"`
		} `json:"networks"`
		PublishedPorts []struct {
			ContainerPort int    `json:"containerPort"`
			HostAddress   string `json:"hostAddress"`
			HostPort      int    `json:"hostPort"`
			Proto         string `json:"proto"`
		} `json:"publishedPorts"`
		Mounts []struct {
			Destination string   `json:"destination"`
			Source      string   `json:"source"`
			Options     []string `json:"options"`
			Type        struct {
				Volume *struct {
					Name string `json:"name"`
				} `json:"volume"`
				VirtioFS *struct{} `json:"virtiofs"`
			} `json:"type"`
		} `json:"mounts"`
		Resources struct {
			CPUs          int   `json:"cpus"`
			MemoryInBytes int64 `json:"memoryInBytes"`
		} `json:"resources"`
	} `json:"configuration"`
	Status struct {
		State    string `json:"state"`
		Networks []struct {
			IPv4Address string `json:"ipv4Address"`
		} `json:"networks"`
	} `json:"status"`
}

type haVolumeInspect struct {
	Configuration struct {
		Name    string            `json:"name"`
		Labels  map[string]string `json:"labels"`
		Options map[string]string `json:"options"`
	} `json:"configuration"`
}

type haNetworkInspect struct {
	Configuration struct {
		Name       string            `json:"name"`
		IPv4Subnet string            `json:"ipv4Subnet"`
		Labels     map[string]string `json:"labels"`
	} `json:"configuration"`
}

func (m *Manager) inspectHAContainer(ctx context.Context, name string) (haContainerInspect, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return haContainerInspect{}, ErrNotFound
		}
		return haContainerInspect{}, commandError("inspect HA server", stderr, err)
	}
	var records []haContainerInspect
	if err := json.Unmarshal(stdout, &records); err != nil {
		return haContainerInspect{}, fmt.Errorf("decode HA container inspect output: %w", err)
	}
	if len(records) != 1 {
		return haContainerInspect{}, fmt.Errorf("HA container inspect returned %d records", len(records))
	}
	return records[0], nil
}

func (m *Manager) inspectHAVolume(ctx context.Context, name string) (haVolumeInspect, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "volume", "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return haVolumeInspect{}, ErrNotFound
		}
		return haVolumeInspect{}, commandError("inspect HA data volume", stderr, err)
	}
	var records []haVolumeInspect
	if err := json.Unmarshal(stdout, &records); err != nil {
		return haVolumeInspect{}, fmt.Errorf("decode HA volume inspect output: %w", err)
	}
	if len(records) != 1 {
		return haVolumeInspect{}, fmt.Errorf("HA volume inspect returned %d records", len(records))
	}
	return records[0], nil
}

func (m *Manager) inspectHANetwork(ctx context.Context, name string) (haNetworkInspect, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "network", "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return haNetworkInspect{}, ErrNotFound
		}
		return haNetworkInspect{}, commandError("inspect HA network", stderr, err)
	}
	var records []haNetworkInspect
	if err := json.Unmarshal(stdout, &records); err != nil {
		return haNetworkInspect{}, fmt.Errorf("decode HA network inspect output: %w", err)
	}
	if len(records) != 1 {
		return haNetworkInspect{}, fmt.Errorf("HA network inspect returned %d records", len(records))
	}
	return records[0], nil
}

func validateHALabels(labels map[string]string, clusterName, role string, memberID int) error {
	if labels["apc.dev/managed"] != "true" || labels["apc.dev/cluster"] != clusterName || labels["apc.dev/role"] != role {
		return fmt.Errorf("resource is not the expected APC %s for cluster %q", role, clusterName)
	}
	if memberID > 0 && labels["apc.dev/member"] != strconv.Itoa(memberID) {
		return fmt.Errorf("resource is not APC HA member %d for cluster %q", memberID, clusterName)
	}
	return nil
}

func validateHAContainer(record haContainerInspect, config HAConfig, member HAMember) error {
	if err := validateHALabels(record.Configuration.Labels, config.Name, "server", member.ID); err != nil {
		return fmt.Errorf("container %q: %w", HAContainerName(config.Name, member.ID), err)
	}
	if record.Configuration.Image.Reference != config.Image {
		return fmt.Errorf("container %q uses image %q, expected %q", HAContainerName(config.Name, member.ID), record.Configuration.Image.Reference, config.Image)
	}
	if !reflect.DeepEqual(record.Configuration.InitProcess.Arguments, haInitArguments(config, member)) {
		return fmt.Errorf("container %q does not match the declared K3s member identity", HAContainerName(config.Name, member.ID))
	}
	networkMatches := false
	for _, network := range record.Configuration.Networks {
		if network.Network == config.NetworkName && strings.EqualFold(network.Options.MACAddress, member.MAC) && network.Options.MTU == 1280 {
			networkMatches = true
			break
		}
	}
	if !networkMatches {
		return fmt.Errorf("container %q does not match network %q, MAC %s and MTU 1280", HAContainerName(config.Name, member.ID), config.NetworkName, member.MAC)
	}
	portMatches := false
	for _, port := range record.Configuration.PublishedPorts {
		if port.ContainerPort == 6443 && port.HostPort == member.HostAPIPort && port.HostAddress == config.ListenAddress && strings.EqualFold(port.Proto, "tcp") {
			portMatches = true
			break
		}
	}
	if !portMatches {
		return fmt.Errorf("container %q does not publish the declared API endpoint", HAContainerName(config.Name, member.ID))
	}
	volumeMatches := false
	tokenMountMatches := false
	for _, mount := range record.Configuration.Mounts {
		if mount.Destination == "/var/lib/rancher/k3s" && mount.Type.Volume != nil && mount.Type.Volume.Name == HAVolumeName(config.Name, member.ID) {
			volumeMatches = true
		}
		if mount.Destination == "/run/secrets/apc" && mount.Source == filepath.Dir(config.TokenFile) && mount.Type.VirtioFS != nil && containsString(mount.Options, "ro") {
			tokenMountMatches = true
		}
	}
	if !volumeMatches || !tokenMountMatches {
		return fmt.Errorf("container %q does not use the declared data volume and read-only token mount", HAContainerName(config.Name, member.ID))
	}
	memoryBytes, _ := parseHAByteSize(config.Memory)
	if record.Configuration.Resources.CPUs != config.CPUs || uint64(record.Configuration.Resources.MemoryInBytes) != memoryBytes {
		return fmt.Errorf("container %q does not match the declared CPU and memory resources", HAContainerName(config.Name, member.ID))
	}
	return nil
}

func validateHAVolume(record haVolumeInspect, config HAConfig, member HAMember) error {
	name := HAVolumeName(config.Name, member.ID)
	if record.Configuration.Name != "" && record.Configuration.Name != name {
		return fmt.Errorf("volume inspect returned unexpected volume %q", record.Configuration.Name)
	}
	if err := validateHALabels(record.Configuration.Labels, config.Name, "server", member.ID); err != nil {
		return fmt.Errorf("volume %q: %w", name, err)
	}
	if size := record.Configuration.Options["size"]; size != "" && !sameHAByteSize(size, config.VolumeSize) {
		return fmt.Errorf("volume %q has size %s, expected %s", name, size, config.VolumeSize)
	}
	return nil
}

func sameHAByteSize(left, right string) bool {
	leftBytes, leftErr := parseHAByteSize(left)
	rightBytes, rightErr := parseHAByteSize(right)
	return leftErr == nil && rightErr == nil && leftBytes == rightBytes
}

func validateHANetwork(record haNetworkInspect, config HAConfig) error {
	if record.Configuration.Name != "" && record.Configuration.Name != config.NetworkName {
		return fmt.Errorf("network inspect returned unexpected network %q", record.Configuration.Name)
	}
	labels := record.Configuration.Labels
	if labels["apc.dev/managed"] != "true" || labels["apc.dev/cluster"] != config.Name {
		return fmt.Errorf("network %q exists but is not owned by APC cluster %q", config.NetworkName, config.Name)
	}
	actual, actualErr := netip.ParsePrefix(record.Configuration.IPv4Subnet)
	expected, _ := netip.ParsePrefix(config.Subnet)
	if actualErr != nil || actual.Masked() != expected.Masked() {
		return fmt.Errorf("network %q uses subnet %q, expected %q", config.NetworkName, record.Configuration.IPv4Subnet, config.Subnet)
	}
	return nil
}

type haPreflight struct {
	networkExists   bool
	volumeExists    map[int]bool
	containerRecord map[int]haContainerInspect
}

func (m *Manager) preflightHA(ctx context.Context, config HAConfig, allowPartialVolumes bool) (haPreflight, error) {
	result := haPreflight{volumeExists: map[int]bool{}, containerRecord: map[int]haContainerInspect{}}
	network, err := m.inspectHANetwork(ctx, config.NetworkName)
	switch {
	case err == nil:
		if err := validateHANetwork(network, config); err != nil {
			return result, err
		}
		result.networkExists = true
	case errors.Is(err, ErrNotFound):
	default:
		return result, err
	}
	for _, member := range config.Members {
		volume, inspectErr := m.inspectHAVolume(ctx, HAVolumeName(config.Name, member.ID))
		switch {
		case inspectErr == nil:
			if err := validateHAVolume(volume, config, member); err != nil {
				return result, err
			}
			result.volumeExists[member.ID] = true
		case errors.Is(inspectErr, ErrNotFound):
		default:
			return result, inspectErr
		}
		containerRecord, inspectErr := m.inspectHAContainer(ctx, HAContainerName(config.Name, member.ID))
		switch {
		case inspectErr == nil:
			if err := validateHAContainer(containerRecord, config, member); err != nil {
				return result, err
			}
			result.containerRecord[member.ID] = containerRecord
		case errors.Is(inspectErr, ErrNotFound):
		default:
			return result, inspectErr
		}
	}
	existingVolumes := len(result.volumeExists)
	if !allowPartialVolumes && existingVolumes != 0 && existingVolumes != haMemberCount {
		return result, fmt.Errorf("refusing mixed HA state: found %d of 3 member volumes", existingVolumes)
	}
	return result, nil
}

func checkHALegacyCollision(name string) error {
	paths := make([]string, 0, 2)
	if path, err := clusterConfigPath(name); err == nil {
		paths = append(paths, path)
	} else {
		return err
	}
	if path, err := agentConfigPath(name); err == nil {
		paths = append(paths, path)
	} else {
		return err
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("refusing HA cluster %q because legacy APC state already exists at %s", name, path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect legacy APC state: %w", err)
		}
	}
	return nil
}

func (m *Manager) CreateHA(ctx context.Context, config HAConfig) (HAState, error) {
	config, err := normalizeHAConfig(config)
	if err != nil {
		return HAState{}, err
	}
	if err := checkHALegacyCollision(config.Name); err != nil {
		return HAState{}, err
	}
	if stored, loadErr := loadHAConfig(config.Name); loadErr == nil {
		if !sameHARuntimeConfig(stored, config) {
			return HAState{}, fmt.Errorf("saved HA topology for cluster %q differs from the requested configuration", config.Name)
		}
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return HAState{}, loadErr
	}
	preflight, err := m.preflightHA(ctx, config, false)
	if err != nil {
		return HAState{}, err
	}
	fresh := len(preflight.volumeExists) == 0
	if err := ensureHAToken(config.TokenFile, fresh); err != nil {
		return HAState{}, err
	}
	// Persist the exact desired topology before the first runtime mutation so a
	// partially failed bootstrap can still be inspected and safely deleted.
	if err := saveHAConfig(config); err != nil {
		return HAState{}, err
	}
	createdNetwork := false
	if !preflight.networkExists {
		if _, stderr, runErr := m.runner.Run(ctx, m.binary,
			"network", "create",
			"--subnet", config.Subnet,
			"--label", "apc.dev/managed=true",
			"--label", "apc.dev/cluster="+config.Name,
			config.NetworkName,
		); runErr != nil {
			return HAState{}, commandError("create HA network", stderr, runErr)
		}
		createdNetwork = true
	}
	createdVolumes := make([]int, 0, haMemberCount)
	for _, member := range config.Members {
		if preflight.volumeExists[member.ID] {
			continue
		}
		if _, stderr, runErr := m.runner.Run(ctx, m.binary,
			"volume", "create",
			"--label", "apc.dev/managed=true",
			"--label", "apc.dev/cluster="+config.Name,
			"--label", "apc.dev/role=server",
			"--label", "apc.dev/member="+strconv.Itoa(member.ID),
			"-s", config.VolumeSize,
			HAVolumeName(config.Name, member.ID),
		); runErr != nil {
			createErr := commandError("create HA member data volume", stderr, runErr)
			return HAState{}, errors.Join(createErr, m.rollbackHAFreshInfrastructure(ctx, config, createdVolumes, createdNetwork))
		}
		createdVolumes = append(createdVolumes, member.ID)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, config.StartupTimeout)
	defer cancelWait()
	if fresh {
		for _, member := range config.Members {
			if err := m.reconcileHAMember(waitCtx, config, member, preflight.containerRecord[member.ID]); err != nil {
				return HAState{}, err
			}
			if err := m.waitHAMemberReady(waitCtx, config, member, config.StartupTimeout); err != nil {
				return HAState{}, err
			}
		}
	} else {
		for _, member := range config.Members {
			if err := m.reconcileHAMember(waitCtx, config, member, preflight.containerRecord[member.ID]); err != nil {
				return HAState{}, err
			}
		}
		for _, member := range config.Members {
			if err := m.waitHAMemberReady(waitCtx, config, member, config.StartupTimeout); err != nil {
				return HAState{}, err
			}
		}
	}
	seed := memberByID(config.Members, 1)
	kubeconfig, err := m.readKubeconfig(ctx, HAContainerName(config.Name, seed.ID), seed.apiEndpoint(config.ListenAddress))
	if err != nil {
		return HAState{}, err
	}
	if err := writePrivateFileAtomic(config.KubeconfigPath, kubeconfig); err != nil {
		return HAState{}, fmt.Errorf("write HA kubeconfig: %w", err)
	}
	if err := SetCurrentCluster(config.Name); err != nil {
		return HAState{}, err
	}
	state, err := m.waitHAClusterReady(waitCtx, config)
	if err != nil {
		return HAState{}, err
	}
	state.Kubeconfig = config.KubeconfigPath
	return state, nil
}

func (m *Manager) rollbackHAFreshInfrastructure(ctx context.Context, config HAConfig, volumeIDs []int, networkCreated bool) error {
	var rollbackErrors []error
	for index := len(volumeIDs) - 1; index >= 0; index-- {
		name := HAVolumeName(config.Name, volumeIDs[index])
		if err := m.runHABounded(ctx, "roll back HA data volume "+name, "volume", "delete", name); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	if networkCreated {
		if err := m.runHABounded(ctx, "roll back HA network "+config.NetworkName, "network", "delete", config.NetworkName); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	return errors.Join(rollbackErrors...)
}

func (m *Manager) reconcileHAMember(ctx context.Context, config HAConfig, member HAMember, record haContainerInspect) error {
	if record.Configuration.Labels != nil {
		if strings.EqualFold(record.Status.State, "running") {
			return nil
		}
		if err := m.runHABounded(ctx, fmt.Sprintf("delete stopped HA server member %d", member.ID), "delete", HAContainerName(config.Name, member.ID)); err != nil {
			return err
		}
	}
	if _, stderr, err := m.runner.Run(ctx, m.binary, HAServerRunArguments(config, member)...); err != nil {
		return commandError("start HA server", stderr, err)
	}
	return nil
}

type haNodeDocument struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		NodeInfo struct {
			KubeletVersion string `json:"kubeletVersion"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

func (m *Manager) readHAMemberNode(ctx context.Context, config HAConfig, member HAMember) (haNodeDocument, error) {
	stdout, stderr, err := m.runner.Run(ctx, m.binary, "exec", HAContainerName(config.Name, member.ID), "kubectl", "get", "node", member.NodeName, "-o", "json")
	if err != nil {
		return haNodeDocument{}, commandError("read HA Kubernetes node", stderr, err)
	}
	var node haNodeDocument
	if err := json.Unmarshal(stdout, &node); err != nil {
		return haNodeDocument{}, fmt.Errorf("decode HA Kubernetes node: %w", err)
	}
	if node.Metadata.Name != member.NodeName {
		return haNodeDocument{}, fmt.Errorf("Kubernetes returned node %q for HA member %q", node.Metadata.Name, member.NodeName)
	}
	return node, nil
}

func nodeDocumentReady(node haNodeDocument) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			return true
		}
	}
	return false
}

func (m *Manager) waitHAMemberReady(ctx context.Context, config HAConfig, member HAMember, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		node, err := m.readHAMemberNode(waitCtx, config, member)
		if err == nil && nodeDocumentReady(node) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("HA member %d did not become Ready within %s: %w", member.ID, timeout, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) waitHAClusterReady(ctx context.Context, config HAConfig) (HAState, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastReady := 0
	for {
		state, err := m.StatusHA(ctx, config.Name)
		if err == nil {
			lastReady = state.ReadyMembers
			if state.ReadyMembers == haMemberCount {
				return state, nil
			}
		} else if ctx.Err() == nil {
			return HAState{}, err
		}
		select {
		case <-ctx.Done():
			return HAState{}, fmt.Errorf("HA cluster %q reached only %d of 3 Ready node/API pairs within %s: %w", config.Name, lastReady, config.StartupTimeout, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) StatusHA(ctx context.Context, name string) (HAState, error) {
	config, err := loadHAConfig(name)
	if err != nil {
		return HAState{}, err
	}
	state := HAState{
		Name:        config.Name,
		NetworkName: config.NetworkName,
		Subnet:      config.Subnet,
		Kubeconfig:  config.KubeconfigPath,
		Quorum:      len(config.Members)/2 + 1,
		Members:     make([]HAMemberState, 0, len(config.Members)),
	}
	for _, member := range config.Members {
		memberState := HAMemberState{
			ID:           member.ID,
			NodeName:     member.NodeName,
			Container:    HAContainerName(config.Name, member.ID),
			RuntimeState: "missing",
			StableIP:     member.StableIP,
			APIEndpoint:  member.apiEndpoint(config.ListenAddress),
		}
		record, inspectErr := m.inspectHAContainer(ctx, memberState.Container)
		if errors.Is(inspectErr, ErrNotFound) {
			state.Members = append(state.Members, memberState)
			continue
		}
		if inspectErr != nil {
			return HAState{}, inspectErr
		}
		if err := validateHAContainer(record, config, member); err != nil {
			return HAState{}, err
		}
		memberState.RuntimeState = record.Status.State
		if len(record.Status.Networks) > 0 {
			memberState.VMAddress = strings.Split(record.Status.Networks[0].IPv4Address, "/")[0]
		}
		if strings.EqualFold(record.Status.State, "running") {
			hostAPIReady := m.probeHAAPI(ctx, config, member)
			node, nodeErr := m.readHAMemberNode(ctx, config, member)
			if nodeErr == nil {
				memberState.APIReady = hostAPIReady
				memberState.NodeReady = nodeDocumentReady(node)
				memberState.K3sVersion = node.Status.NodeInfo.KubeletVersion
			}
		}
		if memberState.NodeReady && memberState.APIReady {
			state.ReadyMembers++
		}
		state.Members = append(state.Members, memberState)
	}
	state.Healthy = state.ReadyMembers >= state.Quorum
	return state, nil
}

func (m *Manager) probeHAHostAPI(ctx context.Context, config HAConfig, member HAMember) bool {
	dialAddress := net.JoinHostPort(config.ListenAddress, strconv.Itoa(member.HostAPIPort))
	if config.ListenAddress == "0.0.0.0" || config.ListenAddress == "::" {
		dialAddress = net.JoinHostPort("127.0.0.1", strconv.Itoa(member.HostAPIPort))
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, time.Second)
	dialErr := m.dialTCP(dialCtx, dialAddress)
	cancelDial()
	if dialErr != nil {
		return false
	}
	tlsConfig, err := loadHAClientTLSConfig(config.KubeconfigPath)
	if err != nil {
		return false
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, 3*time.Second)
	defer cancelProbe()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, member.apiEndpoint(config.ListenAddress)+"/readyz", nil)
	if err != nil {
		return false
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 128))
	return err == nil && response.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) == "ok"
}

func loadHAClientTLSConfig(path string) (*tls.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read HA kubeconfig credentials: %w", err)
	}
	var document struct {
		Clusters []struct {
			Cluster struct {
				CertificateAuthorityData string `yaml:"certificate-authority-data"`
			} `yaml:"cluster"`
		} `yaml:"clusters"`
		Users []struct {
			User struct {
				ClientCertificateData string `yaml:"client-certificate-data"`
				ClientKeyData         string `yaml:"client-key-data"`
			} `yaml:"user"`
		} `yaml:"users"`
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("decode HA kubeconfig credentials: %w", err)
	}
	if len(document.Clusters) == 0 || len(document.Users) == 0 {
		return nil, fmt.Errorf("HA kubeconfig does not contain cluster and user credentials")
	}
	caData, err := base64.StdEncoding.DecodeString(strings.TrimSpace(document.Clusters[0].Cluster.CertificateAuthorityData))
	if err != nil {
		return nil, fmt.Errorf("decode HA certificate authority: %w", err)
	}
	certificateData, err := base64.StdEncoding.DecodeString(strings.TrimSpace(document.Users[0].User.ClientCertificateData))
	if err != nil {
		return nil, fmt.Errorf("decode HA client certificate: %w", err)
	}
	keyData, err := base64.StdEncoding.DecodeString(strings.TrimSpace(document.Users[0].User.ClientKeyData))
	if err != nil {
		return nil, fmt.Errorf("decode HA client key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certificateData, keyData)
	if err != nil {
		return nil, fmt.Errorf("load HA client key pair: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("HA kubeconfig contains an invalid certificate authority")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{certificate},
	}, nil
}

// PrepareKubeconfig selects the first reachable, Ready HA API endpoint and
// refreshes the protected kubeconfig before a kubectl-compatible APC command.
// Legacy single-server clusters keep their existing resolution path.
func (m *Manager) PrepareKubeconfig(ctx context.Context, name string) (string, error) {
	config, err := loadHAConfig(name)
	if errors.Is(err, os.ErrNotExist) {
		return ResolvedKubeconfigPath(name)
	}
	if err != nil {
		return "", err
	}
	for _, member := range config.Members {
		record, inspectErr := m.inspectHAContainer(ctx, HAContainerName(config.Name, member.ID))
		if errors.Is(inspectErr, ErrNotFound) {
			continue
		}
		if inspectErr != nil {
			return "", inspectErr
		}
		if err := validateHAContainer(record, config, member); err != nil {
			return "", err
		}
		if !strings.EqualFold(record.Status.State, "running") {
			continue
		}
		node, nodeErr := m.readHAMemberNode(ctx, config, member)
		if nodeErr != nil || !nodeDocumentReady(node) {
			continue
		}
		kubeconfig, readErr := m.readKubeconfig(ctx, HAContainerName(config.Name, member.ID), member.apiEndpoint(config.ListenAddress))
		if readErr != nil {
			continue
		}
		if err := writePrivateFileAtomic(config.KubeconfigPath, kubeconfig); err != nil {
			return "", fmt.Errorf("refresh HA kubeconfig: %w", err)
		}
		if !m.probeHAAPI(ctx, config, member) {
			continue
		}
		return config.KubeconfigPath, nil
	}
	return "", fmt.Errorf("HA cluster %q has no reachable Ready API endpoint", name)
}

func (m *Manager) StartHA(ctx context.Context, name string, timeout time.Duration) (HAState, error) {
	config, err := loadHAConfig(name)
	if err != nil {
		return HAState{}, err
	}
	if timeout > 0 {
		config.StartupTimeout = timeout
	}
	return m.CreateHA(ctx, config)
}

func (m *Manager) StopHA(ctx context.Context, name string) error {
	config, err := loadHAConfig(name)
	if err != nil {
		return err
	}
	preflight, err := m.preflightHA(ctx, config, true)
	if err != nil {
		return err
	}
	for index := len(config.Members) - 1; index >= 0; index-- {
		member := config.Members[index]
		record, exists := preflight.containerRecord[member.ID]
		if !exists || strings.EqualFold(record.Status.State, "stopped") {
			continue
		}
		if err := m.runHABounded(ctx, fmt.Sprintf("stop HA server member %d", member.ID), "stop", HAContainerName(config.Name, member.ID)); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) DeleteHA(ctx context.Context, name string, keepData bool) error {
	config, err := loadHAConfig(name)
	if err != nil {
		return err
	}
	preflight, err := m.preflightHA(ctx, config, true)
	if err != nil {
		return err
	}
	for index := len(config.Members) - 1; index >= 0; index-- {
		member := config.Members[index]
		record, exists := preflight.containerRecord[member.ID]
		if !exists {
			continue
		}
		name := HAContainerName(config.Name, member.ID)
		if !strings.EqualFold(record.Status.State, "stopped") {
			if err := m.runHABounded(ctx, fmt.Sprintf("stop HA server member %d before deletion", member.ID), "stop", name); err != nil {
				return err
			}
		}
		if err := m.runHABounded(ctx, fmt.Sprintf("delete HA server member %d envelope", member.ID), "delete", name); err != nil {
			return err
		}
	}
	if keepData {
		return nil
	}
	for index := len(config.Members) - 1; index >= 0; index-- {
		member := config.Members[index]
		if !preflight.volumeExists[member.ID] {
			continue
		}
		if err := m.runHABounded(ctx, fmt.Sprintf("delete HA member %d data volume", member.ID), "volume", "delete", HAVolumeName(config.Name, member.ID)); err != nil {
			return err
		}
	}
	if preflight.networkExists {
		if err := m.runHABounded(ctx, "delete HA network "+config.NetworkName, "network", "delete", config.NetworkName); err != nil {
			return err
		}
	}
	configPath, err := HAConfigPath(config.Name)
	if err != nil {
		return err
	}
	if err := removeExactFiles([]string{configPath, config.KubeconfigPath, config.TokenFile}); err != nil {
		return err
	}
	if err := clearCurrentCluster(config.Name); err != nil {
		return err
	}
	return removeEmptyClusterDirectory(config.Name)
}

func (m *Manager) runHABounded(ctx context.Context, operation string, arguments ...string) error {
	operationCtx, cancel := context.WithTimeout(ctx, haRuntimeOperationTimeout)
	defer cancel()
	_, stderr, err := m.runner.Run(operationCtx, m.binary, arguments...)
	if err == nil {
		return nil
	}
	if errors.Is(operationCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%s timed out after %s: %w", operation, haRuntimeOperationTimeout, context.DeadlineExceeded)
	}
	return commandError(operation, stderr, err)
}

func ensureHAToken(path string, create bool) error {
	_, err := os.Stat(path)
	if err == nil {
		if validateErr := validatePrivateTokenFile(path); validateErr != nil {
			return fmt.Errorf("validate HA token file: %w", validateErr)
		}
		data, readErr := os.ReadFile(path)
		token := strings.TrimSpace(string(data))
		if readErr != nil || token == "" || strings.ContainsAny(token, " \t\r\n") {
			return fmt.Errorf("HA token file does not contain one valid token")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect HA token file: %w", err)
	}
	if !create {
		return fmt.Errorf("HA token file %q is missing for existing member data", path)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate HA server token: %w", err)
	}
	if err := writePrivateFileAtomic(path, []byte(hex.EncodeToString(secret)+"\n")); err != nil {
		return fmt.Errorf("write HA server token: %w", err)
	}
	return nil
}

func saveHAConfig(config HAConfig) error {
	path, err := HAConfigPath(config.Name)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("encode HA cluster configuration: %w", err)
	}
	if err := writePrivateFileAtomic(path, append(data, '\n')); err != nil {
		return fmt.Errorf("save HA cluster configuration: %w", err)
	}
	return nil
}

func loadHAConfig(name string) (HAConfig, error) {
	path, err := HAConfigPath(name)
	if err != nil {
		return HAConfig{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return HAConfig{}, fmt.Errorf("read HA cluster configuration: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return HAConfig{}, fmt.Errorf("HA cluster configuration must be a regular file with mode 0600 or stricter")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return HAConfig{}, fmt.Errorf("read HA cluster configuration: %w", err)
	}
	var config HAConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return HAConfig{}, fmt.Errorf("decode HA cluster configuration: %w", err)
	}
	config, err = normalizeHAConfig(config)
	if err != nil {
		return HAConfig{}, fmt.Errorf("validate HA cluster configuration: %w", err)
	}
	return config, nil
}

func sameHARuntimeConfig(left, right HAConfig) bool {
	left.StartupTimeout = 0
	right.StartupTimeout = 0
	return reflect.DeepEqual(left, right)
}

func writePrivateFileAtomic(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create private file directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure private file directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create private temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("secure private temporary file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("write private temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync private temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close private temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace private file: %w", err)
	}
	return nil
}
