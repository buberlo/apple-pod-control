package firewall

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

var (
	safeCluster   = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	safeInterface = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,14}$`)
	pfToken       = regexp.MustCompile(`(?m)Token\s*:\s*([0-9]+)`)
)

type Config struct {
	Cluster     string
	Role        string
	Interface   string
	LocalIP     string
	Peers       []string
	APIPort     int
	VXLANPort   int
	KubeletPort int
}

func Render(config Config) ([]byte, error) {
	config, err := normalize(config)
	if err != nil {
		return nil, err
	}
	table := tableName(config.Cluster)
	tcpPorts := []int{config.KubeletPort}
	if config.Role == "server" {
		tcpPorts = append([]int{config.APIPort}, tcpPorts...)
	}
	peerValues := strings.Join(config.Peers, ", ")
	portValues := make([]string, 0, len(tcpPorts))
	for _, port := range tcpPorts {
		portValues = append(portValues, strconv.Itoa(port))
	}
	var output strings.Builder
	fmt.Fprintf(&output, "# APC managed rules for %s (%s)\n", config.Cluster, config.Role)
	fmt.Fprintf(&output, "table <%s> const { %s }\n", table, peerValues)
	fmt.Fprintf(&output, "pass in quick on %s inet proto tcp from <%s> to %s port { %s } flags S/SA keep state\n", config.Interface, table, config.LocalIP, strings.Join(portValues, ", "))
	fmt.Fprintf(&output, "pass in quick on %s inet proto udp from <%s> to %s port %d keep state\n", config.Interface, table, config.LocalIP, config.VXLANPort)
	fmt.Fprintf(&output, "block in quick log on %s inet proto tcp from any to %s port { %s }\n", config.Interface, config.LocalIP, strings.Join(portValues, ", "))
	fmt.Fprintf(&output, "block in quick log on %s inet proto udp from any to %s port %d\n", config.Interface, config.LocalIP, config.VXLANPort)
	return []byte(output.String()), nil
}

func Validate(ctx context.Context, rules []byte) error {
	command := exec.CommandContext(ctx, "/sbin/pfctl", "-vnf", "-")
	command.Stdin = bytes.NewReader(rules)
	var stderr bytes.Buffer
	command.Stdout = &stderr
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return commandError("validate PF rules", stderr.Bytes(), err)
	}
	return nil
}

func Apply(ctx context.Context, config Config) error {
	rules, err := Render(config)
	if err != nil {
		return err
	}
	if err := Validate(ctx, rules); err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("loading PF rules requires root; rerun this exact command with sudo")
	}
	config, _ = normalize(config)
	return withClusterLock(config.Cluster, func() error {
		if err := runPF(ctx, rules, "-a", anchorName(config.Cluster), "-f", "-"); err != nil {
			return err
		}
		if err := acquirePFReference(ctx, config.Cluster); err != nil {
			_ = runPF(ctx, nil, "-a", anchorName(config.Cluster), "-F", "rules")
			return err
		}
		return nil
	})
}

func Remove(ctx context.Context, cluster string) error {
	if !safeCluster.MatchString(cluster) {
		return fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("removing PF rules requires root; rerun this exact command with sudo")
	}
	return withClusterLock(cluster, func() error {
		if err := runPF(ctx, nil, "-a", anchorName(cluster), "-F", "rules"); err != nil {
			return err
		}
		return releasePFReference(ctx, cluster)
	})
}

func anchorName(cluster string) string {
	return "com.apple/apc/" + cluster
}

func tableName(cluster string) string {
	digest := sha256.Sum256([]byte("apc/firewall/" + cluster))
	return "apc_" + hex.EncodeToString(digest[:6]) + "_peers"
}

func normalize(config Config) (Config, error) {
	if !safeCluster.MatchString(config.Cluster) {
		return Config{}, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if config.Role != "server" && config.Role != "agent" {
		return Config{}, fmt.Errorf("role must be server or agent")
	}
	if config.Interface == "" {
		config.Interface = "en0"
	}
	if !safeInterface.MatchString(config.Interface) {
		return Config{}, fmt.Errorf("network interface contains unsupported characters")
	}
	localIP := net.ParseIP(config.LocalIP)
	if localIP == nil || localIP.To4() == nil {
		return Config{}, fmt.Errorf("local IP must be a valid IPv4 address")
	}
	config.LocalIP = localIP.String()
	if len(config.Peers) == 0 {
		return Config{}, fmt.Errorf("at least one peer IPv4 address is required")
	}
	peers := make(map[string]struct{}, len(config.Peers))
	for _, value := range config.Peers {
		peer := net.ParseIP(value)
		if peer == nil || peer.To4() == nil {
			return Config{}, fmt.Errorf("peer %q is not a valid IPv4 address", value)
		}
		if peer.Equal(localIP) {
			return Config{}, fmt.Errorf("peer IP must differ from the local IP")
		}
		peers[peer.String()] = struct{}{}
	}
	config.Peers = config.Peers[:0]
	for peer := range peers {
		config.Peers = append(config.Peers, peer)
	}
	sort.Strings(config.Peers)
	if config.APIPort == 0 {
		config.APIPort = 16443
	}
	if config.VXLANPort == 0 {
		config.VXLANPort = 8472
	}
	if config.KubeletPort == 0 {
		config.KubeletPort = 10250
	}
	for name, port := range map[string]int{"API": config.APIPort, "VXLAN": config.VXLANPort, "kubelet": config.KubeletPort} {
		if port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("%s port must be between 1 and 65535", name)
		}
	}
	return config, nil
}

func runPF(ctx context.Context, stdin []byte, arguments ...string) error {
	command := exec.CommandContext(ctx, "/sbin/pfctl", arguments...)
	command.Stdin = bytes.NewReader(stdin)
	var stderr bytes.Buffer
	command.Stdout = &stderr
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return commandError("configure PF", stderr.Bytes(), err)
	}
	return nil
}

func acquirePFReference(ctx context.Context, cluster string) error {
	tokenPath := tokenPath(cluster)
	if token, err := os.ReadFile(tokenPath); err == nil && safeToken(string(token)) {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read PF reference token: %w", err)
	}
	command := exec.CommandContext(ctx, "/sbin/pfctl", "-E")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		return commandError("enable PF", output.Bytes(), err)
	}
	match := pfToken.FindSubmatch(output.Bytes())
	if len(match) != 2 {
		return fmt.Errorf("enable PF: pfctl did not return a reference token")
	}
	if err := writeToken(tokenPath, match[1]); err != nil {
		_ = runPF(ctx, nil, "-X", string(match[1]))
		return err
	}
	return nil
}

func releasePFReference(ctx context.Context, cluster string) error {
	tokenPath := tokenPath(cluster)
	token, err := os.ReadFile(tokenPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read PF reference token: %w", err)
	}
	if !safeToken(string(token)) {
		return fmt.Errorf("PF reference token is invalid; refusing to pass it to pfctl")
	}
	if err := runPF(ctx, nil, "-X", strings.TrimSpace(string(token))); err != nil {
		return err
	}
	if err := os.Remove(tokenPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove PF reference token: %w", err)
	}
	return nil
}

func tokenPath(cluster string) string {
	return filepath.Join("/var/run", "apc-firewall-"+cluster+".token")
}

func withClusterLock(cluster string, action func() error) error {
	path := filepath.Join("/var/run", "apc-firewall-"+cluster+".lock")
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open PF cluster lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock PF cluster state: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck -- closing the descriptor also releases the lock
	return action()
}

func safeToken(token string) bool {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return false
	}
	for _, character := range trimmed {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func writeToken(path string, token []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".apc-pf-token-*.tmp")
	if err != nil {
		return fmt.Errorf("create PF reference token: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(append(token, '\n')); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write PF reference token: %w", err)
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect PF reference token: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close PF reference token: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish PF reference token: %w", err)
	}
	return nil
}

func commandError(operation string, stderr []byte, err error) error {
	detail := strings.TrimSpace(string(stderr))
	if detail == "" && err != nil {
		detail = err.Error()
	}
	return fmt.Errorf("%s: %s", operation, detail)
}
