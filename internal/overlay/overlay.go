package overlay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

const tailscaleAppCLI = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"

var routeInterface = regexp.MustCompile(`(?m)^\s*interface:\s*(\S+)\s*$`)

type Config struct {
	Provider  string
	Interface string
	LocalIP   string
	PeerIP    string
}

type Status struct {
	Provider     string `json:"provider" yaml:"provider"`
	BackendState string `json:"backendState" yaml:"backendState"`
	Interface    string `json:"interface" yaml:"interface"`
	LocalIP      string `json:"localIP" yaml:"localIP"`
	PeerIP       string `json:"peerIP" yaml:"peerIP"`
	PeerOnline   bool   `json:"peerOnline" yaml:"peerOnline"`
	CLI          string `json:"cli" yaml:"cli"`
}

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, binary string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Env = append(os.Environ(), "TAILSCALE_BE_CLI=1")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		detail := strings.TrimSpace(output.String())
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("run %s: %s", binary, detail)
	}
	return output.Bytes(), nil
}

type Checker struct {
	runner        commandRunner
	findCLI       func() (string, error)
	findInterface func(string) (string, error)
}

func NewChecker() *Checker {
	return &Checker{runner: execRunner{}, findCLI: findTailscaleCLI, findInterface: interfaceForIP}
}

func (c *Checker) Check(ctx context.Context, config Config) (Status, error) {
	if config.Provider == "" {
		config.Provider = "tailscale"
	}
	status := Status{Provider: config.Provider, PeerIP: config.PeerIP}
	if config.Provider != "tailscale" {
		return status, fmt.Errorf("unsupported overlay provider %q; use tailscale", config.Provider)
	}
	peerIP := net.ParseIP(config.PeerIP)
	if peerIP == nil || peerIP.To4() == nil || !tailscaleIPv4().Contains(peerIP) {
		return status, fmt.Errorf("peer IP must be an IPv4 address in Tailscale's 100.64.0.0/10 range")
	}
	status.PeerIP = peerIP.String()
	cli, err := c.findCLI()
	if err != nil {
		return status, err
	}
	status.CLI = cli
	output, err := c.runner.Run(ctx, cli, "status", "--json")
	if err != nil {
		return status, fmt.Errorf("read Tailscale status: %w", err)
	}
	var tailscaleStatus struct {
		BackendState string `json:"BackendState"`
		Self         struct {
			Online       bool     `json:"Online"`
			TailscaleIPs []string `json:"TailscaleIPs"`
		} `json:"Self"`
		Peer map[string]struct {
			Online       bool     `json:"Online"`
			TailscaleIPs []string `json:"TailscaleIPs"`
		} `json:"Peer"`
	}
	if err := json.Unmarshal(output, &tailscaleStatus); err != nil {
		return status, fmt.Errorf("decode Tailscale status: %w", err)
	}
	status.BackendState = tailscaleStatus.BackendState
	if tailscaleStatus.BackendState != "Running" || !tailscaleStatus.Self.Online {
		return status, fmt.Errorf("Tailscale is not online (backend=%s)", tailscaleStatus.BackendState)
	}
	localIP, err := selectLocalIP(config.LocalIP, tailscaleStatus.Self.TailscaleIPs)
	if err != nil {
		return status, err
	}
	if localIP.Equal(peerIP) {
		return status, fmt.Errorf("peer IP must differ from the local Tailscale IP")
	}
	status.LocalIP = localIP.String()
	for _, peer := range tailscaleStatus.Peer {
		for _, value := range peer.TailscaleIPs {
			if net.ParseIP(value).Equal(peerIP) {
				status.PeerOnline = peer.Online
			}
		}
	}
	if !status.PeerOnline {
		return status, fmt.Errorf("Tailscale peer %s is missing or offline", status.PeerIP)
	}
	resolvedInterface, err := c.findInterface(status.LocalIP)
	if err != nil {
		return status, err
	}
	if config.Interface != "" && config.Interface != "auto" && config.Interface != resolvedInterface {
		return status, fmt.Errorf("local Tailscale IP is assigned to %s, not %s", resolvedInterface, config.Interface)
	}
	status.Interface = resolvedInterface
	route, err := c.runner.Run(ctx, "/sbin/route", "-n", "get", status.PeerIP)
	if err != nil {
		return status, fmt.Errorf("resolve route to Tailscale peer: %w", err)
	}
	match := routeInterface.FindSubmatch(route)
	if len(match) != 2 {
		return status, fmt.Errorf("route to Tailscale peer did not report an interface")
	}
	if string(match[1]) != status.Interface {
		return status, fmt.Errorf("route to peer uses %s instead of local Tailscale interface %s", match[1], status.Interface)
	}
	return status, nil
}

func selectLocalIP(requested string, values []string) (net.IP, error) {
	if requested != "" {
		candidate := net.ParseIP(requested)
		if candidate == nil || candidate.To4() == nil || !tailscaleIPv4().Contains(candidate) {
			return nil, fmt.Errorf("local IP must be an IPv4 address in Tailscale's 100.64.0.0/10 range")
		}
		for _, value := range values {
			if candidate.Equal(net.ParseIP(value)) {
				return candidate, nil
			}
		}
		return nil, fmt.Errorf("local IP %s is not reported by Tailscale", requested)
	}
	for _, value := range values {
		candidate := net.ParseIP(value)
		if candidate != nil && candidate.To4() != nil && tailscaleIPv4().Contains(candidate) {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("Tailscale did not report a local IPv4 address")
}

func findTailscaleCLI() (string, error) {
	if resolved, err := exec.LookPath("tailscale"); err == nil {
		return resolved, nil
	}
	if info, err := os.Stat(tailscaleAppCLI); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
		return tailscaleAppCLI, nil
	}
	return "", fmt.Errorf("Tailscale CLI not found; install and authenticate Tailscale on this Mac first")
}

func interfaceForIP(address string) (string, error) {
	target := net.ParseIP(address)
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("list host network interfaces: %w", err)
	}
	for _, networkInterface := range interfaces {
		addresses, err := networkInterface.Addrs()
		if err != nil {
			continue
		}
		for _, value := range addresses {
			ip, _, err := net.ParseCIDR(value.String())
			if err == nil && ip.Equal(target) {
				return networkInterface.Name, nil
			}
		}
	}
	return "", fmt.Errorf("local Tailscale IP %s is not assigned to a network interface", address)
}

func tailscaleIPv4() *net.IPNet {
	_, network, _ := net.ParseCIDR("100.64.0.0/10")
	return network
}
