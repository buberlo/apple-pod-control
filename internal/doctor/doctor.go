package doctor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

type Status string

const (
	Pass Status = "PASS"
	Warn Status = "WARN"
	Fail Status = "FAIL"
)

var (
	defaultRouteInterfacePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,15}$`)
	tunnelInterfacePattern       = regexp.MustCompile(`^utun[0-9]+$`)
)

type Result struct {
	Name        string `json:"name"`
	Status      Status `json:"status"`
	Detail      string `json:"detail"`
	Remediation string `json:"remediation,omitempty"`
}

type Report struct {
	Role    string   `json:"role"`
	Results []Result `json:"results"`
}

type Options struct {
	Role            string
	ContainerBinary string
	ListenAddress   string
	APIPort         int
	FlannelPort     int
	Peer            string
	Timeout         time.Duration
}

type environment struct {
	goos        string
	goarch      string
	numCPU      int
	lookPath    func(string) (string, error)
	run         func(context.Context, string, ...string) (string, error)
	memoryBytes func(context.Context) (int64, error)
	listenTCP   func(string) (io.Closer, error)
	listenUDP   func(string) (io.Closer, error)
	lookupHost  func(context.Context, string) ([]string, error)
	dialTCP     func(context.Context, string) error
}

func Run(ctx context.Context, options Options) Report {
	return run(ctx, defaultEnvironment(), defaultOptions(options))
}

func (r Report) FailureCount() int {
	count := 0
	for _, result := range r.Results {
		if result.Status == Fail {
			count++
		}
	}
	return count
}

func (r Report) WarningCount() int {
	count := 0
	for _, result := range r.Results {
		if result.Status == Warn {
			count++
		}
	}
	return count
}

func (r Report) PassedCount() int {
	return len(r.Results) - r.FailureCount() - r.WarningCount()
}

func (r Report) WriteText(writer io.Writer) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "CHECK\tSTATUS\tDETAIL"); err != nil {
		return err
	}
	for _, result := range r.Results {
		detail := result.Detail
		if result.Remediation != "" {
			detail += "; " + result.Remediation
		}
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", result.Name, result.Status, detail); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(writer, "\nSummary: %d passed, %d warnings, %d failed\n", r.PassedCount(), r.WarningCount(), r.FailureCount())
	return err
}

func (r Report) WriteJSON(writer io.Writer) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

func defaultOptions(options Options) Options {
	if options.Role == "" {
		options.Role = "server"
	}
	if options.ContainerBinary == "" {
		options.ContainerBinary = "container"
	}
	if options.ListenAddress == "" {
		options.ListenAddress = "127.0.0.1"
	}
	if options.APIPort == 0 {
		options.APIPort = 16443
	}
	if options.FlannelPort == 0 {
		options.FlannelPort = 8472
	}
	if options.Timeout == 0 {
		options.Timeout = 3 * time.Second
	}
	return options
}

func defaultEnvironment() environment {
	return environment{
		goos:   runtime.GOOS,
		goarch: runtime.GOARCH,
		numCPU: runtime.NumCPU(),
		lookPath: func(binary string) (string, error) {
			path, err := exec.LookPath(binary)
			if err == nil || binary != "container" {
				return path, err
			}
			if info, statErr := os.Stat("/usr/local/bin/container"); statErr == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
				return "/usr/local/bin/container", nil
			}
			return "", err
		},
		run: func(ctx context.Context, binary string, arguments ...string) (string, error) {
			command := exec.CommandContext(ctx, binary, arguments...)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			err := command.Run()
			return strings.TrimSpace(output.String()), err
		},
		memoryBytes: func(ctx context.Context) (int64, error) {
			command := exec.CommandContext(ctx, "sysctl", "-n", "hw.memsize")
			output, err := command.Output()
			if err != nil {
				return 0, err
			}
			return strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
		},
		listenTCP: func(address string) (io.Closer, error) {
			return net.Listen("tcp", address)
		},
		listenUDP: func(address string) (io.Closer, error) {
			return net.ListenPacket("udp", address)
		},
		lookupHost: net.DefaultResolver.LookupHost,
		dialTCP: func(ctx context.Context, address string) error {
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if err != nil {
				return err
			}
			return connection.Close()
		},
	}
}

func run(ctx context.Context, env environment, options Options) Report {
	options = defaultOptions(options)
	report := Report{Role: options.Role}
	add := func(name string, status Status, detail, remediation string) {
		report.Results = append(report.Results, Result{Name: name, Status: status, Detail: detail, Remediation: remediation})
	}

	if options.Role != "server" && options.Role != "agent" {
		add("role", Fail, fmt.Sprintf("unsupported role %q", options.Role), "use server or agent")
	} else {
		add("role", Pass, options.Role, "")
	}
	if env.goos == "darwin" && env.goarch == "arm64" {
		add("platform", Pass, "darwin/arm64", "")
		checkDefaultRoute(ctx, env, options.Timeout, add)
	} else {
		add("platform", Fail, env.goos+"/"+env.goarch, "APC K3s nodes require Apple Silicon macOS")
	}

	minimumCPU := 1
	minimumMemory := int64(512 << 20)
	if options.Role == "server" {
		minimumCPU = 2
		minimumMemory = 2 << 30
	}
	if env.numCPU >= minimumCPU {
		add("cpu", Pass, fmt.Sprintf("%d logical CPUs", env.numCPU), "")
	} else {
		add("cpu", Fail, fmt.Sprintf("%d logical CPUs", env.numCPU), fmt.Sprintf("at least %d required", minimumCPU))
	}
	if memory, err := env.memoryBytes(ctx); err != nil {
		add("memory", Warn, "could not determine host memory", err.Error())
	} else if memory < minimumMemory {
		add("memory", Fail, humanBytes(memory), fmt.Sprintf("at least %s required", humanBytes(minimumMemory)))
	} else {
		add("memory", Pass, humanBytes(memory), "")
	}

	containerPath, err := env.lookPath(options.ContainerBinary)
	if err != nil {
		add("container-cli", Fail, "not found", "install apple/container 1.0 or newer")
		return report
	}
	version, versionErr := runWithTimeout(ctx, env, options.Timeout, containerPath, "--version")
	if versionErr != nil {
		add("container-cli", Fail, "could not execute", versionErr.Error())
	} else if !supportedContainerVersion(version) {
		add("container-cli", Fail, version, "apple/container 1.0 or newer is required")
	} else {
		add("container-cli", Pass, version, "")
	}

	status, statusErr := runWithTimeout(ctx, env, options.Timeout, containerPath, "system", "status")
	if statusErr != nil || !strings.Contains(strings.ToLower(status), "status") || !strings.Contains(strings.ToLower(status), "running") {
		add("container-system", Fail, "not running", "run: container system start")
	} else {
		add("container-system", Pass, "running", "")
	}

	runHelp, runHelpErr := runWithTimeout(ctx, env, options.Timeout, containerPath, "run", "--help")
	if runHelpErr != nil {
		add("container-run", Fail, "cannot inspect capabilities", runHelpErr.Error())
	} else {
		missing := missingOptions(runHelp, "--publish", "--cap-add", "--mount", "--arch")
		if len(missing) > 0 {
			add("container-run", Fail, "missing "+strings.Join(missing, ", "), "upgrade apple/container")
		} else {
			add("container-run", Pass, "publish, capabilities, mounts and arm64 supported", "")
		}
	}

	machineHelp, machineHelpErr := runWithTimeout(ctx, env, options.Timeout, containerPath, "machine", "create", "--help")
	if machineHelpErr != nil {
		add("container-machine", Warn, "machine API unavailable", "the run-based node envelope remains usable")
	} else if !strings.Contains(machineHelp, "--publish") {
		add("container-machine", Warn, "no host port publishing in container 1.0", "APC creates K3s nodes with container run")
	} else {
		add("container-machine", Pass, "host port publishing supported", "")
	}

	checkPort := func(name, network string, port int, listener func(string) (io.Closer, error)) {
		address := net.JoinHostPort(options.ListenAddress, strconv.Itoa(port))
		closer, listenErr := listener(address)
		if listenErr != nil {
			if owner, managed := managedPortOwner(ctx, env, options.Timeout, containerPath, network, port); managed {
				add(name, Pass, fmt.Sprintf("%s %s in use by APC node %s", network, address, owner), "")
				return
			}
			add(name, Fail, fmt.Sprintf("%s %s unavailable", network, address), listenErr.Error())
			return
		}
		_ = closer.Close()
		add(name, Pass, fmt.Sprintf("%s %s available", network, address), "")
	}
	if options.Role == "server" {
		checkPort("kubernetes-api-port", "tcp", options.APIPort, env.listenTCP)
	}
	checkPort("flannel-vxlan-port", "udp", options.FlannelPort, env.listenUDP)

	for _, tool := range []string{"kubectl", "helm"} {
		if path, toolErr := env.lookPath(tool); toolErr != nil {
			add(tool, Warn, "not installed", "install it for native Kubernetes workflows")
		} else {
			add(tool, Pass, path, "")
		}
	}

	if options.Peer != "" {
		peerHost := options.Peer
		if host, _, splitErr := net.SplitHostPort(options.Peer); splitErr == nil {
			peerHost = host
		}
		peerCtx, cancel := context.WithTimeout(ctx, options.Timeout)
		addresses, lookupErr := env.lookupHost(peerCtx, peerHost)
		cancel()
		if lookupErr != nil || len(addresses) == 0 {
			add("peer-dns", Fail, options.Peer, "peer hostname or address is not resolvable")
		} else {
			add("peer-dns", Pass, strings.Join(addresses, ", "), "")
		}
		sshAddress := options.Peer
		if _, _, splitErr := net.SplitHostPort(options.Peer); splitErr != nil {
			sshAddress = net.JoinHostPort(options.Peer, "22")
		}
		peerCtx, cancel = context.WithTimeout(ctx, options.Timeout)
		dialErr := env.dialTCP(peerCtx, sshAddress)
		cancel()
		if dialErr != nil {
			add("peer-ssh", Fail, sshAddress, dialErr.Error())
		} else {
			add("peer-ssh", Pass, sshAddress, "")
		}
	}

	return report
}

func checkDefaultRoute(ctx context.Context, env environment, timeout time.Duration, add func(string, Status, string, string)) {
	output, err := runWithTimeout(ctx, env, timeout, "/sbin/route", "-n", "get", "default")
	if err != nil {
		add(
			"host-default-route",
			Warn,
			"default route lookup unavailable",
			"verify host routing and Apple vmnet NAT before relying on VM/pod egress",
		)
		return
	}

	interfaceName, ok := parseDefaultRouteInterface(output)
	if !ok {
		add(
			"host-default-route",
			Warn,
			"default route interface could not be determined",
			"verify host routing and Apple vmnet NAT before relying on VM/pod egress",
		)
		return
	}

	if tunnelInterfacePattern.MatchString(interfaceName) {
		add(
			"host-default-route",
			Warn,
			fmt.Sprintf("default route uses tunnel interface %s; VM/pod egress may be affected", interfaceName),
			"verify the tunnel permits Apple vmnet NAT traffic",
		)
		return
	}

	add("host-default-route", Pass, fmt.Sprintf("default route uses non-tunnel interface %s", interfaceName), "")
}

func parseDefaultRouteInterface(output string) (string, bool) {
	interfaceName := ""
	for _, line := range strings.Split(output, "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if !found || !strings.EqualFold(strings.TrimSpace(key), "interface") {
			continue
		}

		candidate := strings.TrimSpace(value)
		if !defaultRouteInterfacePattern.MatchString(candidate) {
			return "", false
		}
		if interfaceName != "" && interfaceName != candidate {
			return "", false
		}
		interfaceName = candidate
	}
	return interfaceName, interfaceName != ""
}

type listedContainer struct {
	ID            string `json:"id"`
	Configuration struct {
		ID             string            `json:"id"`
		Labels         map[string]string `json:"labels"`
		PublishedPorts []struct {
			HostPort int    `json:"hostPort"`
			Proto    string `json:"proto"`
		} `json:"publishedPorts"`
	} `json:"configuration"`
}

func managedPortOwner(ctx context.Context, env environment, timeout time.Duration, binary, network string, port int) (string, bool) {
	output, err := runWithTimeout(ctx, env, timeout, binary, "list", "--format", "json")
	if err != nil {
		return "", false
	}
	var containers []listedContainer
	if err := json.Unmarshal([]byte(output), &containers); err != nil {
		return "", false
	}
	for _, candidate := range containers {
		labels := candidate.Configuration.Labels
		role := labels["apc.dev/role"]
		if labels["apc.dev/managed"] != "true" || (role != "server" && role != "agent") {
			continue
		}
		for _, published := range candidate.Configuration.PublishedPorts {
			if published.HostPort == port && strings.EqualFold(published.Proto, network) {
				owner := candidate.ID
				if owner == "" {
					owner = candidate.Configuration.ID
				}
				return owner, true
			}
		}
	}
	return "", false
}

func runWithTimeout(ctx context.Context, env environment, timeout time.Duration, binary string, arguments ...string) (string, error) {
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return env.run(commandCtx, binary, arguments...)
}

func supportedContainerVersion(value string) bool {
	const marker = "version "
	index := strings.Index(strings.ToLower(value), marker)
	if index < 0 {
		return false
	}
	version := value[index+len(marker):]
	parts := strings.SplitN(version, ".", 2)
	major, err := strconv.Atoi(parts[0])
	return err == nil && major >= 1
}

func missingOptions(help string, options ...string) []string {
	missing := make([]string, 0)
	for _, option := range options {
		if !strings.Contains(help, option) {
			missing = append(missing, option)
		}
	}
	return missing
}

func humanBytes(value int64) string {
	const gib = int64(1 << 30)
	const mib = int64(1 << 20)
	if value >= gib {
		return fmt.Sprintf("%.1f GiB", float64(value)/float64(gib))
	}
	return fmt.Sprintf("%.0f MiB", float64(value)/float64(mib))
}
