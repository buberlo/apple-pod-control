package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
)

var ErrNotFound = errors.New("container not found")

type Observation struct {
	State   string
	Ready   bool
	Message string
	Address string
}

type Runtime interface {
	Start(context.Context, *apcv1.WorkloadCommand) error
	Stop(context.Context, *apcv1.WorkloadCommand) error
	Observe(context.Context, *apcv1.WorkloadCommand) (Observation, error)
	Probe(context.Context, *apcv1.WorkloadCommand, *apcv1.HealthCheck, string) error
	Version(context.Context) string
}

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, []byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

type AppleContainer struct {
	Binary string
	runner commandRunner
	http   *http.Client
}

func NewAppleContainer(binary string) *AppleContainer {
	if binary == "" {
		binary = "container"
	}
	return &AppleContainer{
		Binary: binary,
		runner: execRunner{},
		http:   &http.Client{Timeout: 2 * time.Second},
	}
}

func (r *AppleContainer) Start(ctx context.Context, command *apcv1.WorkloadCommand) error {
	args, err := RunArguments(command)
	if err != nil {
		return err
	}
	_, stderr, err := r.runner.Run(ctx, r.Binary, args...)
	if err != nil {
		lowerError := strings.ToLower(string(stderr))
		if strings.Contains(lowerError, "already exists") || strings.Contains(lowerError, "already in use") {
			if observation, inspectErr := r.Observe(ctx, command); inspectErr == nil && observation.State != "Failed" {
				return nil
			}
		}
		return commandError("start", command.ContainerName, stderr, err)
	}
	return nil
}

func RunArguments(command *apcv1.WorkloadCommand) ([]string, error) {
	if command == nil || command.ContainerName == "" || command.Image == "" {
		return nil, fmt.Errorf("workload command requires container name and image")
	}
	arch := command.Architecture
	if arch == "" {
		arch = "arm64"
	}
	args := []string{
		"run", "--detach", "--name", command.ContainerName,
		"--arch", arch,
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/workload-id=" + command.WorkloadId,
		"--progress", "plain",
	}
	if command.Cpus > 0 {
		args = append(args, "--cpus", strconv.Itoa(int(command.Cpus)))
	}
	if command.Memory != "" {
		args = append(args, "--memory", command.Memory)
	}
	keys := make([]string, 0, len(command.Environment))
	for key := range command.Environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--env", key+"="+command.Environment[key])
	}
	for _, port := range command.Ports {
		if port.ContainerPort < 1 || port.ContainerPort > 65535 {
			return nil, fmt.Errorf("invalid container port %d", port.ContainerPort)
		}
		if port.HostPort == 0 {
			continue
		}
		protocol := strings.ToLower(port.Protocol)
		if protocol == "" {
			protocol = "tcp"
		}
		host := strconv.Itoa(int(port.HostPort))
		if port.HostIp != "" {
			host = port.HostIp + ":" + host
		}
		args = append(args, "--publish", fmt.Sprintf("%s:%d/%s", host, port.ContainerPort, protocol))
	}
	args = append(args, command.Image)
	args = append(args, command.Arguments...)
	return args, nil
}

func (r *AppleContainer) Stop(ctx context.Context, command *apcv1.WorkloadCommand) error {
	if command == nil || command.ContainerName == "" {
		return fmt.Errorf("stop command requires a container name")
	}
	_, stderr, stopErr := r.runner.Run(ctx, r.Binary, "stop", command.ContainerName)
	if stopErr != nil && !isNotFound(stderr) {
		return commandError("stop", command.ContainerName, stderr, stopErr)
	}
	_, stderr, deleteErr := r.runner.Run(ctx, r.Binary, "delete", command.ContainerName)
	if deleteErr != nil && !isNotFound(stderr) {
		return commandError("delete", command.ContainerName, stderr, deleteErr)
	}
	return nil
}

type inspectRecord struct {
	Status   string `json:"status"`
	Networks []struct {
		Address string `json:"address"`
	} `json:"networks"`
	Configuration struct {
		ID string `json:"id"`
	} `json:"configuration"`
}

func (r *AppleContainer) Observe(ctx context.Context, command *apcv1.WorkloadCommand) (Observation, error) {
	stdout, stderr, err := r.runner.Run(ctx, r.Binary, "inspect", command.ContainerName)
	if err != nil {
		if isNotFound(stderr) {
			return Observation{State: "Pending", Message: "container not found"}, ErrNotFound
		}
		return Observation{}, commandError("inspect", command.ContainerName, stderr, err)
	}
	var records []inspectRecord
	if err := json.Unmarshal(stdout, &records); err != nil {
		return Observation{}, fmt.Errorf("decode container inspect output: %w", err)
	}
	if len(records) != 1 {
		return Observation{}, fmt.Errorf("inspect returned %d records", len(records))
	}
	state := normalizeState(records[0].Status)
	address := ""
	if len(records[0].Networks) > 0 {
		address = strings.Split(records[0].Networks[0].Address, "/")[0]
	}
	return Observation{State: state, Ready: state == "Running", Address: address}, nil
}

func (r *AppleContainer) Probe(ctx context.Context, command *apcv1.WorkloadCommand, probe *apcv1.HealthCheck, address string) error {
	if probe == nil || probe.Type == "" {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, time.Duration(max(1, int(probe.TimeoutSeconds)))*time.Second)
	defer cancel()
	switch probe.Type {
	case "http":
		path := probe.Path
		if path == "" {
			path = "/"
		}
		request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, fmt.Sprintf("http://%s:%d%s", address, probe.Port, path), nil)
		if err != nil {
			return err
		}
		response, err := r.http.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 400 {
			return fmt.Errorf("HTTP probe returned %s", response.Status)
		}
		return nil
	case "tcp":
		connection, err := (&net.Dialer{}).DialContext(probeCtx, "tcp", net.JoinHostPort(address, strconv.Itoa(int(probe.Port))))
		if err != nil {
			return err
		}
		return connection.Close()
	case "exec":
		if len(probe.Command) == 0 {
			return fmt.Errorf("exec probe command is empty")
		}
		args := append([]string{"exec", command.ContainerName}, probe.Command...)
		_, stderr, err := r.runner.Run(probeCtx, r.Binary, args...)
		if err != nil {
			return commandError("probe", command.ContainerName, stderr, err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported probe type %q", probe.Type)
	}
}

func (r *AppleContainer) Version(ctx context.Context) string {
	stdout, _, err := r.runner.Run(ctx, r.Binary, "--version")
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(stdout))
}

func commandError(operation, name string, stderr []byte, err error) error {
	detail := strings.TrimSpace(string(stderr))
	if detail == "" {
		detail = err.Error()
	}
	return fmt.Errorf("container %s %q: %s", operation, name, detail)
}

func isNotFound(stderr []byte) bool {
	value := strings.ToLower(string(stderr))
	return strings.Contains(value, "not found") || strings.Contains(value, "does not exist")
}

func normalizeState(state string) string {
	switch strings.ToLower(state) {
	case "running":
		return "Running"
	case "stopped", "exited":
		return "Failed"
	case "created", "starting":
		return "Pending"
	default:
		return "Unknown"
	}
}

type Fake struct {
	mu         sync.Mutex
	containers map[string]*fakeContainer
}

type fakeContainer struct {
	command *apcv1.WorkloadCommand
	started time.Time
}

func NewFake() *Fake { return &Fake{containers: make(map[string]*fakeContainer)} }

func (f *Fake) Start(_ context.Context, command *apcv1.WorkloadCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.containers[command.ContainerName]; exists {
		return nil
	}
	f.containers[command.ContainerName] = &fakeContainer{command: command, started: time.Now()}
	return nil
}

func (f *Fake) Stop(_ context.Context, command *apcv1.WorkloadCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.containers, command.ContainerName)
	return nil
}

func (f *Fake) Observe(_ context.Context, command *apcv1.WorkloadCommand) (Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	container, exists := f.containers[command.ContainerName]
	if !exists {
		return Observation{State: "Pending"}, ErrNotFound
	}
	ready := time.Since(container.started) > 300*time.Millisecond
	return Observation{State: "Running", Ready: ready, Address: "127.0.0.1"}, nil
}

func (f *Fake) Probe(context.Context, *apcv1.WorkloadCommand, *apcv1.HealthCheck, string) error {
	return nil
}
func (f *Fake) Version(context.Context) string { return "fake-1.0.0" }
