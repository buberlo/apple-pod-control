package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"google.golang.org/protobuf/proto"
)

var ErrNotFound = errors.New("container not found")

const workloadSpecDigestLabel = "apc.dev/spec-sha256"

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
	Binary        string
	runner        commandRunner
	http          *http.Client
	desired       sync.Map // workload ID -> immutable *apcv1.WorkloadCommand
	imageDefaults sync.Map // workload specification digest -> immutable legacyImageDefaults
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
			adoptErr := r.adoptExisting(ctx, command)
			if adoptErr == nil {
				r.rememberDesired(command)
				return nil
			}
			// "Already in use" can refer to a host resource rather than the
			// requested container name. Preserve that actionable run error when
			// there is no same-name envelope to inspect.
			if errors.Is(adoptErr, ErrNotFound) {
				return commandError("start", command.ContainerName, stderr, err)
			}
			return adoptErr
		}
		return commandError("start", command.ContainerName, stderr, err)
	}
	r.rememberDesired(command)
	return nil
}

func (r *AppleContainer) adoptExisting(ctx context.Context, command *apcv1.WorkloadCommand) error {
	record, err := r.inspect(ctx, command.ContainerName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return fmt.Errorf("inspect existing container %q: %w", command.ContainerName, err)
	}
	if err := r.validateOwnedWorkload(ctx, record, command); err != nil {
		return fmt.Errorf("refusing to adopt container %q: %w", command.ContainerName, err)
	}
	status, _, err := decodeStatus(record.Status)
	if err != nil {
		return fmt.Errorf("decode existing container %q status: %w", command.ContainerName, err)
	}
	switch normalizeState(status) {
	case "Running", "Pending":
		return nil
	case "Failed":
		identity, identityErr := inspectedRuntimeIdentity(record, command.ContainerName)
		if identityErr != nil {
			return identityErr
		}
		_, stderr, startErr := r.runner.Run(ctx, r.Binary, "start", identity)
		if startErr == nil {
			return nil
		}

		// Another reconcile may have started the same owned envelope between
		// inspect and start. Re-inspect before turning that harmless race into a
		// failed workload.
		current, inspectErr := r.inspect(ctx, command.ContainerName)
		if inspectErr == nil && r.validateOwnedWorkload(ctx, current, command) == nil {
			currentStatus, _, statusErr := decodeStatus(current.Status)
			if statusErr == nil && normalizeState(currentStatus) == "Running" {
				return nil
			}
		}
		return commandError("start existing", command.ContainerName, stderr, startErr)
	default:
		return fmt.Errorf("refusing to adopt container %q with unsupported state %q", command.ContainerName, status)
	}
}

func RunArguments(command *apcv1.WorkloadCommand) ([]string, error) {
	if command == nil || command.ContainerName == "" || command.Image == "" {
		return nil, fmt.Errorf("workload command requires container name and image")
	}
	arch := command.Architecture
	if arch == "" {
		arch = "arm64"
	}
	digest, err := workloadSpecDigest(command)
	if err != nil {
		return nil, err
	}
	args := []string{
		"run", "--detach", "--name", command.ContainerName,
		"--arch", arch,
		"--label", "apc.dev/managed=true",
		"--label", "apc.dev/workload-id=" + command.WorkloadId,
		"--label", workloadSpecDigestLabel + "=" + digest,
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
	if command == nil || command.ContainerName == "" || command.WorkloadId == "" {
		return fmt.Errorf("stop command requires a container and workload identity")
	}
	record, err := r.inspect(ctx, command.ContainerName)
	if errors.Is(err, ErrNotFound) {
		r.desired.Delete(command.WorkloadId)
		return nil
	}
	if err != nil {
		return err
	}
	validationCommand, exactValidation, err := r.desiredForMutation(command)
	if err != nil {
		return err
	}
	validatedDigest := ""
	if exactValidation {
		if err := r.validateOwnedWorkload(ctx, record, validationCommand); err != nil {
			return err
		}
		validatedDigest = record.Configuration.Labels[workloadSpecDigestLabel]
	} else {
		validatedDigest, err = validateOwnedWorkloadForMutation(record, command)
		if err != nil {
			return err
		}
	}
	identity, err := inspectedRuntimeIdentity(record, command.ContainerName)
	if err != nil {
		return err
	}
	status, _, err := decodeStatus(record.Status)
	if err != nil {
		return fmt.Errorf("decode existing container %q status: %w", command.ContainerName, err)
	}
	if normalizeState(status) != "Failed" {
		_, stderr, stopErr := r.runner.Run(ctx, r.Binary, "stop", identity)
		if stopErr != nil && !isNotFound(stderr) {
			return commandError("stop", command.ContainerName, stderr, stopErr)
		}
	}
	record, err = r.inspect(ctx, command.ContainerName)
	if errors.Is(err, ErrNotFound) {
		r.desired.Delete(command.WorkloadId)
		return nil
	}
	if err != nil {
		return err
	}
	if exactValidation {
		if err := r.validateOwnedWorkload(ctx, record, validationCommand); err != nil {
			return fmt.Errorf("refusing delete after stop: %w", err)
		}
	} else {
		currentDigest, validationErr := validateOwnedWorkloadForMutation(record, command)
		if validationErr != nil {
			return fmt.Errorf("refusing delete after stop: workload envelope changed: %w", validationErr)
		}
		if currentDigest != validatedDigest {
			return fmt.Errorf("refusing delete after stop: immutable workload specification digest changed")
		}
	}
	identity, err = inspectedRuntimeIdentity(record, command.ContainerName)
	if err != nil {
		return fmt.Errorf("refusing delete after stop: %w", err)
	}
	_, stderr, deleteErr := r.runner.Run(ctx, r.Binary, "delete", identity)
	if deleteErr != nil && !isNotFound(stderr) {
		return commandError("delete", command.ContainerName, stderr, deleteErr)
	}
	r.desired.Delete(command.WorkloadId)
	return nil
}

func (r *AppleContainer) rememberDesired(command *apcv1.WorkloadCommand) {
	if command == nil || command.WorkloadId == "" {
		return
	}
	r.desired.Store(command.WorkloadId, cloneWorkloadCommand(command))
}

func (r *AppleContainer) desiredForMutation(command *apcv1.WorkloadCommand) (*apcv1.WorkloadCommand, bool, error) {
	if command.Image != "" {
		return command, true, nil
	}
	value, ok := r.desired.Load(command.WorkloadId)
	if !ok {
		return nil, false, nil
	}
	desired, ok := value.(*apcv1.WorkloadCommand)
	if !ok || desired.ContainerName != command.ContainerName || desired.WorkloadId != command.WorkloadId {
		return nil, false, fmt.Errorf("refusing to mutate container %q: cached workload identity does not match", command.ContainerName)
	}
	return desired, true, nil
}

func cloneWorkloadCommand(command *apcv1.WorkloadCommand) *apcv1.WorkloadCommand {
	return proto.Clone(command).(*apcv1.WorkloadCommand)
}

func inspectedRuntimeIdentity(record inspectRecord, expectedName string) (string, error) {
	identity := record.ID
	if identity == "" {
		identity = record.Configuration.ID
	}
	if identity == "" || identity != expectedName {
		return "", fmt.Errorf("refusing container %q: inspect returned identity %q", expectedName, identity)
	}
	return identity, nil
}

type inspectRecord struct {
	ID       string          `json:"id"`
	Status   json.RawMessage `json:"status"`
	Networks []struct {
		Address string `json:"address"`
	} `json:"networks"`
	Configuration struct {
		ID       string            `json:"id"`
		Labels   map[string]string `json:"labels"`
		Platform struct {
			Architecture string `json:"architecture"`
			OS           string `json:"os"`
		} `json:"platform"`
		Image struct {
			Reference string `json:"reference"`
		} `json:"image"`
		InitProcess struct {
			Arguments   []string `json:"arguments"`
			Environment []string `json:"environment"`
			Executable  string   `json:"executable"`
			Terminal    bool     `json:"terminal"`
		} `json:"initProcess"`
		CapAdd   []string          `json:"capAdd"`
		CapDrop  []string          `json:"capDrop"`
		Mounts   []json.RawMessage `json:"mounts"`
		Networks []struct {
			Network string `json:"network"`
		} `json:"networks"`
		PublishedPorts []struct {
			ContainerPort int    `json:"containerPort"`
			Count         int    `json:"count"`
			HostAddress   string `json:"hostAddress"`
			HostPort      int    `json:"hostPort"`
			Proto         string `json:"proto"`
		} `json:"publishedPorts"`
		PublishedSockets []json.RawMessage `json:"publishedSockets"`
		Resources        struct {
			CPUs          int   `json:"cpus"`
			MemoryInBytes int64 `json:"memoryInBytes"`
		} `json:"resources"`
		ReadOnly       bool              `json:"readOnly"`
		Rosetta        bool              `json:"rosetta"`
		SSH            bool              `json:"ssh"`
		Sysctls        map[string]string `json:"sysctls"`
		UseInit        bool              `json:"useInit"`
		Virtualization bool              `json:"virtualization"`
	} `json:"configuration"`
}

type structuredStatus struct {
	State    string `json:"state"`
	Networks []struct {
		IPv4Address string `json:"ipv4Address"`
		IPv6Address string `json:"ipv6Address"`
	} `json:"networks"`
}

func (r *AppleContainer) Observe(ctx context.Context, command *apcv1.WorkloadCommand) (Observation, error) {
	if command == nil || command.ContainerName == "" {
		return Observation{}, fmt.Errorf("observe command requires a container name")
	}
	record, err := r.inspect(ctx, command.ContainerName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Observation{State: "Pending", Message: "container not found"}, ErrNotFound
		}
		return Observation{}, err
	}
	if err := r.validateOwnedWorkload(ctx, record, command); err != nil {
		return Observation{State: "Failed", Message: err.Error()}, err
	}
	statusValue, statusNetworks, err := decodeStatus(record.Status)
	if err != nil {
		return Observation{}, fmt.Errorf("decode container status: %w", err)
	}
	state := normalizeState(statusValue)
	address := ""
	if len(statusNetworks) > 0 {
		address = strings.Split(statusNetworks[0], "/")[0]
	} else if len(record.Networks) > 0 {
		address = strings.Split(record.Networks[0].Address, "/")[0]
	}
	return Observation{State: state, Ready: state == "Running", Address: address}, nil
}

func (r *AppleContainer) inspect(ctx context.Context, name string) (inspectRecord, error) {
	stdout, stderr, err := r.runner.Run(ctx, r.Binary, "inspect", name)
	if err != nil {
		if isNotFound(stderr) {
			return inspectRecord{}, ErrNotFound
		}
		return inspectRecord{}, commandError("inspect", name, stderr, err)
	}
	var records []inspectRecord
	if err := json.Unmarshal(stdout, &records); err != nil {
		return inspectRecord{}, fmt.Errorf("decode container inspect output: %w", err)
	}
	if len(records) != 1 {
		return inspectRecord{}, fmt.Errorf("inspect returned %d records", len(records))
	}
	return records[0], nil
}

func (r *AppleContainer) validateOwnedWorkload(ctx context.Context, record inspectRecord, command *apcv1.WorkloadCommand) error {
	actualDigest, err := validateOwnedWorkloadIdentity(record, command)
	if err != nil {
		return err
	}
	expectedDigest, err := workloadSpecDigest(command)
	if err != nil {
		return err
	}
	if actualDigest != "" && actualDigest != expectedDigest {
		return fmt.Errorf("refusing container %q: desired workload specification digest does not match", command.ContainerName)
	}
	defaults, err := r.inspectImageDefaults(ctx, command.Image, normalizedWorkloadArchitecture(command.Architecture), expectedDigest)
	if err != nil {
		return fmt.Errorf("refusing container %q: inspect declared image defaults: %w", command.ContainerName, err)
	}
	if err := validateWorkloadEnvelope(record, command, defaults); err != nil {
		if actualDigest == "" {
			return fmt.Errorf("refusing legacy container %q without a specification digest: %w", command.ContainerName, err)
		}
		return fmt.Errorf("refusing container %q: %w", command.ContainerName, err)
	}
	return nil
}

func validateOwnedWorkloadIdentity(record inspectRecord, command *apcv1.WorkloadCommand) (string, error) {
	if command == nil || command.ContainerName == "" || command.WorkloadId == "" {
		return "", fmt.Errorf("refusing workload operation without container and workload identity")
	}
	labels := record.Configuration.Labels
	if labels["apc.dev/managed"] != "true" || labels["apc.dev/workload-id"] != command.WorkloadId {
		return "", fmt.Errorf("refusing container %q: it is not owned by APC workload %q", command.ContainerName, command.WorkloadId)
	}
	identity := record.ID
	if identity == "" {
		identity = record.Configuration.ID
	}
	if identity == "" || identity != command.ContainerName {
		return "", fmt.Errorf("refusing container %q: inspect returned container %q", command.ContainerName, identity)
	}
	return labels[workloadSpecDigestLabel], nil
}

func validateOwnedWorkloadForMutation(record inspectRecord, command *apcv1.WorkloadCommand) (string, error) {
	digest, err := validateOwnedWorkloadIdentity(record, command)
	if err != nil {
		return "", err
	}
	decoded, decodeErr := hex.DecodeString(digest)
	if decodeErr != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
		return "", fmt.Errorf("refusing container %q: immutable workload specification digest is missing or invalid", command.ContainerName)
	}
	configuration := record.Configuration
	if configuration.Platform.OS != "linux" || configuration.Platform.Architecture != "arm64" || configuration.Image.Reference == "" {
		return "", fmt.Errorf("refusing container %q: runtime platform or image is not an APC ARM64 workload envelope", command.ContainerName)
	}
	if configuration.InitProcess.Executable == "" || configuration.InitProcess.Terminal || len(configuration.CapAdd) != 0 || len(configuration.CapDrop) != 0 || len(configuration.Mounts) != 0 || len(configuration.PublishedSockets) != 0 || configuration.ReadOnly || configuration.Rosetta || configuration.SSH || configuration.UseInit || configuration.Virtualization || len(configuration.Sysctls) != 0 {
		return "", fmt.Errorf("refusing container %q: runtime enables an unexpected process, capability, mount, socket or virtualization feature", command.ContainerName)
	}
	if _, err := parseWorkloadEnvironment(configuration.InitProcess.Environment); err != nil {
		return "", fmt.Errorf("refusing container %q: %w", command.ContainerName, err)
	}
	if len(configuration.Networks) != 1 || configuration.Networks[0].Network != "default" || configuration.Resources.CPUs < 1 || configuration.Resources.MemoryInBytes < 1 {
		return "", fmt.Errorf("refusing container %q: runtime network or resource envelope is invalid", command.ContainerName)
	}
	seenPorts := make(map[string]struct{}, len(configuration.PublishedPorts))
	for _, port := range configuration.PublishedPorts {
		protocol := strings.ToLower(port.Proto)
		if port.Count != 1 || port.ContainerPort < 1 || port.ContainerPort > 65535 || port.HostPort < 1 || port.HostPort > 65535 || (protocol != "tcp" && protocol != "udp") {
			return "", fmt.Errorf("refusing container %q: runtime published port envelope is invalid", command.ContainerName)
		}
		key := fmt.Sprintf("%s/%s/%d/%d", port.HostAddress, protocol, port.HostPort, port.ContainerPort)
		if _, duplicate := seenPorts[key]; duplicate {
			return "", fmt.Errorf("refusing container %q: runtime publishes a duplicate port", command.ContainerName)
		}
		seenPorts[key] = struct{}{}
	}
	return digest, nil
}

type workloadDigestEnvironment struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type workloadDigestPort struct {
	ContainerPort int32  `json:"containerPort"`
	HostPort      int32  `json:"hostPort"`
	HostIP        string `json:"hostIP"`
	Protocol      string `json:"protocol"`
}

func workloadSpecDigest(command *apcv1.WorkloadCommand) (string, error) {
	if command == nil || command.WorkloadId == "" || command.ContainerName == "" || command.Image == "" {
		return "", fmt.Errorf("workload command requires workload ID, container name and image")
	}
	environment := make([]workloadDigestEnvironment, 0, len(command.Environment))
	for name, value := range command.Environment {
		if name == "" || strings.Contains(name, "=") {
			return "", fmt.Errorf("workload environment contains invalid name %q", name)
		}
		environment = append(environment, workloadDigestEnvironment{Name: name, Value: value})
	}
	sort.Slice(environment, func(i, j int) bool { return environment[i].Name < environment[j].Name })
	ports := make([]workloadDigestPort, 0, len(command.Ports))
	for _, port := range command.Ports {
		if port == nil {
			return "", fmt.Errorf("workload contains a nil port")
		}
		protocol := strings.ToLower(port.Protocol)
		if protocol == "" {
			protocol = "tcp"
		}
		ports = append(ports, workloadDigestPort{ContainerPort: port.ContainerPort, HostPort: port.HostPort, HostIP: port.HostIp, Protocol: protocol})
	}
	sort.Slice(ports, func(i, j int) bool {
		left := fmt.Sprintf("%s/%s/%05d/%05d", ports[i].HostIP, ports[i].Protocol, ports[i].HostPort, ports[i].ContainerPort)
		right := fmt.Sprintf("%s/%s/%05d/%05d", ports[j].HostIP, ports[j].Protocol, ports[j].HostPort, ports[j].ContainerPort)
		return left < right
	})
	payload := struct {
		WorkloadID    string                      `json:"workloadID"`
		ContainerName string                      `json:"containerName"`
		Image         string                      `json:"image"`
		Architecture  string                      `json:"architecture"`
		CPUs          int32                       `json:"cpus"`
		Memory        string                      `json:"memory"`
		Environment   []workloadDigestEnvironment `json:"environment"`
		Ports         []workloadDigestPort        `json:"ports"`
		Arguments     []string                    `json:"arguments"`
	}{
		WorkloadID: command.WorkloadId, ContainerName: command.ContainerName,
		Image: command.Image, Architecture: normalizedWorkloadArchitecture(command.Architecture),
		CPUs: command.Cpus, Memory: command.Memory, Environment: environment, Ports: ports,
		Arguments: append([]string(nil), command.Arguments...),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode immutable workload specification: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func normalizedWorkloadArchitecture(value string) string {
	if value == "" {
		return "arm64"
	}
	return value
}

type legacyImageDefaults struct {
	Entrypoint  []string
	Command     []string
	Environment map[string]string
}

func (r *AppleContainer) inspectImageDefaults(ctx context.Context, image, architecture, cacheKey string) (legacyImageDefaults, error) {
	if cached, ok := r.imageDefaults.Load(cacheKey); ok {
		return cached.(legacyImageDefaults), nil
	}
	stdout, stderr, err := r.runner.Run(ctx, r.Binary, "image", "inspect", image)
	if err != nil {
		return legacyImageDefaults{}, commandError("inspect image for legacy workload validation", image, stderr, err)
	}
	type imageInspectRecord struct {
		Variants []struct {
			Platform struct {
				Architecture string `json:"architecture"`
				OS           string `json:"os"`
			} `json:"platform"`
			Config struct {
				Config struct {
					Command     []string `json:"Cmd"`
					Entrypoint  []string `json:"Entrypoint"`
					Environment []string `json:"Env"`
				} `json:"config"`
			} `json:"config"`
		} `json:"variants"`
	}
	var records []imageInspectRecord
	if err := json.Unmarshal(stdout, &records); err != nil || len(records) != 1 {
		if err == nil {
			err = fmt.Errorf("image inspect returned %d records", len(records))
		}
		return legacyImageDefaults{}, fmt.Errorf("decode image defaults: %w", err)
	}
	for _, variant := range records[0].Variants {
		if variant.Platform.OS != "linux" || variant.Platform.Architecture != architecture {
			continue
		}
		environment, err := parseWorkloadEnvironment(variant.Config.Config.Environment)
		if err != nil {
			return legacyImageDefaults{}, fmt.Errorf("decode image environment: %w", err)
		}
		defaults := legacyImageDefaults{
			Entrypoint:  append([]string(nil), variant.Config.Config.Entrypoint...),
			Command:     append([]string(nil), variant.Config.Config.Command...),
			Environment: environment,
		}
		r.imageDefaults.Store(cacheKey, defaults)
		return defaults, nil
	}
	return legacyImageDefaults{}, fmt.Errorf("image has no linux/%s variant", architecture)
}

func validateWorkloadEnvelope(record inspectRecord, command *apcv1.WorkloadCommand, imageDefaults legacyImageDefaults) error {
	configuration := record.Configuration
	architecture := normalizedWorkloadArchitecture(command.Architecture)
	if configuration.Platform.OS != "linux" || configuration.Platform.Architecture != architecture {
		return fmt.Errorf("runtime platform is %s/%s, want linux/%s", configuration.Platform.OS, configuration.Platform.Architecture, architecture)
	}
	if configuration.Image.Reference != command.Image {
		return fmt.Errorf("runtime image is %q, want %q", configuration.Image.Reference, command.Image)
	}
	if configuration.InitProcess.Terminal || len(configuration.CapAdd) != 0 || len(configuration.CapDrop) != 0 || len(configuration.Mounts) != 0 || len(configuration.PublishedSockets) != 0 || configuration.ReadOnly || configuration.Rosetta || configuration.SSH || configuration.UseInit || configuration.Virtualization || len(configuration.Sysctls) != 0 {
		return fmt.Errorf("runtime enables an unexpected terminal, capability, mount, socket or virtualization feature")
	}
	if len(configuration.Networks) != 1 || configuration.Networks[0].Network != "default" {
		return fmt.Errorf("runtime does not use exactly the default network")
	}
	if command.Cpus > 0 && configuration.Resources.CPUs != int(command.Cpus) {
		return fmt.Errorf("runtime CPU count is %d, want %d", configuration.Resources.CPUs, command.Cpus)
	}
	if command.Memory != "" {
		expectedMemory, err := parseRuntimeByteSize(command.Memory)
		if err != nil {
			return err
		}
		if configuration.Resources.MemoryInBytes != int64(expectedMemory) {
			return fmt.Errorf("runtime memory is %d bytes, want %d", configuration.Resources.MemoryInBytes, expectedMemory)
		}
	}
	if err := validateWorkloadPorts(configuration.PublishedPorts, command.Ports); err != nil {
		return err
	}
	actualEnvironment, err := parseWorkloadEnvironment(configuration.InitProcess.Environment)
	if err != nil {
		return err
	}
	for name, value := range command.Environment {
		if actualEnvironment[name] != value {
			return fmt.Errorf("runtime environment does not match explicit variable %q", name)
		}
	}
	expectedEnvironment := make(map[string]string, len(imageDefaults.Environment)+len(command.Environment))
	for name, value := range imageDefaults.Environment {
		expectedEnvironment[name] = value
	}
	for name, value := range command.Environment {
		expectedEnvironment[name] = value
	}
	if !equalStringMap(actualEnvironment, expectedEnvironment) {
		return fmt.Errorf("runtime environment does not exactly match image defaults plus explicit variables")
	}
	expectedProcess := append([]string(nil), imageDefaults.Entrypoint...)
	if len(command.Arguments) == 0 {
		expectedProcess = append(expectedProcess, imageDefaults.Command...)
	} else {
		expectedProcess = append(expectedProcess, command.Arguments...)
	}
	if len(expectedProcess) == 0 {
		return fmt.Errorf("declared image and arguments do not define a process")
	}
	if configuration.InitProcess.Executable != expectedProcess[0] || !equalStrings(configuration.InitProcess.Arguments, expectedProcess[1:]) {
		return fmt.Errorf("runtime process does not exactly match image defaults and explicit arguments")
	}
	return nil
}

func parseWorkloadEnvironment(values []string) (map[string]string, error) {
	result := make(map[string]string, len(values))
	for _, value := range values {
		parts := strings.SplitN(value, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("runtime environment contains invalid entry")
		}
		if _, duplicate := result[parts[0]]; duplicate {
			return nil, fmt.Errorf("runtime environment contains duplicate variable %q", parts[0])
		}
		result[parts[0]] = parts[1]
	}
	return result, nil
}

func validateWorkloadPorts(actual []struct {
	ContainerPort int    `json:"containerPort"`
	Count         int    `json:"count"`
	HostAddress   string `json:"hostAddress"`
	HostPort      int    `json:"hostPort"`
	Proto         string `json:"proto"`
}, desired []*apcv1.Port) error {
	expected := make([]string, 0, len(desired))
	for _, port := range desired {
		if port == nil || port.HostPort == 0 {
			continue
		}
		protocol := strings.ToLower(port.Protocol)
		if protocol == "" {
			protocol = "tcp"
		}
		hostAddress := port.HostIp
		if hostAddress == "" {
			hostAddress = "0.0.0.0"
		}
		expected = append(expected, fmt.Sprintf("%s/%s/%d/%d/1", hostAddress, protocol, port.HostPort, port.ContainerPort))
	}
	got := make([]string, 0, len(actual))
	for _, port := range actual {
		got = append(got, fmt.Sprintf("%s/%s/%d/%d/%d", port.HostAddress, strings.ToLower(port.Proto), port.HostPort, port.ContainerPort, port.Count))
	}
	sort.Strings(expected)
	sort.Strings(got)
	if !equalStrings(got, expected) {
		return fmt.Errorf("runtime published ports do not exactly match the desired workload")
	}
	return nil
}

func parseRuntimeByteSize(value string) (uint64, error) {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0, fmt.Errorf("runtime memory is empty")
	}
	digitEnd := 0
	for digitEnd < len(value) && value[digitEnd] >= '0' && value[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == 0 {
		return 0, fmt.Errorf("runtime memory must be a positive non-overflowing integer")
	}
	factors := map[string]uint64{
		"": 1, "K": 1 << 10, "KI": 1 << 10, "M": 1 << 20, "MI": 1 << 20,
		"G": 1 << 30, "GI": 1 << 30, "T": 1 << 40, "TI": 1 << 40,
		"P": 1 << 50, "PI": 1 << 50,
	}
	multiplier, ok := factors[value[digitEnd:]]
	if !ok {
		return 0, fmt.Errorf("runtime memory uses unsupported suffix %q", value[digitEnd:])
	}
	number, err := strconv.ParseUint(value[:digitEnd], 10, 64)
	const maxInt64 = uint64(1<<63 - 1)
	if err != nil || number == 0 || number > maxInt64/multiplier {
		return 0, fmt.Errorf("runtime memory must be a positive non-overflowing integer")
	}
	return number * multiplier, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func decodeStatus(raw json.RawMessage) (string, []string, error) {
	var legacy string
	if err := json.Unmarshal(raw, &legacy); err == nil {
		return legacy, nil, nil
	}
	var status structuredStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		return "", nil, err
	}
	addresses := make([]string, 0, len(status.Networks))
	for _, network := range status.Networks {
		if network.IPv4Address != "" {
			addresses = append(addresses, network.IPv4Address)
		} else if network.IPv6Address != "" {
			addresses = append(addresses, network.IPv6Address)
		}
	}
	return status.State, addresses, nil
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
		if command == nil || command.ContainerName == "" || command.WorkloadId == "" {
			return fmt.Errorf("exec probe requires a container and workload identity")
		}
		record, err := r.inspect(probeCtx, command.ContainerName)
		if err != nil {
			return err
		}
		if err := r.validateOwnedWorkload(probeCtx, record, command); err != nil {
			return err
		}
		identity, err := inspectedRuntimeIdentity(record, command.ContainerName)
		if err != nil {
			return err
		}
		args := append([]string{"exec", identity}, probe.Command...)
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
