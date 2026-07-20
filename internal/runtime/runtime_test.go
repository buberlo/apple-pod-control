package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
)

var testImageEnvironment = map[string]string{"BASE": "image", "PATH": "/usr/bin"}

func TestRunArgumentsAreDeterministicAndNative(t *testing.T) {
	command := testWorkloadCommand()
	actual, err := RunArguments(command)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := workloadSpecDigest(command)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{
		"run", "--detach", "--name", "web-abc", "--arch", "arm64",
		"--label", "apc.dev/managed=true", "--label", "apc.dev/workload-id=workload-1",
		"--label", workloadSpecDigestLabel + "=" + digest, "--progress", "plain",
		"--cpus", "2", "--memory", "1G", "--env", "A_FIRST=a", "--env", "Z_LAST=z",
		"--publish", "127.0.0.1:8080:80/tcp", "nginx:alpine", "nginx", "-g", "daemon off;",
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("arguments mismatch\nactual: %#v\nexpected: %#v", actual, expected)
	}
}

func TestWorkloadSpecDigestIsCanonicalAndSensitiveToImmutableSpec(t *testing.T) {
	left := testWorkloadCommand()
	right := testWorkloadCommand()
	right.Environment = map[string]string{"A_FIRST": "a", "Z_LAST": "z"}
	leftDigest, err := workloadSpecDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := workloadSpecDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest || len(leftDigest) != sha256HexLength {
		t.Fatalf("canonical digests = %q and %q", leftDigest, rightDigest)
	}
	right.Image = "foreign.example/image:latest"
	changed, err := workloadSpecDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if changed == leftDigest {
		t.Fatal("image change did not alter immutable specification digest")
	}
}

func TestParseRuntimeByteSizeMatchesAcceptedAPCMemoryQuantities(t *testing.T) {
	for value, want := range map[string]uint64{"512M": 512 << 20, "128Mi": 128 << 20, "1G": 1 << 30, "2Gi": 2 << 30} {
		got, err := parseRuntimeByteSize(value)
		if err != nil || got != want {
			t.Fatalf("parseRuntimeByteSize(%q) = %d, %v; want %d", value, got, err, want)
		}
	}
}

func TestObserveParsesAndValidatesAppleContainerInspect(t *testing.T) {
	command := testWorkloadCommand()
	status := map[string]any{
		"state":    "running",
		"networks": []map[string]any{{"ipv4Address": "192.168.64.47/24", "ipv6Address": "fd00::1/64"}},
	}
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: configuredInspectOutput(t, command, status, ownedLabels(t, command, true), nil)},
		{stdout: imageInspectOutput(t)},
	}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner
	observation, err := runtime.Observe(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if observation.State != "Running" || !observation.Ready || observation.Address != "192.168.64.47" {
		t.Fatalf("unexpected observation: %#v", observation)
	}
}

func TestObserveMapsNotFound(t *testing.T) {
	runtime := NewAppleContainer("container")
	runtime.runner = &stubRunner{stderr: []byte("container not found"), err: errors.New("exit 1")}
	_, err := runtime.Observe(context.Background(), testWorkloadCommand())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestStartRecoversStoppedOwnedEnvelope(t *testing.T) {
	command := testWorkloadCommand()
	runner := &scriptedRunner{responses: []runnerResponse{
		{stderr: []byte("Error: container with id web-abc already exists"), err: errors.New("exit 1")},
		{stdout: configuredInspectOutput(t, command, "stopped", ownedLabels(t, command, true), nil)},
		{stdout: imageInspectOutput(t)},
		{},
	}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner

	if err := runtime.Start(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("calls = %#v", runner.calls)
	}
	if !reflect.DeepEqual(runner.calls[1], []string{"inspect", "web-abc"}) ||
		!reflect.DeepEqual(runner.calls[2], []string{"image", "inspect", "nginx:alpine"}) ||
		!reflect.DeepEqual(runner.calls[3], []string{"start", "web-abc"}) {
		t.Fatalf("recovery calls = %#v", runner.calls)
	}
	if _, ok := runtime.desired.Load(command.WorkloadId); !ok {
		t.Fatal("successful adoption did not remember immutable desired state")
	}
}

func TestStartAdoptsExactLegacyEnvelope(t *testing.T) {
	command := testWorkloadCommand()
	runner := &scriptedRunner{responses: []runnerResponse{
		{stderr: []byte("container already exists"), err: errors.New("exit 1")},
		{stdout: configuredInspectOutput(t, command, "running", ownedLabels(t, command, false), nil)},
		{stdout: imageInspectOutput(t)},
	}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner

	if err := runtime.Start(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("legacy adoption executed unexpected command: %#v", runner.calls)
	}
}

func TestStartRefusesForeignOrMismatchedOwnershipWithoutMutation(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
	}{
		{name: "foreign", labels: map[string]string{}},
		{name: "not managed", labels: map[string]string{"apc.dev/managed": "false", "apc.dev/workload-id": "workload-1"}},
		{name: "different workload", labels: map[string]string{"apc.dev/managed": "true", "apc.dev/workload-id": "workload-2"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := testWorkloadCommand()
			runner := &scriptedRunner{responses: []runnerResponse{
				{stderr: []byte("container already exists"), err: errors.New("exit 1")},
				{stdout: configuredInspectOutput(t, command, "stopped", test.labels, nil)},
			}}
			runtime := NewAppleContainer("container")
			runtime.runner = runner

			err := runtime.Start(context.Background(), command)
			if err == nil || !strings.Contains(err.Error(), "refusing to adopt") {
				t.Fatalf("error = %v", err)
			}
			if len(runner.calls) != 2 {
				t.Fatalf("foreign container was mutated: %#v", runner.calls)
			}
		})
	}
}

func TestStartRefusesForgedOrLegacyEnvelopeDifferences(t *testing.T) {
	tests := []struct {
		name   string
		digest bool
		mutate func(map[string]any)
	}{
		{name: "digest mismatch", digest: true, mutate: func(record map[string]any) {
			configuration(record)["labels"].(map[string]string)[workloadSpecDigestLabel] = strings.Repeat("0", sha256HexLength)
		}},
		{name: "image", mutate: func(record map[string]any) {
			configuration(record)["image"] = map[string]any{"reference": "foreign.example/image:latest"}
		}},
		{name: "architecture", mutate: func(record map[string]any) {
			configuration(record)["platform"] = map[string]any{"os": "linux", "architecture": "amd64"}
		}},
		{name: "resources", mutate: func(record map[string]any) {
			configuration(record)["resources"] = map[string]any{"cpus": 8, "memoryInBytes": 1 << 30}
		}},
		{name: "arguments", mutate: func(record map[string]any) {
			configuration(record)["initProcess"].(map[string]any)["arguments"] = []string{"unexpected"}
		}},
		{name: "environment", mutate: func(record map[string]any) {
			configuration(record)["initProcess"].(map[string]any)["environment"] = []string{"PATH=/usr/bin", "BASE=image", "EXTRA=foreign"}
		}},
		{name: "ports", mutate: func(record map[string]any) {
			configuration(record)["publishedPorts"] = []map[string]any{{"containerPort": 80, "count": 1, "hostAddress": "127.0.0.1", "hostPort": 9090, "proto": "tcp"}}
		}},
		{name: "dangerous feature", mutate: func(record map[string]any) {
			configuration(record)["mounts"] = []map[string]any{{"source": "/", "destination": "/host"}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := testWorkloadCommand()
			responses := []runnerResponse{
				{stderr: []byte("container already exists"), err: errors.New("exit 1")},
				{stdout: configuredInspectOutput(t, command, "running", ownedLabels(t, command, test.digest), test.mutate)},
			}
			if test.name != "digest mismatch" {
				responses = append(responses, runnerResponse{stdout: imageInspectOutput(t)})
			}
			runner := &scriptedRunner{responses: responses}
			runtime := NewAppleContainer("container")
			runtime.runner = runner
			if err := runtime.Start(context.Background(), command); err == nil || !strings.Contains(err.Error(), "refusing") {
				t.Fatalf("mismatched envelope error = %v", err)
			}
			for _, call := range runner.calls[2:] {
				if len(call) > 0 && (call[0] == "start" || call[0] == "stop" || call[0] == "delete") {
					t.Fatalf("mismatched envelope was mutated: %#v", runner.calls)
				}
			}
		})
	}
}

func TestStopInspectsExactOwnedEnvelopeBeforeStopAndDelete(t *testing.T) {
	command := testWorkloadCommand()
	minimalStop := &apcv1.WorkloadCommand{WorkloadId: command.WorkloadId, ContainerName: command.ContainerName}
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: configuredInspectOutput(t, command, "running", ownedLabels(t, command, true), nil)},
		{stdout: imageInspectOutput(t)},
		{},
		{stdout: configuredInspectOutput(t, command, "stopped", ownedLabels(t, command, true), nil)},
		{},
	}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner
	runtime.rememberDesired(command)
	if err := runtime.Stop(context.Background(), minimalStop); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"inspect", "web-abc"}, {"image", "inspect", "nginx:alpine"}, {"stop", "web-abc"},
		{"inspect", "web-abc"}, {"delete", "web-abc"},
	}
	if !reflect.DeepEqual(runner.calls, want) {
		t.Fatalf("stop calls = %#v, want %#v", runner.calls, want)
	}
}

func TestStopAfterAgentRestartUsesGuardedDigestEnvelope(t *testing.T) {
	command := testWorkloadCommand()
	minimalStop := &apcv1.WorkloadCommand{WorkloadId: command.WorkloadId, ContainerName: command.ContainerName}
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: configuredInspectOutput(t, command, "running", ownedLabels(t, command, true), nil)},
		{},
		{stdout: configuredInspectOutput(t, command, "stopped", ownedLabels(t, command, true), nil)},
		{},
	}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner
	if err := runtime.Stop(context.Background(), minimalStop); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"inspect", "web-abc"}, {"stop", "web-abc"}, {"inspect", "web-abc"}, {"delete", "web-abc"}}
	if !reflect.DeepEqual(runner.calls, want) {
		t.Fatalf("restart stop calls = %#v, want %#v", runner.calls, want)
	}
}

func TestStopFailsClosedForLegacyWithoutDesiredStateOrForeignEnvelope(t *testing.T) {
	command := testWorkloadCommand()
	minimalStop := &apcv1.WorkloadCommand{WorkloadId: command.WorkloadId, ContainerName: command.ContainerName}
	runner := &scriptedRunner{responses: []runnerResponse{{stdout: configuredInspectOutput(t, command, "running", ownedLabels(t, command, false), nil)}}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner
	if err := runtime.Stop(context.Background(), minimalStop); err == nil || !strings.Contains(err.Error(), "digest is missing or invalid") {
		t.Fatalf("unguarded legacy stop error = %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0][0] != "inspect" {
		t.Fatalf("unguarded legacy stop mutated runtime: %#v", runner.calls)
	}

	runner.calls = nil
	runner.responses = []runnerResponse{{stdout: configuredInspectOutput(t, command, "running", map[string]string{}, nil)}}
	if err := runtime.Stop(context.Background(), minimalStop); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("foreign stop error = %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0][0] != "inspect" {
		t.Fatalf("foreign stop mutated runtime: %#v", runner.calls)
	}
}

func TestExecProbeValidatesIdentityBeforeContainerExec(t *testing.T) {
	command := testWorkloadCommand()
	probe := &apcv1.HealthCheck{Type: "exec", TimeoutSeconds: 2, Command: []string{"/bin/check", "ready"}}
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: configuredInspectOutput(t, command, "running", ownedLabels(t, command, true), nil)},
		{stdout: imageInspectOutput(t)},
		{},
	}}
	runtime := NewAppleContainer("container")
	runtime.runner = runner
	if err := runtime.Probe(context.Background(), command, probe, ""); err != nil {
		t.Fatal(err)
	}
	if got := runner.calls[len(runner.calls)-1]; !reflect.DeepEqual(got, []string{"exec", "web-abc", "/bin/check", "ready"}) {
		t.Fatalf("exec call = %#v", got)
	}

	foreignRunner := &scriptedRunner{responses: []runnerResponse{{stdout: configuredInspectOutput(t, command, "running", map[string]string{}, nil)}}}
	runtime = NewAppleContainer("container")
	runtime.runner = foreignRunner
	if err := runtime.Probe(context.Background(), command, probe, ""); err == nil {
		t.Fatal("exec probe accepted a foreign same-name container")
	}
	if len(foreignRunner.calls) != 1 || foreignRunner.calls[0][0] != "inspect" {
		t.Fatalf("foreign exec probe executed command: %#v", foreignRunner.calls)
	}
}

const sha256HexLength = 64

func testWorkloadCommand() *apcv1.WorkloadCommand {
	return &apcv1.WorkloadCommand{
		WorkloadId: "workload-1", ContainerName: "web-abc", Image: "nginx:alpine", Architecture: "arm64",
		Environment: map[string]string{"Z_LAST": "z", "A_FIRST": "a"}, Cpus: 2, Memory: "1G",
		Ports:     []*apcv1.Port{{ContainerPort: 80, HostPort: 8080, HostIp: "127.0.0.1", Protocol: "tcp"}},
		Arguments: []string{"nginx", "-g", "daemon off;"},
	}
}

func ownedLabels(t *testing.T, command *apcv1.WorkloadCommand, includeDigest bool) map[string]string {
	t.Helper()
	labels := map[string]string{"apc.dev/managed": "true", "apc.dev/workload-id": command.WorkloadId}
	if includeDigest {
		digest, err := workloadSpecDigest(command)
		if err != nil {
			t.Fatal(err)
		}
		labels[workloadSpecDigestLabel] = digest
	}
	return labels
}

func configuredInspectOutput(t *testing.T, command *apcv1.WorkloadCommand, status any, labels map[string]string, mutate func(map[string]any)) []byte {
	t.Helper()
	imageProcess := []string{"/entrypoint.sh"}
	if len(command.Arguments) == 0 {
		imageProcess = append(imageProcess, "nginx", "-g", "daemon off;")
	} else {
		imageProcess = append(imageProcess, command.Arguments...)
	}
	environment := make(map[string]string, len(testImageEnvironment)+len(command.Environment))
	for name, value := range testImageEnvironment {
		environment[name] = value
	}
	for name, value := range command.Environment {
		environment[name] = value
	}
	environmentNames := make([]string, 0, len(environment))
	for name := range environment {
		environmentNames = append(environmentNames, name)
	}
	sort.Strings(environmentNames)
	environmentList := make([]string, 0, len(environmentNames))
	for _, name := range environmentNames {
		environmentList = append(environmentList, name+"="+environment[name])
	}
	publishedPorts := make([]map[string]any, 0, len(command.Ports))
	for _, port := range command.Ports {
		if port == nil || port.HostPort == 0 {
			continue
		}
		hostAddress := port.HostIp
		if hostAddress == "" {
			hostAddress = "0.0.0.0"
		}
		protocol := strings.ToLower(port.Protocol)
		if protocol == "" {
			protocol = "tcp"
		}
		publishedPorts = append(publishedPorts, map[string]any{
			"containerPort": port.ContainerPort, "count": 1, "hostAddress": hostAddress,
			"hostPort": port.HostPort, "proto": protocol,
		})
	}
	memory, err := parseRuntimeByteSize(command.Memory)
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{
		"id":     command.ContainerName,
		"status": status,
		"configuration": map[string]any{
			"id": command.ContainerName, "labels": labels,
			"platform": map[string]any{"os": "linux", "architecture": normalizedWorkloadArchitecture(command.Architecture)},
			"image":    map[string]any{"reference": command.Image},
			"initProcess": map[string]any{
				"executable": imageProcess[0], "arguments": imageProcess[1:], "environment": environmentList, "terminal": false,
			},
			"capAdd": []string{}, "capDrop": []string{}, "mounts": []any{},
			"networks":       []map[string]any{{"network": "default"}},
			"publishedPorts": publishedPorts, "publishedSockets": []any{},
			"resources": map[string]any{"cpus": command.Cpus, "memoryInBytes": memory},
			"readOnly":  false, "rosetta": false, "ssh": false, "sysctls": map[string]string{},
			"useInit": false, "virtualization": false,
		},
	}
	if mutate != nil {
		mutate(record)
	}
	data, err := json.Marshal([]map[string]any{record})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func configuration(record map[string]any) map[string]any {
	return record["configuration"].(map[string]any)
}

func imageInspectOutput(t *testing.T) []byte {
	t.Helper()
	environment := []string{"PATH=/usr/bin", "BASE=image"}
	records := []map[string]any{{
		"variants": []map[string]any{{
			"platform": map[string]any{"os": "linux", "architecture": "arm64"},
			"config": map[string]any{"config": map[string]any{
				"Entrypoint": []string{"/entrypoint.sh"}, "Cmd": []string{"nginx", "-g", "daemon off;"}, "Env": environment,
			}},
		}},
	}}
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type stubRunner struct {
	stdout []byte
	stderr []byte
	err    error
}

func (s *stubRunner) Run(context.Context, string, ...string) ([]byte, []byte, error) {
	return s.stdout, s.stderr, s.err
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

func (s *scriptedRunner) Run(_ context.Context, _ string, args ...string) ([]byte, []byte, error) {
	s.calls = append(s.calls, append([]string(nil), args...))
	if len(s.responses) == 0 {
		return nil, nil, fmt.Errorf("unexpected command: %v", args)
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response.stdout, response.stderr, response.err
}
