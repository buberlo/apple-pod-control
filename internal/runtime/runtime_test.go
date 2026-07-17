package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
)

func TestRunArgumentsAreDeterministicAndNative(t *testing.T) {
	command := &apcv1.WorkloadCommand{
		WorkloadId: "workload-1", ContainerName: "web-abc", Image: "nginx:alpine", Architecture: "arm64",
		Environment: map[string]string{"Z_LAST": "z", "A_FIRST": "a"}, Cpus: 2, Memory: "1G",
		Ports:     []*apcv1.Port{{ContainerPort: 80, HostPort: 8080, HostIp: "127.0.0.1", Protocol: "tcp"}},
		Arguments: []string{"nginx", "-g", "daemon off;"},
	}
	actual, err := RunArguments(command)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{
		"run", "--detach", "--name", "web-abc", "--arch", "arm64",
		"--label", "apc.dev/managed=true", "--label", "apc.dev/workload-id=workload-1", "--progress", "plain",
		"--cpus", "2", "--memory", "1G", "--env", "A_FIRST=a", "--env", "Z_LAST=z",
		"--publish", "127.0.0.1:8080:80/tcp", "nginx:alpine", "nginx", "-g", "daemon off;",
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("arguments mismatch\nactual: %#v\nexpected: %#v", actual, expected)
	}
}

func TestObserveParsesAppleContainerInspect(t *testing.T) {
	runner := &stubRunner{stdout: []byte(`[{"status":"running","networks":[{"address":"192.168.64.3/24"}],"configuration":{"id":"web"}}]`)}
	runtime := NewAppleContainer("container")
	runtime.runner = runner
	observation, err := runtime.Observe(context.Background(), &apcv1.WorkloadCommand{ContainerName: "web"})
	if err != nil {
		t.Fatal(err)
	}
	if observation.State != "Running" || !observation.Ready || observation.Address != "192.168.64.3" {
		t.Fatalf("unexpected observation: %#v", observation)
	}
}

type stubRunner struct {
	stdout []byte
	stderr []byte
	err    error
}

func (s *stubRunner) Run(context.Context, string, ...string) ([]byte, []byte, error) {
	return s.stdout, s.stderr, s.err
}

func TestObserveMapsNotFound(t *testing.T) {
	runtime := NewAppleContainer("container")
	runtime.runner = &stubRunner{stderr: []byte("container not found"), err: errors.New("exit 1")}
	_, err := runtime.Observe(context.Background(), &apcv1.WorkloadCommand{ContainerName: "missing"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
}
