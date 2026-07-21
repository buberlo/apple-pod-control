package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

func TestKubernetesPassthroughUsesExplicitClusterAndPreservesKubectlArguments(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	handled, err := tryKubernetesPassthrough(
		context.Background(),
		[]string{"--cluster", "home", "get", "pods", "-A", "-o", "wide"},
		nil, io.Discard, io.Discard, env,
	)
	if err != nil || !handled {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if !reflect.DeepEqual(captured.arguments, []string{"get", "pods", "-A", "-o", "wide"}) {
		t.Fatalf("arguments = %#v", captured.arguments)
	}
	if captured.cluster != "home" {
		t.Fatalf("cluster = %q", captured.cluster)
	}
}

func TestKubernetesPassthroughAcceptsClusterFlagAfterVerb(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	handled, err := tryKubernetesPassthrough(
		context.Background(),
		[]string{"get", "pods", "--cluster", "ha-lab", "-o", "wide"},
		nil, io.Discard, io.Discard, env,
	)
	if err != nil || !handled {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if captured.cluster != "ha-lab" || !reflect.DeepEqual(captured.arguments, []string{"get", "pods", "-o", "wide"}) {
		t.Fatalf("cluster = %q, arguments = %#v", captured.cluster, captured.arguments)
	}
}

func TestKubernetesPassthroughPreservesClusterTextAfterDoubleDash(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	handled, err := tryKubernetesPassthrough(
		context.Background(),
		[]string{"exec", "pod/web", "--", "echo", "--cluster", "literal"},
		nil, io.Discard, io.Discard, env,
	)
	if err != nil || !handled {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if captured.cluster != "current" || !reflect.DeepEqual(captured.arguments, []string{"exec", "pod/web", "--", "echo", "--cluster", "literal"}) {
		t.Fatalf("cluster = %q, arguments = %#v", captured.cluster, captured.arguments)
	}
}

func TestKubernetesPassthroughUsesCurrentCluster(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	handled, err := tryKubernetesPassthrough(context.Background(), []string{"apply", "-f", "deployment.yaml"}, nil, io.Discard, io.Discard, env)
	if err != nil || !handled {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if captured.cluster != "current" {
		t.Fatalf("cluster = %q", captured.cluster)
	}
}

func TestKubernetesPassthroughPreparesHAEndpointBeforeKubectl(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	prepared := false
	env.prepareKubeconfig = func(_ context.Context, name string) (string, error) {
		prepared = true
		return env.kubeconfigPath(name)
	}
	handled, err := tryKubernetesPassthrough(context.Background(), []string{"--cluster", "ha-lab", "get", "pods"}, nil, io.Discard, io.Discard, env)
	if err != nil || !handled {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if !prepared || captured.cluster != "ha-lab" {
		t.Fatalf("prepare called = %t, cluster = %q", prepared, captured.cluster)
	}
}

func TestKubernetesPassthroughForwardsFutureCommandsAndPlugins(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	handled, err := tryKubernetesPassthrough(context.Background(), []string{"example-plugin", "--future-flag"}, nil, io.Discard, io.Discard, env)
	if err != nil || !handled {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if !reflect.DeepEqual(captured.arguments, []string{"example-plugin", "--future-flag"}) {
		t.Fatalf("arguments = %#v", captured.arguments)
	}
}

func TestKubernetesPassthroughLeavesAPCAndCompletionCommandsToCobra(t *testing.T) {
	env, captured := passthroughTestEnvironment(t)
	for _, arguments := range [][]string{{"cluster", "status"}, {"config", "current-cluster"}, {"completion", "zsh"}, {"__complete", "cluster", "st"}, {"__completeNoDesc", "config", "current-"}} {
		handled, err := tryKubernetesPassthrough(context.Background(), arguments, nil, io.Discard, io.Discard, env)
		if err != nil || handled {
			t.Fatalf("arguments = %#v, handled = %t, err = %v", arguments, handled, err)
		}
	}
	if captured.calls != 0 {
		t.Fatalf("kubectl calls = %d", captured.calls)
	}
}

func TestKubernetesPassthroughRequiresSelectedCluster(t *testing.T) {
	env, _ := passthroughTestEnvironment(t)
	env.currentCluster = func() (string, error) { return "", cluster.ErrNoCurrentCluster }
	handled, err := tryKubernetesPassthrough(context.Background(), []string{"get", "pods"}, nil, io.Discard, io.Discard, env)
	if !handled || err == nil {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
	if !errors.Is(err, errNoSelectedCluster) {
		t.Fatalf("error = %v, want no-selected-cluster sentinel", err)
	}
	if message := err.Error(); !strings.Contains(message, "apc config use-cluster NAME") || !strings.Contains(message, "--cluster NAME") {
		t.Fatalf("error = %q, want actionable cluster selection guidance", message)
	}
}

type passthroughCapture struct {
	calls     int
	cluster   string
	arguments []string
}

func passthroughTestEnvironment(t *testing.T) (passthroughEnvironment, *passthroughCapture) {
	t.Helper()
	directory := t.TempDir()
	capture := &passthroughCapture{}
	return passthroughEnvironment{
		getenv:         func(string) string { return "" },
		lookPath:       func(string) (string, error) { return "/usr/local/bin/kubectl", nil },
		currentCluster: func() (string, error) { return "current", nil },
		kubeconfigPath: func(name string) (string, error) {
			capture.cluster = name
			path := filepath.Join(directory, name)
			if err := os.WriteFile(path, []byte("apiVersion: v1\n"), 0o600); err != nil {
				return "", err
			}
			return path, nil
		},
		stat: os.Stat,
		run: func(_ context.Context, _ string, arguments []string, _ string, _ io.Reader, _, _ io.Writer) error {
			capture.calls++
			capture.arguments = append([]string(nil), arguments...)
			return nil
		},
	}, capture
}

func TestKubernetesPassthroughReportsMissingKubectl(t *testing.T) {
	env, _ := passthroughTestEnvironment(t)
	env.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	handled, err := tryKubernetesPassthrough(context.Background(), []string{"get", "pods"}, nil, io.Discard, io.Discard, env)
	if !handled || err == nil {
		t.Fatalf("handled = %t, err = %v", handled, err)
	}
}
