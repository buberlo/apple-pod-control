package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestHelmCommandUsesSelectedProtectedKubeconfigAndForwardsArguments(t *testing.T) {
	directory := t.TempDir()
	kubeconfig := filepath.Join(directory, "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotBinary, gotKubeconfig string
	var gotArguments []string
	previous := newHelmEnvironment
	newHelmEnvironment = func() helmEnvironment {
		return helmEnvironment{
			lookPath: func(name string) (string, error) {
				if name != "helm" {
					t.Fatalf("lookPath(%q)", name)
				}
				return "/opt/homebrew/bin/helm", nil
			},
			prepareKubeconfig: func(_ context.Context, name string) (string, error) {
				if name != "ha-lab" {
					t.Fatalf("cluster = %q", name)
				}
				return kubeconfig, nil
			},
			stat: os.Stat,
			run: func(_ context.Context, binary string, arguments []string, path string, _ io.Reader, _, _ io.Writer) error {
				gotBinary, gotKubeconfig = binary, path
				gotArguments = append([]string(nil), arguments...)
				return nil
			},
		}
	}
	t.Cleanup(func() { newHelmEnvironment = previous })

	command := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
	command.SetArgs([]string{"--cluster", "ha-lab", "helm", "upgrade", "--install", "web", "./chart", "--wait"})
	if err := command.Execute(); err != nil {
		t.Fatalf("helm command: %v", err)
	}
	if gotBinary != "/opt/homebrew/bin/helm" || gotKubeconfig != kubeconfig {
		t.Fatalf("run binary=%q kubeconfig=%q", gotBinary, gotKubeconfig)
	}
	want := []string{"upgrade", "--install", "web", "./chart", "--wait"}
	if !reflect.DeepEqual(gotArguments, want) {
		t.Fatalf("arguments = %#v, want %#v", gotArguments, want)
	}
}

func TestHelmCommandRejectsReadableKubeconfigBeforeLookupOrRun(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	previous := newHelmEnvironment
	newHelmEnvironment = func() helmEnvironment {
		return helmEnvironment{
			prepareKubeconfig: func(context.Context, string) (string, error) { return kubeconfig, nil },
			stat:              os.Stat,
			lookPath:          func(string) (string, error) { t.Fatal("lookPath called"); return "", errors.New("unexpected") },
			run: func(context.Context, string, []string, string, io.Reader, io.Writer, io.Writer) error {
				t.Fatal("run called")
				return nil
			},
		}
	}
	t.Cleanup(func() { newHelmEnvironment = previous })

	command := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
	command.SetArgs([]string{"--cluster", "ha-lab", "helm", "list"})
	err := command.Execute()
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("mode 0600")) {
		t.Fatalf("error = %v", err)
	}
}

func TestKubernetesRouterReservesHelmForCobra(t *testing.T) {
	_, _, kubernetesCommand, legacy, err := routeKubernetesArguments([]string{"helm", "list"})
	if err != nil || kubernetesCommand || legacy {
		t.Fatalf("route helm: kubernetes=%t legacy=%t err=%v", kubernetesCommand, legacy, err)
	}
}

func TestHelmSelectedClusterAcceptsSelectorAfterSubcommand(t *testing.T) {
	name, arguments := helmSelectedCluster("current", []string{"list", "--cluster", "ha-lab", "--all-namespaces"})
	if name != "ha-lab" || !reflect.DeepEqual(arguments, []string{"list", "--all-namespaces"}) {
		t.Fatalf("selection = %q, arguments = %#v", name, arguments)
	}
}
