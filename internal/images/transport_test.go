package images

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type recordedCall struct {
	binary    string
	arguments []string
	hasInput  bool
	input     []byte
}

type fakeRunner struct {
	calls []recordedCall
}

func (r *fakeRunner) Run(_ context.Context, stdin io.Reader, stdout, _ io.Writer, binary string, arguments ...string) error {
	var input []byte
	var err error
	if stdin != nil {
		input, err = io.ReadAll(stdin)
		if err != nil {
			return err
		}
	}
	r.calls = append(r.calls, recordedCall{binary: binary, arguments: append([]string(nil), arguments...), hasInput: stdin != nil, input: input})
	if len(arguments) >= 2 && arguments[0] == "image" && arguments[1] == "save" {
		for index, argument := range arguments {
			if argument == "--output" && index+1 < len(arguments) {
				return os.WriteFile(arguments[index+1], []byte("oci-archive"), 0o600)
			}
		}
	}
	for _, argument := range arguments {
		if strings.HasPrefix(argument, "name==") {
			_, err := fmt.Fprintln(stdout, strings.TrimPrefix(argument, "name=="))
			return err
		}
	}
	return nil
}

func TestTransferPullsOnceAndStreamsArchiveToLocalAndRemoteK3s(t *testing.T) {
	setImagesConfigHome(t)
	runner := &fakeRunner{}
	manager := NewManager()
	manager.runner = runner
	result, err := manager.Transfer(context.Background(), Options{
		Cluster: "home", Images: []string{"example.test/web:v1", "example.test/web:v1"},
		Peers: []string{"user@mini.local"}, Pull: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Images) != 1 || len(result.Targets) != 2 || result.ArchiveBytes == 0 {
		t.Fatalf("unexpected result: %#v", result)
	}
	wantTargets := []string{"apc-k3s-home-server", "user@mini.local/apc-k3s-home-agent"}
	if !reflect.DeepEqual(result.Targets, wantTargets) {
		t.Fatalf("targets = %#v, want %#v", result.Targets, wantTargets)
	}
	if len(runner.calls) != 6 {
		t.Fatalf("calls = %d, want 6: %#v", len(runner.calls), runner.calls)
	}
	if !runner.calls[2].hasInput || !runner.calls[4].hasInput {
		t.Fatalf("imports were not streamed: %#v", runner.calls)
	}
	if !strings.Contains(strings.Join(runner.calls[4].arguments, " "), "apc-k3s-home-agent") {
		t.Fatalf("remote agent target missing: %#v", runner.calls[4])
	}
	if !strings.Contains(strings.Join(runner.calls[4].arguments, " "), "BatchMode=yes") {
		t.Fatalf("remote import does not enforce key-only SSH: %#v", runner.calls[4])
	}
}

func TestTransferExportsOnceAndImportsEveryResolvedHAMember(t *testing.T) {
	runner := &fakeRunner{}
	manager := NewManager()
	manager.runner = runner
	manager.resolveTargets = func(context.Context, string, string) ([]string, error) {
		return []string{
			"apc-k3s-ha-lab-server-1",
			"apc-k3s-ha-lab-server-2",
			"apc-k3s-ha-lab-server-3",
		}, nil
	}
	result, err := manager.Transfer(context.Background(), Options{
		Cluster: "ha-lab", Images: []string{"example.test/web:v1"}, Pull: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantTargets := []string{
		"apc-k3s-ha-lab-server-1",
		"apc-k3s-ha-lab-server-2",
		"apc-k3s-ha-lab-server-3",
	}
	if !reflect.DeepEqual(result.Targets, wantTargets) || result.ArchiveBytes == 0 {
		t.Fatalf("unexpected result: %#v", result)
	}

	saves := 0
	imports := 0
	for _, call := range runner.calls {
		if len(call.arguments) >= 2 && call.arguments[0] == "image" && call.arguments[1] == "save" {
			saves++
		}
		if len(call.arguments) >= 4 && call.arguments[0] == "exec" && call.arguments[1] == "-i" {
			imports++
			if string(call.input) != "oci-archive" {
				t.Fatalf("target received different archive: %q", call.input)
			}
		}
	}
	if saves != 1 || imports != 3 {
		t.Fatalf("save calls = %d, import calls = %d; all calls: %#v", saves, imports, runner.calls)
	}
}

func TestTransferResolutionFailureMakesNoImportOrHostMutation(t *testing.T) {
	runner := &fakeRunner{}
	manager := NewManager()
	manager.runner = runner
	manager.resolveTargets = func(context.Context, string, string) ([]string, error) {
		return nil, errors.New("HA member 3 is missing")
	}
	result, err := manager.Transfer(context.Background(), Options{
		Cluster: "ha-lab", Images: []string{"example.test/web:v1"}, Pull: true,
	})
	if err == nil || !strings.Contains(err.Error(), "member 3") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("commands ran before complete target resolution: %#v", runner.calls)
	}
}

func TestNormalizeRejectsUnsafeInputs(t *testing.T) {
	for _, options := range []Options{
		{Cluster: "HOME", Images: []string{"web:v1"}},
		{Cluster: "home", Images: []string{"--help"}},
		{Cluster: "home", Images: []string{"web:v1;false"}},
		{Cluster: "home", Images: []string{"web:v1"}, Peers: []string{"user@mini;false"}},
		{Cluster: "home", Images: []string{"web:v1"}, Platform: "arm64"},
	} {
		if _, err := normalize(options); err == nil {
			t.Fatalf("unsafe options accepted: %#v", options)
		}
	}
}

func setImagesConfigHome(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
}
