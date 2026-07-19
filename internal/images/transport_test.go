package images

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

type recordedCall struct {
	binary    string
	arguments []string
	hasInput  bool
}

type fakeRunner struct {
	calls []recordedCall
}

func (r *fakeRunner) Run(_ context.Context, stdin io.Reader, stdout, _ io.Writer, binary string, arguments ...string) error {
	r.calls = append(r.calls, recordedCall{binary: binary, arguments: append([]string(nil), arguments...), hasInput: stdin != nil})
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
	if len(runner.calls) != 6 {
		t.Fatalf("calls = %d, want 6: %#v", len(runner.calls), runner.calls)
	}
	if !runner.calls[2].hasInput || !runner.calls[4].hasInput {
		t.Fatalf("imports were not streamed: %#v", runner.calls)
	}
	if !strings.Contains(strings.Join(runner.calls[4].arguments, " "), "apc-k3s-home-agent") {
		t.Fatalf("remote agent target missing: %#v", runner.calls[4])
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
