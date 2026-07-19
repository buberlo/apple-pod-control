package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

func TestDestructiveLifecycleCommandsRequireExplicitConfirmation(t *testing.T) {
	for _, arguments := range [][]string{{"cluster", "delete", "home"}, {"cluster", "restore", "home", "--from", "/tmp/backup"}, {"cluster", "upgrade", "home", "--image", cluster.DefaultK3sImage}, {"node", "remove", "home"}} {
		command := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
		command.SetArgs(arguments)
		err := command.Execute()
		if err == nil || !strings.Contains(err.Error(), "without --yes") {
			t.Fatalf("arguments = %#v, error = %v", arguments, err)
		}
	}
}
