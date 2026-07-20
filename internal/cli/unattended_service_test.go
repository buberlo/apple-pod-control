package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestUnattendedServiceCommandsRequireExplicitUserAndConfirmation(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "install confirmation", args: []string{"system", "install", "--unattended", "--user", "apc"}, want: "without --yes"},
		{name: "install user", args: []string{"system", "install", "--unattended", "--yes"}, want: "--user is required"},
		{name: "uninstall confirmation", args: []string{"system", "uninstall", "--unattended", "--user", "apc"}, want: "without explicit confirmation"},
		{name: "status user", args: []string{"system", "status", "--unattended"}, want: "--user is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
			command.SetArgs(test.args)
			if err := command.Execute(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}
