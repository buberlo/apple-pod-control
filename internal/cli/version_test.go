package cli

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestVersionIsLocalOnly(t *testing.T) {
	var output bytes.Buffer
	command := NewCommand(&output, &bytes.Buffer{})
	command.SetArgs([]string{"version"})

	if err := command.Execute(); err != nil {
		t.Fatalf("execute version: %v", err)
	}
	if got, want := output.String(), fmt.Sprintf("APC Version: %s\n", Version); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestVersionRejectsRemovedControlPlaneFlags(t *testing.T) {
	for _, arguments := range [][]string{
		{"--server", "https://removed.invalid", "version"},
		{"--token", "not-used", "version"},
		{"version", "--client"},
	} {
		t.Run(strings.Join(arguments, "_"), func(t *testing.T) {
			command := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
			command.SetArgs(arguments)
			if err := command.Execute(); err == nil || !strings.Contains(err.Error(), "unknown flag") {
				t.Fatalf("error = %v, want removed flag to be rejected", err)
			}
		})
	}
}
