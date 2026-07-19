package cluster

import (
	"strings"
	"testing"
)

func TestUpgradeRequiresImmutableImageDigest(t *testing.T) {
	for _, image := range []string{"rancher/k3s:latest", "rancher/k3s@sha256:short", "rancher/k3s@sha512:" + strings.Repeat("a", 64)} {
		if immutableImageReference.MatchString(image) {
			t.Fatalf("mutable or invalid image accepted: %q", image)
		}
	}
	valid := "docker.io/rancher/k3s@sha256:" + strings.Repeat("a", 64)
	if !immutableImageReference.MatchString(valid) {
		t.Fatalf("valid digest rejected: %q", valid)
	}
}
