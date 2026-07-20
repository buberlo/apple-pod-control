// Package buildinfo owns the version reported by the APC command-line client.
package buildinfo

import "strings"

// Version may be replaced at build time with:
//
//	-ldflags "-X github.com/buberlo/apple-pod-control/internal/buildinfo.Version=vX.Y.Z"
var Version = "v0.2.0-alpha.2"

func Current() string {
	if value := strings.TrimSpace(Version); value != "" {
		return value
	}
	return "devel"
}
