package overlay

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

type fakeRunner struct {
	responses map[string][]byte
}

func (f fakeRunner) Run(_ context.Context, binary string, arguments ...string) ([]byte, error) {
	key := binary + " " + strings.Join(arguments, " ")
	value, ok := f.responses[key]
	if !ok {
		return nil, fmt.Errorf("unexpected command %s", key)
	}
	return value, nil
}

func TestCheckVerifiesOnlinePeerAndExactRoute(t *testing.T) {
	checker := &Checker{
		findCLI: func() (string, error) { return "/tailscale", nil },
		findInterface: func(ip string) (string, error) {
			if ip != "100.64.0.10" {
				t.Fatalf("unexpected local IP %s", ip)
			}
			return "utun7", nil
		},
		runner: fakeRunner{responses: map[string][]byte{
			"/tailscale status --json":       []byte(`{"BackendState":"Running","Self":{"Online":true,"TailscaleIPs":["100.64.0.10"]},"Peer":{"node":{"Online":true,"TailscaleIPs":["100.64.0.20"]}}}`),
			"/sbin/route -n get 100.64.0.20": []byte("   route to: 100.64.0.20\n  interface: utun7\n"),
		}},
	}
	status, err := checker.Check(context.Background(), Config{Provider: "tailscale", Interface: "auto", PeerIP: "100.64.0.20"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Interface != "utun7" || status.LocalIP != "100.64.0.10" || !status.PeerOnline {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestCheckRejectsOfflinePeerAndNonTailscaleAddresses(t *testing.T) {
	checker := &Checker{
		findCLI:       func() (string, error) { return "/tailscale", nil },
		findInterface: func(string) (string, error) { return "utun7", nil },
		runner: fakeRunner{responses: map[string][]byte{
			"/tailscale status --json": []byte(`{"BackendState":"Running","Self":{"Online":true,"TailscaleIPs":["100.64.0.10"]},"Peer":{"node":{"Online":false,"TailscaleIPs":["100.64.0.20"]}}}`),
		}},
	}
	if _, err := checker.Check(context.Background(), Config{PeerIP: "192.0.2.20"}); err == nil || !strings.Contains(err.Error(), "100.64.0.0/10") {
		t.Fatalf("non-Tailscale peer error = %v", err)
	}
	if _, err := checker.Check(context.Background(), Config{PeerIP: "100.64.0.20"}); err == nil || !strings.Contains(err.Error(), "offline") {
		t.Fatalf("offline peer error = %v", err)
	}
}
