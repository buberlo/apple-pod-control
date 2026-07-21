package firewall

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestRenderRestrictsClusterPortsToExactPeers(t *testing.T) {
	rules, err := Render(Config{
		Cluster: "home", Role: "server", Interface: "en0", LocalIP: "192.0.2.10",
		Peers: []string{"192.0.2.30", "192.0.2.20", "192.0.2.20"},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(rules)
	for _, required := range []string{
		"{ 192.0.2.20, 192.0.2.30 }", "proto tcp", "port { 16443, 10250 }",
		"proto udp", "port 8472", "block in quick log", "to 192.0.2.10",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("rules missing %q:\n%s", required, text)
		}
	}
	if _, err := os.Stat("/sbin/pfctl"); err != nil {
		t.Skip("pfctl is only available on macOS runners")
	}
	if err := Validate(context.Background(), rules); err != nil {
		t.Fatalf("pfctl rejected generated rules: %v\n%s", err, text)
	}
}

func TestNormalizeRejectsUnsafeOrAmbiguousInputs(t *testing.T) {
	for _, config := range []Config{
		{Cluster: "../home", Role: "server", LocalIP: "192.0.2.10", Peers: []string{"192.0.2.20"}},
		{Cluster: "home", Role: "root", LocalIP: "192.0.2.10", Peers: []string{"192.0.2.20"}},
		{Cluster: "home", Role: "server", Interface: "en0;block", LocalIP: "192.0.2.10", Peers: []string{"192.0.2.20"}},
		{Cluster: "home", Role: "server", LocalIP: "192.0.2.10", Peers: []string{"192.0.2.10"}},
	} {
		if _, err := normalize(config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
}

func TestRenderCanResolveInterfaceFromLocalAddress(t *testing.T) {
	rules, err := Render(Config{Cluster: "home", Role: "agent", Interface: "auto", LocalIP: "127.0.0.1", Peers: []string{"192.0.2.20"}})
	if err != nil {
		t.Fatal(err)
	}
	name, err := interfaceForIP("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rules), " on "+name+" ") {
		t.Fatalf("rules did not use resolved interface %q:\n%s", name, rules)
	}
}
