package firewall

import (
	"encoding/xml"
	"io"
	"strings"
	"testing"
)

func TestRenderLaunchDaemonUsesRootOwnedHelperAndExactConfiguration(t *testing.T) {
	config, err := normalize(Config{
		Cluster: "home", Role: "server", Interface: "en0", LocalIP: "192.0.2.10",
		Peers: []string{"192.0.2.30", "192.0.2.20"},
	})
	if err != nil {
		t.Fatal(err)
	}
	plist := string(RenderLaunchDaemon(config))
	for _, required := range []string{
		"dev.apc.firewall.home", helperPath, "<string>--local-ip</string>", "<string>192.0.2.10</string>",
		"<string>--peer</string>\n    <string>192.0.2.20</string>",
		"<string>--peer</string>\n    <string>192.0.2.30</string>", "<key>RunAtLoad</key>",
	} {
		if !strings.Contains(plist, required) {
			t.Fatalf("plist missing %q:\n%s", required, plist)
		}
	}
	decoder := xml.NewDecoder(strings.NewReader(plist))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("plist is not well-formed XML: %v", err)
		}
	}
}

func TestSafeTokenAcceptsOnlyDecimalPFReference(t *testing.T) {
	for _, valid := range []string{"1", "123456789\n"} {
		if !safeToken(valid) {
			t.Fatalf("token %q should be valid", valid)
		}
	}
	for _, invalid := range []string{"", "-1", "1; pfctl -d", "abc"} {
		if safeToken(invalid) {
			t.Fatalf("token %q should be invalid", invalid)
		}
	}
}
