package firewall

import (
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
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
		"<string>--peer</string>\n    <string>192.0.2.30</string>", "<key>RunAtLoad</key>", "<key>StartInterval</key>",
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

func TestVerifyRootFileRejectsSymlinkAndUnexpectedMode(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "helper")
	if err := os.WriteFile(file, []byte("test"), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := verifyRootFile(file, 0o755); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("unexpected mode error = %v", err)
	}
	symlink := filepath.Join(directory, "link")
	if err := os.Symlink(file, symlink); err != nil {
		t.Fatal(err)
	}
	if err := verifyRootFile(symlink, 0o755); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("symlink error = %v", err)
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

func TestReferenceContainsMatchesWholeTokenOnly(t *testing.T) {
	output := "PID  Process  Token\n123  dev.apc.firewall  987654321\n"
	if !referenceContains(output, "987654321") {
		t.Fatal("reference token was not found")
	}
	if referenceContains(output, "7654") {
		t.Fatal("partial token unexpectedly matched")
	}
}
