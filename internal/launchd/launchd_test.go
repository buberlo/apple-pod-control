package launchd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRenderPlistUsesStableSupervisorArgumentsAndEscapesPaths(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "apc&test")
	if err := os.WriteFile(executable, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	config, err := normalizeConfig(Config{Role: "server", Cluster: "home", Executable: executable, Interval: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	plist := string(RenderPlist(config, "/tmp/apc&home.log"))
	for _, required := range []string{"dev.apc.server.home", "<string>system</string>", "<string>supervise</string>", "<string>20s</string>", "apc&amp;test", "apc&amp;home.log", "<key>SuccessfulExit</key>", "<key>LimitLoadToSessionType</key>", "<string>Background</string>"} {
		if !strings.Contains(plist, required) {
			t.Fatalf("plist missing %q:\n%s", required, plist)
		}
	}
}

func TestNormalizeConfigRejectsUnsafeRoleNameAndFastLoop(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "apc")
	if err := os.WriteFile(executable, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, config := range []Config{
		{Role: "root", Cluster: "home", Executable: executable},
		{Role: "server", Cluster: "../home", Executable: executable},
		{Role: "server", Cluster: "home", Executable: executable, Interval: time.Second},
	} {
		if _, err := normalizeConfig(config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
}
