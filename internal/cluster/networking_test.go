package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNetworkPolicyConfigurationRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	config, err := normalizeConfig(Config{Name: "home", EnableNetworkPolicy: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := saveClusterConfig(config); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadClusterConfig("home")
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.EnableNetworkPolicy {
		t.Fatal("enabled NetworkPolicy setting was not persisted")
	}
	if _, err := os.Stat(config.KubeconfigPath); err == nil {
		t.Fatal("config round-trip unexpectedly created kubeconfig")
	}
}
