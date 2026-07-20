package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

type fakeHAManager struct {
	createConfig cluster.HAConfig
	createState  cluster.HAState
	statusState  cluster.HAState
	startState   cluster.HAState
	deletedName  string
	keepData     bool
	deleteCalls  int
	snapshotName string
	snapshotPath string
	snapshot     cluster.HASnapshotResult
	restoreName  string
	restorePath  string
	restoreCalls int
	memberName   string
	memberID     int
	memberOp     string
}

func (m *fakeHAManager) CreateHA(_ context.Context, config cluster.HAConfig) (cluster.HAState, error) {
	m.createConfig = config
	return m.createState, nil
}

func (m *fakeHAManager) StatusHA(context.Context, string) (cluster.HAState, error) {
	return m.statusState, nil
}

func (m *fakeHAManager) StartHA(context.Context, string, time.Duration) (cluster.HAState, error) {
	return m.startState, nil
}

func (*fakeHAManager) StopHA(context.Context, string) error {
	return nil
}

func (m *fakeHAManager) DeleteHA(_ context.Context, name string, keepData bool) error {
	m.deletedName = name
	m.keepData = keepData
	m.deleteCalls++
	return nil
}

func (m *fakeHAManager) SnapshotHA(_ context.Context, name, output string) (cluster.HASnapshotResult, error) {
	m.snapshotName = name
	m.snapshotPath = output
	return m.snapshot, nil
}

func (m *fakeHAManager) RestoreHA(_ context.Context, name, input string, _ time.Duration) (cluster.HAState, error) {
	m.restoreName = name
	m.restorePath = input
	m.restoreCalls++
	return m.startState, nil
}

func (m *fakeHAManager) StopHAMember(_ context.Context, name string, id int, _ time.Duration) (cluster.HAState, error) {
	m.memberName, m.memberID, m.memberOp = name, id, "stop"
	return m.statusState, nil
}

func (m *fakeHAManager) StartHAMember(_ context.Context, name string, id int, _ time.Duration) (cluster.HAState, error) {
	m.memberName, m.memberID, m.memberOp = name, id, "start"
	return m.statusState, nil
}

func (m *fakeHAManager) RestartHAMember(_ context.Context, name string, id int, _ time.Duration) (cluster.HAState, error) {
	m.memberName, m.memberID, m.memberOp = name, id, "restart"
	return m.statusState, nil
}

func (*fakeHAManager) ServeHAProxy(context.Context, string) error { return nil }

func TestClusterHACommandRegistersLifecycleCommands(t *testing.T) {
	command := (&options{}).clusterHACommand()
	want := []string{"create", "delete", "member", "proxy", "restore", "snapshot", "start", "status", "stop"}
	got := make([]string, 0, len(command.Commands()))
	for _, child := range command.Commands() {
		got = append(got, child.Name())
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("subcommands = %v, want %v", got, want)
	}
}

func TestClusterHASnapshotForwardsProtectedOutput(t *testing.T) {
	fake := &fakeHAManager{snapshot: cluster.HASnapshotResult{Path: "/secure/snapshot", Bytes: 42, DataSHA256: "abc", Warning: "in-cluster cleanup pending"}}
	previous := newHAManager
	newHAManager = func() haManager { return fake }
	t.Cleanup(func() { newHAManager = previous })

	var output, errorOutput bytes.Buffer
	command := (&options{out: &output, errOut: &errorOutput}).clusterHACommand()
	command.SetArgs([]string{"snapshot", "ha-lab", "--output", "/secure/snapshot"})
	if err := command.Execute(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if fake.snapshotName != "ha-lab" || fake.snapshotPath != "/secure/snapshot" {
		t.Fatalf("snapshot call = cluster %q, path %q", fake.snapshotName, fake.snapshotPath)
	}
	if !strings.Contains(output.String(), "snapshot.apc.dev created: /secure/snapshot (42 bytes") || strings.Contains(output.String(), "token") {
		t.Fatalf("unexpected snapshot output: %s", output.String())
	}
	if !strings.Contains(errorOutput.String(), "warning: in-cluster cleanup pending") {
		t.Fatalf("snapshot warning was not surfaced: %s", errorOutput.String())
	}
}

func TestClusterHARestoreRequiresConfirmationBeforeManagerCall(t *testing.T) {
	fake := &fakeHAManager{}
	previous := newHAManager
	newHAManager = func() haManager { return fake }
	t.Cleanup(func() { newHAManager = previous })

	command := (&options{out: &bytes.Buffer{}, errOut: &bytes.Buffer{}}).clusterHACommand()
	command.SetArgs([]string{"restore", "ha-lab", "--from", "/secure/snapshot"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "without --yes") || fake.restoreCalls != 0 {
		t.Fatalf("restore error = %v, calls = %d", err, fake.restoreCalls)
	}
}

func TestClusterHAMemberRestartRequiresConfirmationAndTargetsExactMember(t *testing.T) {
	fake := &fakeHAManager{statusState: cluster.HAState{Name: "ha-lab", ReadyMembers: 3}}
	previous := newHAManager
	newHAManager = func() haManager { return fake }
	t.Cleanup(func() { newHAManager = previous })

	command := (&options{out: &bytes.Buffer{}, errOut: &bytes.Buffer{}}).clusterHACommand()
	command.SetArgs([]string{"member", "restart", "2", "ha-lab"})
	if err := command.Execute(); err == nil || !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("unconfirmed restart error = %v", err)
	}
	if fake.memberOp != "" {
		t.Fatalf("member was mutated before confirmation: %s", fake.memberOp)
	}

	command = (&options{out: &bytes.Buffer{}, errOut: &bytes.Buffer{}}).clusterHACommand()
	command.SetArgs([]string{"member", "restart", "2", "ha-lab", "--yes"})
	if err := command.Execute(); err != nil {
		t.Fatalf("confirmed restart: %v", err)
	}
	if fake.memberOp != "restart" || fake.memberName != "ha-lab" || fake.memberID != 2 {
		t.Fatalf("member call = %s %s/%d", fake.memberOp, fake.memberName, fake.memberID)
	}
}

func TestRootCommandExposesClusterHA(t *testing.T) {
	command := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
	command.SetArgs([]string{"cluster", "ha", "create"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "received 0") {
		t.Fatalf("root HA create error = %v", err)
	}
}

func TestHAConfigForCreateBuildsThreeStableMembers(t *testing.T) {
	config, err := haConfigForCreate("ha-lab", haCreateOptions{
		networkName:    "apc-ha-lab",
		subnet:         "192.168.96.0/24",
		stableIP:       "192.168.96.11",
		apiPortBase:    17443,
		image:          cluster.DefaultK3sImage,
		cpus:           2,
		memory:         "2G",
		volumeSize:     "8G",
		listen:         "127.0.0.1",
		wait:           3 * time.Minute,
		disableIngress: true,
	})
	if err != nil {
		t.Fatalf("build config: %v", err)
	}
	if config.Name != "ha-lab" || config.NetworkName != "apc-ha-lab" || len(config.Members) != 3 {
		t.Fatalf("unexpected config: %+v", config)
	}
	for index, member := range config.Members {
		id := index + 1
		if member.ID != id {
			t.Errorf("member %d ID = %d", index, member.ID)
		}
		if member.StableIP != []string{"192.168.96.11", "192.168.96.12", "192.168.96.13"}[index] {
			t.Errorf("member %d stable IP = %q", index, member.StableIP)
		}
		if member.HostAPIPort != 17443+index {
			t.Errorf("member %d API port = %d", index, member.HostAPIPort)
		}
		if member.MAC != []string{"02:ac:96:00:00:01", "02:ac:96:00:00:02", "02:ac:96:00:00:03"}[index] {
			t.Errorf("member %d MAC = %q", index, member.MAC)
		}
		if member.NodeName == "" {
			t.Errorf("member %d has no node name", index)
		}
	}
	if config.TokenFile == "" {
		t.Fatal("private token file path was not assigned")
	}
}

func TestBuildHAMembersRejectsAddressesOutsideUsableSubnet(t *testing.T) {
	_, err := buildHAMembers("ha-lab", nil, "192.168.96.0/24", "192.168.96.254", 17443)
	if err == nil || !strings.Contains(err.Error(), "not usable") {
		t.Fatalf("build members error = %v", err)
	}
}

func TestPrintHAStatusSortsMembersAndDoesNotExposeSecrets(t *testing.T) {
	state := cluster.HAState{
		Name:         "ha-lab",
		NetworkName:  "apc-ha-lab",
		ReadyMembers: 3,
		Healthy:      true,
		Members: []cluster.HAMemberState{
			{ID: 3, NodeName: "apc-ha-3", RuntimeState: "running", StableIP: "192.168.96.13", APIEndpoint: "https://127.0.0.1:17445", NodeReady: true},
			{ID: 1, NodeName: "apc-ha-1", RuntimeState: "running", StableIP: "192.168.96.11", APIEndpoint: "https://127.0.0.1:17443", NodeReady: true},
			{ID: 2, NodeName: "apc-ha-2", RuntimeState: "running", StableIP: "192.168.96.12", APIEndpoint: "https://127.0.0.1:17444", NodeReady: true},
		},
	}
	var wide bytes.Buffer
	if err := printHAStatus(&wide, state, "wide"); err != nil {
		t.Fatalf("print wide status: %v", err)
	}
	output := wide.String()
	if !strings.Contains(output, "NODE-READY") || !strings.Contains(output, "API-READY") {
		t.Fatalf("status does not distinguish node and API readiness:\n%s", output)
	}
	first := strings.Index(output, "apc-ha-1")
	second := strings.Index(output, "apc-ha-2")
	third := strings.Index(output, "apc-ha-3")
	if first == -1 || !(first < second && second < third) {
		t.Fatalf("member rows are not deterministic:\n%s", output)
	}

	var document bytes.Buffer
	if err := printHAStatus(&document, state, "json"); err != nil {
		t.Fatalf("print JSON status: %v", err)
	}
	if strings.Contains(strings.ToLower(document.String()), "token") || strings.Contains(strings.ToLower(document.String()), "secret") {
		t.Fatalf("status exposed secret material: %s", document.String())
	}
}

func TestClusterHADeleteRequiresConfirmationBeforeManagerCall(t *testing.T) {
	fake := &fakeHAManager{}
	previous := newHAManager
	newHAManager = func() haManager { return fake }
	t.Cleanup(func() { newHAManager = previous })

	command := (&options{out: &bytes.Buffer{}, errOut: &bytes.Buffer{}}).clusterHACommand()
	command.SetArgs([]string{"delete", "ha-lab"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("delete error = %v", err)
	}
	if fake.deleteCalls != 0 {
		t.Fatalf("DeleteHA called %d times before confirmation", fake.deleteCalls)
	}
}

func TestClusterHADeleteForwardsKeepDataAfterConfirmation(t *testing.T) {
	fake := &fakeHAManager{}
	previous := newHAManager
	newHAManager = func() haManager { return fake }
	t.Cleanup(func() { newHAManager = previous })

	var output bytes.Buffer
	command := (&options{out: &output, errOut: &bytes.Buffer{}}).clusterHACommand()
	command.SetArgs([]string{"delete", "ha-lab", "--yes", "--keep-data"})
	if err := command.Execute(); err != nil {
		t.Fatalf("confirmed delete: %v", err)
	}
	if fake.deleteCalls != 1 || fake.deletedName != "ha-lab" || !fake.keepData {
		t.Fatalf("delete call = count %d, name %q, keepData %t", fake.deleteCalls, fake.deletedName, fake.keepData)
	}
	if !strings.Contains(output.String(), "data retained") {
		t.Fatalf("unexpected output: %s", output.String())
	}
}
