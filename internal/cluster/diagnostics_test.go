package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDiagnoseSingleNodePassesAndCleansUp(t *testing.T) {
	inspect := `[{"configuration":{"labels":{"apc.dev/managed":"true","apc.dev/cluster":"home","apc.dev/role":"server","apc.dev/api-port":"16443"},"publishedPorts":[{"containerPort":16443,"hostAddress":"127.0.0.1","hostPort":16443,"proto":"tcp"}]},"status":{"state":"running","networks":[{"ipv4Address":"192.168.64.9/24"}]}}]`
	nodes := `{"items":[{"metadata":{"name":"server"},"spec":{"unschedulable":false},"status":{"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"},"conditions":[{"type":"Ready","status":"True"}],"addresses":[{"type":"InternalIP","address":"192.168.64.9"},{"type":"ExternalIP","address":"192.0.2.10"}]}}]}`
	responses := []runnerResponse{
		{stdout: []byte(inspect)},
		{stdout: []byte(nodes)},
		{stdout: []byte(nodes)},
		{stdout: []byte("pod created")},
		{stdout: []byte("condition met")},
		{stdout: []byte("10.42.0.2")},
		{},
		{},
		{},
		{stdout: []byte("service exposed")},
		{},
		{stdout: []byte("namespace deleted")},
	}
	runner := &scriptedRunner{responses: responses}
	manager := NewManager("container")
	manager.runner = runner
	manager.dialTCP = func(_ context.Context, address string) error {
		if address != "127.0.0.1:16443" {
			t.Fatalf("API address = %q", address)
		}
		return nil
	}

	report, err := manager.Diagnose(context.Background(), "home", DiagnoseOptions{Timeout: time.Minute, ProbeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if report.FailureCount() != 0 || report.WarningCount() != 1 {
		t.Fatalf("unexpected report: %#v", report.Results)
	}
	if len(runner.calls) != len(responses) {
		t.Fatalf("calls = %d, want %d", len(runner.calls), len(responses))
	}
	last := strings.Join(runner.calls[len(runner.calls)-1], " ")
	if !strings.Contains(last, "delete -n default") || !strings.Contains(last, "pod/apc-doctor-") || !strings.Contains(last, "service/apc-doctor-") {
		t.Fatalf("cleanup call = %q", last)
	}
	for _, call := range runner.calls {
		if len(call) >= 3 && call[0] == "exec" && call[2] == "kubectl" && call[1] != ContainerName("home") {
			t.Fatalf("legacy diagnostic dispatched through %q: %#v", call[1], call)
		}
	}
}

func TestDiagnoseHAReportsMembersQuorumAndUsesReadyMember(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	dispatchedThroughMemberTwo := false
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) == 5 && arguments[0] == "exec" && arguments[2] == "/bin/sh" && arguments[3] == "-c" && arguments[4] == haEtcdLocalProbeScript:
			member := memberForHAContainer(t, config, arguments[1])
			return fakeHAEtcdProbeOutput(member.ID), nil, nil
		case len(arguments) == 8 && arguments[0] == "exec" && arguments[2] == "kubectl" && arguments[3] == "get" && arguments[4] == "node":
			member := memberForHAContainer(t, config, arguments[1])
			return diagnosticHANode(member, member.ID != 1), nil, nil
		case len(arguments) == 7 && arguments[0] == "exec" && arguments[1] == HAContainerName(config.Name, 2) && arguments[2] == "kubectl" && arguments[3] == "get" && arguments[4] == "nodes":
			dispatchedThroughMemberTwo = true
			return []byte(`{"items":[]}`), nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, member HAMember) bool {
		return member.ID != 1
	}

	report, err := manager.Diagnose(context.Background(), config.Name, DiagnoseOptions{Timeout: time.Minute, ProbeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !dispatchedThroughMemberTwo {
		t.Fatal("Kubernetes diagnostics did not fail over to HA member 2")
	}
	assertDiagnosticStatus(t, report, "ha-runtime/"+config.Members[0].NodeName, DiagnosticPass)
	assertDiagnosticStatus(t, report, "host-api/"+config.Members[0].NodeName, DiagnosticFail)
	assertDiagnosticStatus(t, report, "ha-node/"+config.Members[0].NodeName, DiagnosticFail)
	quorum := assertDiagnosticStatus(t, report, "etcd-quorum", DiagnosticPass)
	if !strings.Contains(quorum.Detail, "2/3") || !strings.Contains(quorum.Detail, "requires 2") {
		t.Fatalf("quorum detail = %q", quorum.Detail)
	}
	assertDiagnosticStatus(t, report, "etcd-topology", DiagnosticPass)
	for _, member := range config.Members[1:] {
		assertDiagnosticStatus(t, report, "ha-runtime/"+member.NodeName, DiagnosticPass)
		assertDiagnosticStatus(t, report, "host-api/"+member.NodeName, DiagnosticPass)
		assertDiagnosticStatus(t, report, "ha-node/"+member.NodeName, DiagnosticPass)
	}
}

func TestDiagnoseHAStopsBeforeWorkloadProbesWithoutQuorum(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			if member.ID == 3 {
				return nil, []byte("container not found"), errors.New("exit 1")
			}
			state := "running"
			if member.ID == 2 {
				state = "stopped"
			}
			return marshalHAInspect(t, configuredHAContainer(config, member, state)), nil, nil
		case len(arguments) == 8 && arguments[0] == "exec" && arguments[2] == "kubectl" && arguments[3] == "get" && arguments[4] == "node":
			member := memberForHAContainer(t, config, arguments[1])
			return diagnosticHANode(member, true), nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(_ context.Context, _ HAConfig, member HAMember) bool {
		return member.ID == 1
	}

	report, err := manager.Diagnose(context.Background(), config.Name, DiagnoseOptions{Timeout: time.Minute, ProbeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	quorum := assertDiagnosticStatus(t, report, "etcd-quorum", DiagnosticFail)
	if !strings.Contains(quorum.Detail, "1/3") || !strings.Contains(quorum.Detail, "requires 2") {
		t.Fatalf("quorum detail = %q", quorum.Detail)
	}
	missing := assertDiagnosticStatus(t, report, "ha-runtime/"+config.Members[2].NodeName, DiagnosticFail)
	if !strings.Contains(missing.Detail, "missing") {
		t.Fatalf("missing member detail = %q", missing.Detail)
	}
	for _, call := range runner.calls {
		if len(call) >= 5 && call[0] == "exec" && call[2] == "kubectl" && call[3] == "get" && call[4] == "nodes" {
			t.Fatalf("workload diagnostics ran without quorum: %#v", call)
		}
	}
}

func TestDiagnoseHAStopsBeforeWorkloadsOnDivergentEtcdTopology(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		switch {
		case len(arguments) == 2 && arguments[0] == "inspect":
			member := memberForHAContainer(t, config, arguments[1])
			return marshalHAInspect(t, configuredHAContainer(config, member, "running")), nil, nil
		case len(arguments) == 8 && arguments[0] == "exec" && arguments[2] == "kubectl" && arguments[3] == "get" && arguments[4] == "node":
			member := memberForHAContainer(t, config, arguments[1])
			return diagnosticHANode(member, true), nil, nil
		case len(arguments) == 5 && arguments[0] == "exec" && arguments[2] == "/bin/sh" && arguments[4] == haEtcdLocalProbeScript:
			member := memberForHAContainer(t, config, arguments[1])
			mutate := (func(string) string)(nil)
			if member.ID == 2 {
				mutate = func(value string) string {
					return strings.Replace(value, "etcd_server_is_learner 0", "etcd_server_is_learner 1", 1)
				}
			}
			return fakeHAEtcdProbeOutputWith(member.ID, mutate), nil, nil
		default:
			t.Fatalf("unexpected command: %#v", arguments)
			return nil, nil, errors.New("unexpected command")
		}
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.probeHAAPI = func(context.Context, HAConfig, HAMember) bool { return true }

	report, err := manager.Diagnose(context.Background(), config.Name, DiagnoseOptions{Timeout: time.Minute, ProbeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	assertDiagnosticStatus(t, report, "etcd-topology", DiagnosticFail)
	for _, call := range runner.calls {
		if len(call) >= 5 && call[0] == "exec" && call[2] == "kubectl" && call[3] == "get" && call[4] == "nodes" {
			t.Fatalf("workload diagnostics ran with divergent etcd: %#v", call)
		}
	}
}

func TestDiagnoseCleansUpAmbiguousPodCreate(t *testing.T) {
	inspect := `[{"configuration":{"labels":{"apc.dev/managed":"true","apc.dev/cluster":"home","apc.dev/role":"server","apc.dev/api-port":"16443"},"publishedPorts":[{"containerPort":16443,"hostAddress":"127.0.0.1","hostPort":16443,"proto":"tcp"}]},"status":{"state":"running","networks":[{"ipv4Address":"192.168.64.9/24"}]}}]`
	nodes := `{"items":[{"metadata":{"name":"server"},"spec":{"unschedulable":false},"status":{"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"},"conditions":[{"type":"Ready","status":"True"}],"addresses":[{"type":"InternalIP","address":"192.168.64.9"}]}}]}`
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: []byte(inspect)}, {stdout: []byte(nodes)}, {stdout: []byte(nodes)},
		{stderr: []byte("request timed out"), err: context.DeadlineExceeded},
		{stdout: []byte("pod absent or deleted")},
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.dialTCP = func(context.Context, string) error { return nil }
	if _, err := manager.Diagnose(context.Background(), "home", DiagnoseOptions{Timeout: time.Minute, ProbeTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	last := strings.Join(runner.calls[len(runner.calls)-1], " ")
	if !strings.Contains(last, "delete -n default") || !strings.Contains(last, "pod/apc-doctor-") {
		t.Fatalf("ambiguous create cleanup = %q", last)
	}
}

func TestDiagnoseCleansUpAmbiguousServiceExpose(t *testing.T) {
	inspect := `[{"configuration":{"labels":{"apc.dev/managed":"true","apc.dev/cluster":"home","apc.dev/role":"server","apc.dev/api-port":"16443"},"publishedPorts":[{"containerPort":16443,"hostAddress":"127.0.0.1","hostPort":16443,"proto":"tcp"}]},"status":{"state":"running","networks":[{"ipv4Address":"192.168.64.9/24"}]}}]`
	nodes := `{"items":[{"metadata":{"name":"server"},"spec":{"unschedulable":false},"status":{"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"},"conditions":[{"type":"Ready","status":"True"}],"addresses":[{"type":"InternalIP","address":"192.168.64.9"}]}}]}`
	runner := &scriptedRunner{responses: []runnerResponse{
		{stdout: []byte(inspect)}, {stdout: []byte(nodes)}, {stdout: []byte(nodes)},
		{stdout: []byte("pod created")}, {stdout: []byte("condition met")}, {stdout: []byte("10.42.0.2")},
		{}, {},
		{stderr: []byte("request timed out"), err: context.DeadlineExceeded},
		{stdout: []byte("resources absent or deleted")},
	}}
	manager := NewManager("container")
	manager.runner = runner
	manager.dialTCP = func(context.Context, string) error { return nil }
	if _, err := manager.Diagnose(context.Background(), "home", DiagnoseOptions{Timeout: time.Minute, ProbeTimeout: time.Second, SkipEgress: true}); err != nil {
		t.Fatal(err)
	}
	last := strings.Join(runner.calls[len(runner.calls)-1], " ")
	if !strings.Contains(last, "pod/apc-doctor-") || !strings.Contains(last, "service/apc-doctor-") {
		t.Fatalf("ambiguous expose cleanup = %q", last)
	}
}

func TestDiagnosticReportTextAndCounts(t *testing.T) {
	report := DiagnosticReport{Results: []DiagnosticResult{
		{Name: "ok", Status: DiagnosticPass, Detail: "ready"},
		{Name: "warning", Status: DiagnosticWarn, Detail: "single node"},
		{Name: "failure", Status: DiagnosticFail, Detail: "timeout", Remediation: "check routing"},
	}}
	if report.PassedCount() != 1 || report.WarningCount() != 1 || report.FailureCount() != 1 {
		t.Fatalf("unexpected counts: %#v", report)
	}
	var output strings.Builder
	if err := report.WriteText(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "1 passed, 1 warnings, 1 failed") || !strings.Contains(output.String(), "check routing") {
		t.Fatalf("unexpected output: %s", output.String())
	}
}

func TestConciseErrorIsSingleLineAndBounded(t *testing.T) {
	value := conciseError(errors.New("first line\n" + strings.Repeat("long detail ", 40)))
	if strings.Contains(value, "\n") || len(value) > 280 {
		t.Fatalf("value is not concise: %q", value)
	}
}

func diagnosticHANode(member HAMember, ready bool) []byte {
	status := "False"
	if ready {
		status = "True"
	}
	return []byte(fmt.Sprintf(`{"metadata":{"name":%q},"status":{"conditions":[{"type":"Ready","status":%q}],"nodeInfo":{"kubeletVersion":"v1.36.2+k3s1"}}}`, member.NodeName, status))
}

func assertDiagnosticStatus(t *testing.T, report DiagnosticReport, name string, status DiagnosticStatus) DiagnosticResult {
	t.Helper()
	for _, result := range report.Results {
		if result.Name == name {
			if result.Status != status {
				t.Fatalf("diagnostic %q status = %s, want %s: %#v", name, result.Status, status, result)
			}
			return result
		}
	}
	t.Fatalf("diagnostic %q missing from %#v", name, report.Results)
	return DiagnosticResult{}
}
