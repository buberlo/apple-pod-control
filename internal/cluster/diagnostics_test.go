package cluster

import (
	"context"
	"errors"
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
