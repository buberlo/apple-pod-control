package doctor

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func healthyEnvironment() environment {
	return environment{
		goos:   "darwin",
		goarch: "arm64",
		numCPU: 8,
		lookPath: func(name string) (string, error) {
			return "/usr/local/bin/" + name, nil
		},
		run: func(_ context.Context, _ string, arguments ...string) (string, error) {
			joined := strings.Join(arguments, " ")
			switch joined {
			case "-n get default":
				return "route to: default\ninterface: en0", nil
			case "--version":
				return "container CLI version 1.0.0 (build: release)", nil
			case "system status":
				return "status running", nil
			case "run --help":
				return "--publish --cap-add --mount --arch", nil
			case "machine create --help":
				return "--cpus --memory", nil
			default:
				return "", errors.New("unexpected command")
			}
		},
		memoryBytes: func(context.Context) (int64, error) { return 8 << 30, nil },
		listenTCP:   func(string) (io.Closer, error) { return nopCloser{}, nil },
		listenUDP:   func(string) (io.Closer, error) { return nopCloser{}, nil },
		lookupHost:  func(context.Context, string) ([]string, error) { return []string{"192.0.2.2"}, nil },
		dialTCP:     func(context.Context, string) error { return nil },
	}
}

func TestDefaultRouteDiagnostic(t *testing.T) {
	tests := []struct {
		name            string
		output          string
		err             error
		wantStatus      Status
		wantDetail      string
		forbiddenDetail string
	}{
		{
			name:       "physical interface",
			output:     "   route to: default\ninterface: en7\n   gateway: 192.0.2.1",
			wantStatus: Pass,
			wantDetail: "non-tunnel interface en7",
		},
		{
			name:       "tunnel interface",
			output:     "route to: default\n  interface: utun12\nflags: <UP,DONE,STATIC>",
			wantStatus: Warn,
			wantDetail: "tunnel interface utun12; VM/pod egress may be affected",
		},
		{
			name:            "lookup unavailable",
			err:             errors.New("route failed via gateway 192.0.2.1"),
			wantStatus:      Warn,
			wantDetail:      "default route lookup unavailable",
			forbiddenDetail: "192.0.2.1",
		},
		{
			name:            "malformed output",
			output:          "route to: default\ninterface: en0 injected\ngateway: 192.0.2.1",
			wantStatus:      Warn,
			wantDetail:      "default route interface could not be determined",
			forbiddenDetail: "192.0.2.1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := healthyEnvironment()
			baseRun := env.run
			env.run = func(ctx context.Context, binary string, arguments ...string) (string, error) {
				if binary == "/sbin/route" && strings.Join(arguments, " ") == "-n get default" {
					return test.output, test.err
				}
				return baseRun(ctx, binary, arguments...)
			}

			report := run(context.Background(), env, Options{Role: "server", Timeout: time.Second})
			result, ok := findResult(report, "host-default-route")
			if !ok {
				t.Fatalf("host-default-route result missing: %#v", report.Results)
			}
			if result.Status != test.wantStatus {
				t.Fatalf("status = %s, want %s: %#v", result.Status, test.wantStatus, result)
			}
			if !strings.Contains(result.Detail, test.wantDetail) {
				t.Fatalf("detail = %q, want substring %q", result.Detail, test.wantDetail)
			}
			if test.forbiddenDetail != "" && strings.Contains(result.Detail+result.Remediation, test.forbiddenDetail) {
				t.Fatalf("diagnostic leaked sensitive route output: %#v", result)
			}
		})
	}
}

func TestParseDefaultRouteInterfaceRejectsConflictingInterfaces(t *testing.T) {
	if interfaceName, ok := parseDefaultRouteInterface("interface: en0\ninterface: utun4"); ok {
		t.Fatalf("parse succeeded with conflicting interface %q", interfaceName)
	}
}

func findResult(report Report, name string) (Result, bool) {
	for _, result := range report.Results {
		if result.Name == name {
			return result, true
		}
	}
	return Result{}, false
}

func TestHealthyServerReportOnlyWarnsAboutMachinePortPublishing(t *testing.T) {
	report := run(context.Background(), healthyEnvironment(), Options{Role: "server", Timeout: time.Second})
	if report.FailureCount() != 0 {
		t.Fatalf("unexpected failures: %#v", report.Results)
	}
	if report.WarningCount() != 1 {
		t.Fatalf("warnings = %d, want 1: %#v", report.WarningCount(), report.Results)
	}
}

func TestUnsupportedPlatformAndBusyPortFail(t *testing.T) {
	env := healthyEnvironment()
	env.goarch = "amd64"
	env.listenTCP = func(string) (io.Closer, error) { return nil, errors.New("address already in use") }
	report := run(context.Background(), env, Options{Role: "server"})
	if report.FailureCount() != 2 {
		t.Fatalf("failures = %d, want 2: %#v", report.FailureCount(), report.Results)
	}
}

func TestBusyPortOwnedByAPCK3sNodePasses(t *testing.T) {
	env := healthyEnvironment()
	env.listenUDP = func(string) (io.Closer, error) { return nil, errors.New("address already in use") }
	baseRun := env.run
	env.run = func(ctx context.Context, binary string, arguments ...string) (string, error) {
		if strings.Join(arguments, " ") == "list --format json" {
			return `[{"id":"apc-k3s-home-server","configuration":{"labels":{"apc.dev/managed":"true","apc.dev/role":"server"},"publishedPorts":[{"hostPort":8472,"proto":"udp"}]}}]`, nil
		}
		return baseRun(ctx, binary, arguments...)
	}

	report := run(context.Background(), env, Options{Role: "server"})
	if report.FailureCount() != 0 {
		t.Fatalf("unexpected failures: %#v", report.Results)
	}
	found := false
	for _, result := range report.Results {
		if result.Name == "flannel-vxlan-port" && result.Status == Pass && strings.Contains(result.Detail, "apc-k3s-home-server") {
			found = true
		}
	}
	if !found {
		t.Fatalf("managed port owner not reported: %#v", report.Results)
	}
}

func TestMissingContainerStopsRuntimeChecks(t *testing.T) {
	env := healthyEnvironment()
	env.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	report := run(context.Background(), env, Options{Role: "agent"})
	if report.FailureCount() != 1 {
		t.Fatalf("failures = %d, want 1: %#v", report.FailureCount(), report.Results)
	}
	if report.Results[len(report.Results)-1].Name != "container-cli" {
		t.Fatalf("last check = %q", report.Results[len(report.Results)-1].Name)
	}
}

func TestReportTextIncludesSummary(t *testing.T) {
	report := Report{Role: "server", Results: []Result{{Name: "one", Status: Pass, Detail: "ok"}}}
	var output strings.Builder
	if err := report.WriteText(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "1 passed, 0 warnings, 0 failed") {
		t.Fatalf("unexpected output: %s", output.String())
	}
}
