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
