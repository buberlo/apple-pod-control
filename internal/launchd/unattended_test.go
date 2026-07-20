package launchd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type testOwnership struct {
	uid int
	gid int
}

type unattendedFixture struct {
	manager    *Manager
	runner     *systemRunner
	config     Config
	username   string
	home       string
	executable string
	daemonDir  string
	owners     map[string]testOwnership
}

type systemRunner struct {
	mu                   sync.Mutex
	loaded               bool
	userLoaded           bool
	guiLoaded            bool
	state                string
	pid                  int
	lastExit             string
	arguments            []string
	bootoutDelayPrints   int
	pendingBootoutPrints int
	calls                [][]string
}

func (r *systemRunner) Run(_ context.Context, binary string, arguments ...string) ([]byte, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	call := append([]string{binary}, arguments...)
	r.calls = append(r.calls, call)
	if binary != unattendedLaunchctlPath || len(arguments) == 0 {
		return nil, []byte("unsupported command"), errors.New("command failed")
	}
	switch arguments[0] {
	case "print":
		loaded := r.loaded
		finishDelayedBootout := false
		if len(arguments) == 2 && strings.HasPrefix(arguments[1], "system/") && loaded && r.pendingBootoutPrints > 0 {
			r.pendingBootoutPrints--
			finishDelayedBootout = r.pendingBootoutPrints == 0
		}
		if len(arguments) == 2 && strings.HasPrefix(arguments[1], "user/") {
			loaded = r.userLoaded
		}
		if len(arguments) == 2 && strings.HasPrefix(arguments[1], "gui/") {
			loaded = r.guiLoaded
		}
		if !loaded {
			return nil, []byte("Could not find service"), errors.New("not found")
		}
		output := renderLaunchctlPrint(arguments[1], r)
		if finishDelayedBootout {
			r.loaded = false
		}
		return output, nil, nil
	case "bootstrap":
		if len(arguments) != 3 || arguments[1] != "system" || r.loaded {
			return nil, []byte("invalid bootstrap"), errors.New("bootstrap failed")
		}
		r.loaded = true
		return nil, nil, nil
	case "enable", "kickstart":
		if !r.loaded {
			return nil, []byte("service is not loaded"), errors.New("not loaded")
		}
		return nil, nil, nil
	case "bootout":
		if !r.loaded {
			return nil, []byte("Could not find service"), errors.New("not found")
		}
		if r.bootoutDelayPrints > 0 {
			r.pendingBootoutPrints = r.bootoutDelayPrints
		} else {
			r.loaded = false
		}
		return nil, nil, nil
	default:
		return nil, []byte("unsupported launchctl operation"), errors.New("command failed")
	}
}

func renderLaunchctlPrint(serviceTarget string, runner *systemRunner) []byte {
	state := runner.state
	if state == "" {
		state = "running"
	}
	pid := runner.pid
	if pid == 0 {
		pid = 4242
	}
	lastExit := runner.lastExit
	if lastExit == "" {
		lastExit = "(never exited)"
	}
	var output strings.Builder
	fmt.Fprintf(&output, "%s = {\n", serviceTarget)
	fmt.Fprintln(&output, "\tactive count = 1")
	fmt.Fprintf(&output, "\tstate = %s\n", state)
	fmt.Fprintln(&output, "\tprogram = /bin/launchctl")
	fmt.Fprintln(&output, "\targuments = {")
	for _, argument := range runner.arguments {
		fmt.Fprintf(&output, "\t\t%s\n", argument)
	}
	fmt.Fprintln(&output, "\t}")
	fmt.Fprintf(&output, "\tpid = %d\n", pid)
	fmt.Fprintf(&output, "\tlast exit code = %s\n", lastExit)
	fmt.Fprintln(&output, "}")
	return []byte(output.String())
}

func (r *systemRunner) snapshotCalls() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([][]string, len(r.calls))
	for index := range r.calls {
		result[index] = append([]string(nil), r.calls[index]...)
	}
	return result
}

func TestUnattendedInstallStatusAndConfirmedUninstall(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}

	path, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username)
	if err != nil {
		t.Fatalf("install unattended: %v", err)
	}
	if path != plan.plistPath || filepath.Dir(path) != fixture.daemonDir {
		t.Fatalf("plist path = %q, want %q", path, plan.plistPath)
	}
	assertExactUnattendedPlist(t, fixture, plan)
	assertSystemDomainCalls(t, fixture.runner.snapshotCalls(), plan)
	if _, err := os.Lstat(plan.logDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root install touched target-user log directory: %v", err)
	}

	status, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username)
	if err != nil || !bytes.Contains(status, []byte("state = running")) {
		t.Fatalf("status = %q, %v", status, err)
	}
	fixture.manager.euid = 501
	if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("read-only status unexpectedly required root: %v", err)
	}
	fixture.manager.euid = 0

	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("idempotent install: %v", err)
	}
	after, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || countLaunchctlOperation(fixture.runner.snapshotCalls(), "bootstrap") != 1 {
		t.Fatal("idempotent install replaced the plist or bootstrapped twice")
	}
	if _, err := os.Lstat(plan.logDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("idempotent root install touched target-user log directory: %v", err)
	}

	if err := fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, false); err == nil {
		t.Fatal("unconfirmed unattended uninstall was accepted")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("unconfirmed uninstall changed plist: %v", err)
	}
	if err := fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, true); err != nil {
		t.Fatalf("confirmed unattended uninstall: %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("plist remains after uninstall: %v", err)
	}
	if _, err := os.Lstat(plan.logDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root uninstall touched target-user logs: %v", err)
	}
	if err := fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, true); err != nil {
		t.Fatalf("idempotent absent uninstall: %v", err)
	}
}

func TestUnattendedPlistRunsOnlyAsResolvedNonRootTarget(t *testing.T) {
	fixture := newUnattendedFixture(t)
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}
	document := string(plan.plist)
	wantArguments := []string{
		"/bin/launchctl", "asuser", "501",
		"/usr/bin/sudo", "-n", "-H", "-u", "worker", "--",
		"/usr/bin/env", "-i",
		"HOME=" + fixture.home,
		"PATH=/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin",
		fixture.executable,
		"system", "supervise", "--role", "server", "--cluster", "home", "--interval", "20s",
		"--log-file", plan.logPath,
	}
	if got := unattendedProgramArguments(plan.config, plan.target, plan.logPath); !reflect.DeepEqual(got, wantArguments) {
		t.Fatalf("unattended ProgramArguments = %#v, want %#v", got, wantArguments)
	}
	var expectedProgramArguments strings.Builder
	expectedProgramArguments.WriteString("  <key>ProgramArguments</key>\n  <array>\n")
	for _, argument := range wantArguments {
		expectedProgramArguments.WriteString("    <string>" + escape(argument) + "</string>\n")
	}
	expectedProgramArguments.WriteString("  </array>\n")
	if !strings.Contains(document, expectedProgramArguments.String()) {
		t.Fatalf("unattended plist does not render the exact protected argument array:\n%s", document)
	}
	required := []string{
		"<key>HOME</key>\n    <string>" + fixture.home + "</string>",
		"<key>SessionCreate</key>\n  <true/>",
		"<key>ProcessType</key>\n  <string>Background</string>",
		"<key>RunAtLoad</key>\n  <true/>",
		"<key>ThrottleInterval</key>\n  <integer>30</integer>",
		"<key>Umask</key>\n  <integer>63</integer>",
		"<string>system</string>", "<string>supervise</string>", "<string>server</string>",
		plan.logPath,
	}
	for _, value := range required {
		if !strings.Contains(document, value) {
			t.Fatalf("unattended plist missing %q:\n%s", value, document)
		}
	}
	for _, forbiddenKey := range []string{"<key>UserName</key>", "<key>GroupName</key>"} {
		if strings.Contains(document, forbiddenKey) {
			t.Fatalf("root LaunchDaemon must not use %s because that leaves it in the wrong bootstrap namespace", forbiddenKey)
		}
	}
	if plan.logDir != filepath.Join(fixture.home, "Library", "Logs", "APC") || !strings.HasPrefix(plan.logPath, fixture.home+string(filepath.Separator)) {
		t.Fatalf("non-root bounded log path is not inside the exact target home: %q", plan.logPath)
	}
	for _, standardPath := range []string{"StandardOutPath", "StandardErrorPath"} {
		expected := "<key>" + standardPath + "</key>\n  <string>/dev/null</string>"
		if !strings.Contains(document, expected) {
			t.Fatalf("root LaunchDaemon %s is not /dev/null:\n%s", standardPath, document)
		}
	}
	if wantArguments[0] == fixture.executable {
		t.Fatal("unattended LaunchDaemon would execute APC directly as root")
	}
	apcArguments := 0
	for _, argument := range wantArguments {
		if argument == fixture.executable {
			apcArguments++
		}
		if argument == "/bin/sh" || argument == "/bin/bash" || argument == "sh" || argument == "bash" || argument == "-c" {
			t.Fatalf("unattended privilege drop unexpectedly uses a shell: %#v", wantArguments)
		}
	}
	if apcArguments != 1 || !filepath.IsAbs(wantArguments[0]) || !filepath.IsAbs(wantArguments[3]) || !filepath.IsAbs(wantArguments[9]) || !filepath.IsAbs(wantArguments[13]) {
		t.Fatalf("unattended wrapper/APC executables are not exact absolute paths: %#v", wantArguments)
	}
}

func TestUnattendedNonRootStatusDoesNotOpenTargetLog(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manager.euid = 501
	status, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username)
	if err != nil || !bytes.Contains(status, []byte("state = running")) {
		t.Fatalf("non-root metadata-only status = %q, %v", status, err)
	}
	if _, err := os.Lstat(plan.logDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("status opened or created the target-user log path: %v", err)
	}
}

func TestUnattendedMutationsRequireRoot(t *testing.T) {
	fixture := newUnattendedFixture(t)
	fixture.manager.euid = 501
	if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
		t.Fatal("non-root unattended install was accepted")
	}
	if err := fixture.manager.UninstallUnattended(context.Background(), fixture.config, fixture.username, true); err == nil {
		t.Fatal("non-root unattended uninstall was accepted")
	}
	if len(fixture.runner.snapshotCalls()) != 0 {
		t.Fatal("non-root mutation invoked launchctl")
	}
}

func TestUnattendedRejectsRootTargetAndUnsafePaths(t *testing.T) {
	t.Run("root target", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		fixture.manager.lookupAccount = func(string) (accountRecord, error) {
			return accountRecord{username: "root", uid: 0, gid: 0, home: fixture.home}, nil
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, "root"); err == nil {
			t.Fatal("root target was accepted")
		}
	})

	t.Run("root group", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		fixture.manager.lookupAccount = func(string) (accountRecord, error) {
			return accountRecord{username: fixture.username, uid: 501, gid: 0, home: fixture.home}, nil
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("root primary group was accepted")
		}
	})

	t.Run("symlink home", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		realHome := filepath.Join(filepath.Dir(fixture.home), "real-home")
		if err := os.Mkdir(realHome, 0o755); err != nil {
			t.Fatal(err)
		}
		fixture.owners[realHome] = testOwnership{uid: 501, gid: 20}
		if err := os.Remove(fixture.home); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realHome, fixture.home); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("symbolic-link home was accepted")
		}
	})

	t.Run("symlink executable", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		realExecutable := fixture.executable + "-real"
		if err := os.WriteFile(realExecutable, []byte("binary"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(fixture.executable); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realExecutable, fixture.executable); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("symbolic-link executable was accepted")
		}
	})
}

func TestUnattendedRefusesUnownedMismatchedOrUnsafePlist(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *unattendedFixture, unattendedPlan)
	}{
		{name: "wrong mode", mutate: func(t *testing.T, _ *unattendedFixture, plan unattendedPlan) {
			if err := os.Chmod(plan.plistPath, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong ownership", mutate: func(_ *testing.T, fixture *unattendedFixture, plan unattendedPlan) {
			fixture.owners[plan.plistPath] = testOwnership{uid: 501, gid: 20}
		}},
		{name: "mismatched contents", mutate: func(t *testing.T, _ *unattendedFixture, plan unattendedPlan) {
			if err := os.WriteFile(plan.plistPath, []byte("foreign plist"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symbolic link", mutate: func(t *testing.T, fixture *unattendedFixture, plan unattendedPlan) {
			if err := os.Remove(plan.plistPath); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "foreign.plist")
			if err := os.WriteFile(target, plan.plist, 0o644); err != nil {
				t.Fatal(err)
			}
			fixture.owners[target] = testOwnership{uid: 0, gid: 0}
			if err := os.Symlink(target, plan.plistPath); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newUnattendedFixture(t)
			plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err != nil {
				t.Fatal(err)
			}
			mutationsBefore := launchctlMutationCounts(fixture.runner.snapshotCalls())
			test.mutate(t, fixture, plan)
			if _, err := fixture.manager.StatusUnattended(context.Background(), fixture.config, fixture.username); err == nil {
				t.Fatal("status accepted unsafe plist")
			}
			if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
				t.Fatal("install accepted unsafe existing plist")
			}
			if err := fixture.manager.UninstallUnattended(context.Background(), fixture.config, fixture.username, true); err == nil {
				t.Fatal("uninstall accepted unsafe existing plist")
			}
			if launchctlMutationCounts(fixture.runner.snapshotCalls()) != mutationsBefore {
				t.Fatal("unsafe plist caused a launchctl mutation")
			}
		})
	}
}

func TestUnattendedRefusesMismatchedConfig(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	changed := fixture.config
	changed.Interval = 45 * time.Second
	if _, err := fixture.manager.InstallUnattended(ctx, changed, fixture.username); err == nil {
		t.Fatal("install accepted a mismatched existing plist")
	}
	if err := fixture.manager.UninstallUnattended(ctx, changed, fixture.username, true); err == nil {
		t.Fatal("uninstall accepted a mismatched existing plist")
	}
}

func TestUnattendedRootLifecycleNeverOpensTargetUserLogPath(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(plan.logDir), 0o700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign-logs")
	if err := os.Mkdir(foreign, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, plan.logDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	before, err := os.ReadDir(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("root install inspected target-user log symlink: %v", err)
	}
	if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("status inspected target-user log symlink: %v", err)
	}
	after, err := os.ReadDir(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("root lifecycle mutated target-controlled replacement: before=%d after=%d", len(before), len(after))
	}
}

func TestUnattendedValidatesLaunchDaemonsDirectory(t *testing.T) {
	t.Run("mode", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		if err := os.Chmod(fixture.daemonDir, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("writable LaunchDaemons directory was accepted")
		}
	})
	t.Run("ownership", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		fixture.owners[fixture.daemonDir] = testOwnership{uid: 501, gid: 20}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("unowned LaunchDaemons directory was accepted")
		}
	})
	t.Run("parent mode", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		library := filepath.Dir(fixture.daemonDir)
		if err := os.Chmod(library, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("writable LaunchDaemons parent was accepted")
		}
	})
	t.Run("parent ownership", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		library := filepath.Dir(fixture.daemonDir)
		fixture.owners[library] = testOwnership{uid: 501, gid: 20}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("unowned LaunchDaemons parent was accepted")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		realDirectory := fixture.daemonDir + "-real"
		if err := os.Mkdir(realDirectory, 0o755); err != nil {
			t.Fatal(err)
		}
		fixture.owners[realDirectory] = testOwnership{uid: 0, gid: 0}
		if err := os.Remove(fixture.daemonDir); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realDirectory, fixture.daemonDir); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil {
			t.Fatal("symbolic-link LaunchDaemons directory was accepted")
		}
	})
}

func TestUnattendedRejectsExtendedACLsAndPublishesACLFreePlist(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS extended ACL semantics")
	}
	t.Run("LaunchDaemons directory", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		addTestExtendedACL(t, fixture.daemonDir)
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "extended ACL") {
			t.Fatalf("install ACL error = %v", err)
		}
		if launchctlMutationCounts(fixture.runner.snapshotCalls()) != [4]int{} {
			t.Fatal("ACL-bearing LaunchDaemons directory reached launchctl")
		}
	})
	t.Run("LaunchDaemons parent allow grant", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		addTestExtendedACL(t, filepath.Dir(fixture.daemonDir))
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "ACL grant") {
			t.Fatalf("install ancestor ACL error = %v", err)
		}
		if launchctlMutationCounts(fixture.runner.snapshotCalls()) != [4]int{} {
			t.Fatal("ACL-bearing LaunchDaemons parent reached launchctl")
		}
	})
	t.Run("LaunchDaemons parent deny only", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		addTestACL(t, filepath.Dir(fixture.daemonDir), "everyone deny delete")
		if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err != nil {
			t.Fatalf("deny-only LaunchDaemons ancestor was rejected: %v", err)
		}
	})
	t.Run("existing plist", func(t *testing.T) {
		fixture := newUnattendedFixture(t)
		ctx := context.Background()
		if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
			t.Fatal(err)
		}
		plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateNoExtendedACL(plan.plistPath); err != nil {
			t.Fatalf("new plist retained an ACL: %v", err)
		}
		addTestExtendedACL(t, plan.plistPath)
		mutationsBefore := launchctlMutationCounts(fixture.runner.snapshotCalls())
		if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err == nil {
			t.Fatal("status accepted ACL-bearing plist")
		}
		if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err == nil {
			t.Fatal("install accepted ACL-bearing plist")
		}
		if err := fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, true); err == nil {
			t.Fatal("uninstall accepted ACL-bearing plist")
		}
		if launchctlMutationCounts(fixture.runner.snapshotCalls()) != mutationsBefore {
			t.Fatal("ACL-bearing plist caused launchctl mutation")
		}
	})
}

func addTestExtendedACL(t *testing.T, path string) {
	t.Helper()
	addTestACL(t, path, "everyone allow write,delete")
	if err := validateNoExtendedACL(path); err == nil {
		t.Fatal("test fixture did not acquire an extended ACL")
	}
}

func addTestACL(t *testing.T, path, entry string) {
	t.Helper()
	output, err := exec.Command(unattendedChmodPath, "+a", entry, path).CombinedOutput()
	if err != nil {
		t.Fatalf("add extended ACL: %v: %s", err, output)
	}
	t.Cleanup(func() {
		_, _ = exec.Command(unattendedChmodPath, "-N", path).CombinedOutput()
	})
}

func TestUnattendedStatusRequiresLoadedSystemService(t *testing.T) {
	fixture := newUnattendedFixture(t)
	if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	fixture.runner.mu.Lock()
	fixture.runner.loaded = false
	fixture.runner.mu.Unlock()
	if _, err := fixture.manager.StatusUnattended(context.Background(), fixture.config, fixture.username); err == nil {
		t.Fatal("status accepted an unloaded service")
	}
}

func TestUnattendedInstallAndStatusRejectUnhealthyLoadedService(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*systemRunner)
	}{
		{name: "waiting", mutate: func(runner *systemRunner) { runner.state = "waiting" }},
		{name: "non-positive pid", mutate: func(runner *systemRunner) { runner.pid = -1 }},
		{name: "wrong loaded wrapper", mutate: func(runner *systemRunner) {
			runner.arguments = append([]string(nil), runner.arguments...)
			runner.arguments[0] = "/tmp/foreign-launchctl"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newUnattendedFixture(t)
			ctx := context.Background()
			if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
				t.Fatal(err)
			}
			fixture.runner.mu.Lock()
			test.mutate(fixture.runner)
			fixture.runner.mu.Unlock()
			mutationsBefore := launchctlMutationCounts(fixture.runner.snapshotCalls())
			if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "loaded but unhealthy") {
				t.Fatalf("status unhealthy error = %v", err)
			}
			if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "loaded but unhealthy") {
				t.Fatalf("install unhealthy error = %v", err)
			}
			if launchctlMutationCounts(fixture.runner.snapshotCalls()) != mutationsBefore {
				t.Fatal("unhealthy visible label caused a launchctl mutation")
			}
		})
	}
}

func TestUnattendedRunningServiceAcceptsHistoricalNonzeroLastExit(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	fixture.runner.mu.Lock()
	fixture.runner.lastExit = "78"
	fixture.runner.mu.Unlock()

	if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("running service rejected because of historical last exit: %v", err)
	}
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("idempotent install rejected running service because of historical last exit: %v", err)
	}
}

func TestUnattendedInstallReplacesLoadedStaleDefinitionWhenPlistWasAbsent(t *testing.T) {
	fixture := newUnattendedFixture(t)
	fixture.runner.loaded = true
	fixture.runner.bootoutDelayPrints = 2
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(plan.plistPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture unexpectedly has a plist: %v", err)
	}

	if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err != nil {
		t.Fatalf("replace stale loaded definition: %v", err)
	}
	calls := fixture.runner.snapshotCalls()
	bootoutIndex, bootstrapIndex := -1, -1
	for index, call := range calls {
		if len(call) > 1 && call[1] == "bootout" {
			bootoutIndex = index
		}
		if len(call) > 1 && call[1] == "bootstrap" {
			bootstrapIndex = index
		}
	}
	if bootoutIndex < 0 || bootstrapIndex < 0 || bootoutIndex >= bootstrapIndex {
		t.Fatalf("stale service was not booted out before exact bootstrap: %#v", calls)
	}
	if countLaunchctlOperation(calls, "bootout") != 1 || countLaunchctlOperation(calls, "bootstrap") != 1 {
		t.Fatalf("unexpected stale replacement lifecycle: %#v", calls)
	}
	assertExactUnattendedPlist(t, fixture, plan)
}

func TestUnattendedUninstallWaitsForDelayedBootoutBeforeRemovingPlist(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runner.mu.Lock()
	fixture.runner.bootoutDelayPrints = 2
	fixture.runner.mu.Unlock()

	if err := fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, true); err != nil {
		t.Fatalf("uninstall with delayed bootout: %v", err)
	}
	if _, err := os.Lstat(plan.plistPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("plist remains after delayed uninstall: %v", err)
	}
	fixture.runner.mu.Lock()
	loaded := fixture.runner.loaded
	pending := fixture.runner.pendingBootoutPrints
	fixture.runner.mu.Unlock()
	if loaded || pending != 0 {
		t.Fatalf("uninstall returned before delayed bootout completed: loaded=%v pending=%d", loaded, pending)
	}
}

func TestUnattendedUninstallTimeoutKeepsManagedPlist(t *testing.T) {
	fixture := newUnattendedFixture(t)
	if _, err := fixture.manager.InstallUnattended(context.Background(), fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.manager.unattendedPlan(fixture.config, fixture.username)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runner.mu.Lock()
	fixture.runner.bootoutDelayPrints = 1_000_000
	fixture.runner.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err = fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, true)
	if err == nil || !strings.Contains(err.Error(), "wait for unattended LaunchDaemon to unload") {
		t.Fatalf("uninstall timeout error = %v", err)
	}
	if _, statErr := os.Lstat(plan.plistPath); statErr != nil {
		t.Fatalf("timed-out uninstall removed the managed plist: %v", statErr)
	}
}

func TestUnattendedLifecycleUsesOnlyAbsoluteLaunchctl(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.UninstallUnattended(ctx, fixture.config, fixture.username, true); err != nil {
		t.Fatal(err)
	}
	for _, call := range fixture.runner.snapshotCalls() {
		if len(call) == 0 || call[0] != unattendedLaunchctlPath {
			t.Fatalf("unattended lifecycle used non-absolute launchctl: %#v", call)
		}
	}
}

func TestUnattendedRefusesPerUserLaunchAgentPlist(t *testing.T) {
	for _, test := range []struct {
		name    string
		symlink bool
	}{
		{name: "regular mismatched plist"},
		{name: "symbolic-link plist", symlink: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newUnattendedFixture(t)
			ctx := context.Background()
			if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
				t.Fatal(err)
			}
			perUserPath := createPerUserPlistConflict(t, fixture, test.symlink)
			mutationsBefore := launchctlMutationCounts(fixture.runner.snapshotCalls())

			if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "per-user uninstall") {
				t.Fatalf("install conflict error = %v", err)
			}
			if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "per-user uninstall") {
				t.Fatalf("status conflict error = %v", err)
			}
			if launchctlMutationCounts(fixture.runner.snapshotCalls()) != mutationsBefore {
				t.Fatal("per-user plist conflict triggered a launchctl mutation")
			}
			if _, err := os.Lstat(perUserPath); err != nil {
				t.Fatalf("per-user plist conflict was changed: %v", err)
			}
		})
	}
}

func TestUnattendedRefusesLoadedPerUserDomains(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*systemRunner, bool)
		get  func(*systemRunner) bool
	}{
		{
			name: "user domain", set: func(runner *systemRunner, value bool) { runner.userLoaded = value },
			get: func(runner *systemRunner) bool { return runner.userLoaded },
		},
		{
			name: "GUI domain", set: func(runner *systemRunner, value bool) { runner.guiLoaded = value },
			get: func(runner *systemRunner) bool { return runner.guiLoaded },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newUnattendedFixture(t)
			ctx := context.Background()
			if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
				t.Fatal(err)
			}
			fixture.runner.mu.Lock()
			test.set(fixture.runner, true)
			fixture.runner.mu.Unlock()
			mutationsBefore := launchctlMutationCounts(fixture.runner.snapshotCalls())

			if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "per-user uninstall") {
				t.Fatalf("install conflict error = %v", err)
			}
			if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "per-user uninstall") {
				t.Fatalf("status conflict error = %v", err)
			}
			if launchctlMutationCounts(fixture.runner.snapshotCalls()) != mutationsBefore {
				t.Fatal("loaded per-user conflict triggered a launchctl mutation")
			}
			fixture.runner.mu.Lock()
			stillLoaded := fixture.runner.loaded
			conflictStillLoaded := test.get(fixture.runner)
			test.set(fixture.runner, false)
			fixture.runner.mu.Unlock()
			if !stillLoaded || !conflictStillLoaded {
				t.Fatal("conflict gate disturbed a loaded supervisor")
			}
		})
	}
}

func TestUnattendedCleanSystemOnlyModePassesConflictGate(t *testing.T) {
	fixture := newUnattendedFixture(t)
	ctx := context.Background()
	if _, err := fixture.manager.InstallUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("clean system-only install: %v", err)
	}
	if _, err := fixture.manager.StatusUnattended(ctx, fixture.config, fixture.username); err != nil {
		t.Fatalf("clean system-only status: %v", err)
	}
	for _, call := range fixture.runner.snapshotCalls() {
		if len(call) >= 3 && (strings.HasPrefix(call[2], "user/") || strings.HasPrefix(call[2], "gui/")) && call[1] != "print" {
			t.Fatalf("clean conflict check mutated a per-user domain: %#v", call)
		}
	}
}

func TestUnattendedValidationRootMustContainLaunchDaemonsDirectory(t *testing.T) {
	fixture := newUnattendedFixture(t)
	fixture.manager.launchDaemonsValidationRoot = fixture.home

	if _, err := fixture.manager.unattendedPlan(fixture.config, fixture.username); err == nil || !strings.Contains(err.Error(), "beneath its protected validation root") {
		t.Fatalf("validation-root escape error = %v", err)
	}
}

func newUnattendedFixture(t *testing.T) *unattendedFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	daemonDirectory := filepath.Join(root, "Library", "LaunchDaemons")
	home := filepath.Join(root, "Users", "worker")
	executable := filepath.Join(root, "usr", "local", "bin", "apc")
	for _, directory := range []string{daemonDirectory, home, filepath.Dir(executable)} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(executable, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(executable, 0o755); err != nil {
		t.Fatal(err)
	}
	owners := make(map[string]testOwnership)
	for _, path := range absoluteDirectoryChain(filepath.Clean(daemonDirectory)) {
		owners[path] = testOwnership{uid: 0, gid: 0}
	}
	owners[filepath.Clean(home)] = testOwnership{uid: 501, gid: 20}
	runner := &systemRunner{}
	manager := &Manager{
		runner: runner, uid: 501, euid: 0, home: home,
		launchDaemonsDirectory: daemonDirectory, launchDaemonsValidationRoot: root,
		lookupAccount: func(username string) (accountRecord, error) {
			if username != "worker" {
				return accountRecord{}, errors.New("account not found")
			}
			return accountRecord{username: username, uid: 501, gid: 20, home: home}, nil
		},
		lookupGroup: func(gid int) (string, error) {
			if gid != 20 {
				return "", errors.New("group not found")
			}
			return "staff", nil
		},
		chown: func(file *os.File, uid, gid int) error {
			owners[filepath.Clean(file.Name())] = testOwnership{uid: uid, gid: gid}
			return nil
		},
		ownership: func(path string, _ os.FileInfo) (int, int, error) {
			owner, exists := owners[filepath.Clean(path)]
			if !exists {
				return 0, 0, errors.New("ownership not recorded")
			}
			return owner.uid, owner.gid, nil
		},
	}
	config := Config{Role: "server", Cluster: "home", Executable: executable, Interval: 20 * time.Second}
	target := targetIdentity{accountRecord: accountRecord{username: "worker", uid: 501, gid: 20, home: home}, groupName: "staff"}
	_, logPath := unattendedUserLogPaths(target, config)
	runner.arguments = unattendedProgramArguments(config, target, logPath)
	return &unattendedFixture{
		manager: manager, runner: runner,
		config:     config,
		username:   "worker",
		home:       home,
		executable: executable,
		daemonDir:  daemonDirectory,
		owners:     owners,
	}
}

func assertExactUnattendedPlist(t *testing.T, fixture *unattendedFixture, plan unattendedPlan) {
	t.Helper()
	info, err := os.Lstat(plan.plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o644 {
		t.Fatalf("plist mode = %v", info.Mode())
	}
	owner := fixture.owners[plan.plistPath]
	if owner.uid != 0 || owner.gid != 0 {
		t.Fatalf("plist owner = %d:%d", owner.uid, owner.gid)
	}
	data, err := os.ReadFile(plan.plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, plan.plist) {
		t.Fatal("installed plist differs from the exact rendered document")
	}
}

func assertSystemDomainCalls(t *testing.T, calls [][]string, plan unattendedPlan) {
	t.Helper()
	wantTarget := "system/" + Label(plan.config)
	foundBootstrap := false
	for _, call := range calls {
		joined := strings.Join(call, " ")
		if strings.Contains(joined, "user/") || strings.Contains(joined, "gui/") {
			if len(call) != 3 || call[1] != "print" {
				t.Fatalf("unattended lifecycle mutated a per-user domain: %#v", call)
			}
			continue
		}
		if len(call) >= 4 && call[1] == "bootstrap" {
			foundBootstrap = call[2] == "system" && call[3] == plan.plistPath
		}
		if (len(call) >= 3 && (call[1] == "print" || call[1] == "enable" || call[1] == "bootout") && call[2] != wantTarget) ||
			(len(call) >= 4 && call[1] == "kickstart" && call[3] != wantTarget) {
			t.Fatalf("unexpected system service target: %#v", call)
		}
	}
	if !foundBootstrap {
		t.Fatalf("system bootstrap call missing: %#v", calls)
	}
}

func countLaunchctlOperation(calls [][]string, operation string) int {
	count := 0
	for _, call := range calls {
		if len(call) > 1 && call[0] == unattendedLaunchctlPath && call[1] == operation {
			count++
		}
	}
	return count
}

func createPerUserPlistConflict(t *testing.T, fixture *unattendedFixture, symbolicLink bool) string {
	t.Helper()
	directory := filepath.Join(fixture.home, "Library", "LaunchAgents")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, Label(fixture.config)+".plist")
	if symbolicLink {
		target := filepath.Join(t.TempDir(), "foreign.plist")
		if err := os.WriteFile(target, []byte("foreign plist"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, path); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		return path
	}
	if err := os.WriteFile(path, []byte("foreign plist"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func launchctlMutationCounts(calls [][]string) [4]int {
	return [4]int{
		countLaunchctlOperation(calls, "bootstrap"),
		countLaunchctlOperation(calls, "enable"),
		countLaunchctlOperation(calls, "kickstart"),
		countLaunchctlOperation(calls, "bootout"),
	}
}
