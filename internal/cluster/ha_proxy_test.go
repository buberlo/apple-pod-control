package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHAProxyEndpointIsDeterministicLoopbackAndUnprivileged(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	endpoint, err := HAProxyEndpoint(config.Name)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://" + ports.proxy.Addr().String()
	if endpoint != want {
		t.Fatalf("endpoint = %q, want %q", endpoint, want)
	}

	for index := range config.Members {
		config.Members[index].HostAPIPort = 1024 + index
	}
	if _, err := normalizeHAConfig(config); err == nil || !strings.Contains(err.Error(), "derived HA proxy endpoint") {
		t.Fatalf("normalization accepted privileged derived endpoint: %v", err)
	}
	if _, err := haProxyEndpoint(config); err == nil || !strings.Contains(err.Error(), "privileged") {
		t.Fatalf("direct privileged endpoint error = %v", err)
	}
}

func TestHAProxyForwardsOpaqueBytes(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	closeListener(t, ports.proxy)
	backend := startEchoBackend(t, ports.backends[0], nil)
	closeListener(t, ports.backends[1])
	closeListener(t, ports.backends[2])

	manager := newTestHAProxyManager(config, proxyHealth(true, false, false), 0)
	endpoint, cancel, serveErrors := startTestHAProxy(t, manager, config.Name, testHAProxyOptions(time.Hour))
	payload := []byte{0x16, 0x03, 0x03, 0x00, 0x05, 0xde, 0xad, 0xbe, 0xef, 0x00}
	response := proxyRoundTrip(t, endpoint, payload)
	if !bytes.Equal(response, payload) {
		t.Fatalf("proxy altered opaque payload: %x", response)
	}
	stopTestHAProxy(t, cancel, serveErrors)
	backend.stop(t)
}

func TestHAProxyRoundRobinsHealthyBackends(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	closeListener(t, ports.proxy)
	first := startEchoBackend(t, ports.backends[0], []byte("one:"))
	second := startEchoBackend(t, ports.backends[1], []byte("two:"))
	closeListener(t, ports.backends[2])

	manager := newTestHAProxyManager(config, proxyHealth(true, true, false), 0)
	endpoint, cancel, serveErrors := startTestHAProxy(t, manager, config.Name, testHAProxyOptions(time.Hour))
	want := []string{"one:x", "two:x", "one:x", "two:x"}
	for index, expected := range want {
		if actual := string(proxyRoundTrip(t, endpoint, []byte("x"))); actual != expected {
			t.Fatalf("request %d reached %q, want %q", index, actual, expected)
		}
	}
	stopTestHAProxy(t, cancel, serveErrors)
	first.stop(t)
	second.stop(t)
}

func TestHAProxyHealthMonitorFailsOver(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	closeListener(t, ports.proxy)
	first := startEchoBackend(t, ports.backends[0], []byte("one:"))
	second := startEchoBackend(t, ports.backends[1], []byte("two:"))
	closeListener(t, ports.backends[2])
	health := proxyHealth(true, false, false)
	manager := newTestHAProxyManager(config, health, 0)
	inspectRunner := manager.runner.(*haProxyInspectRunner)
	endpoint, cancel, serveErrors := startTestHAProxy(t, manager, config.Name, testHAProxyOptions(10*time.Millisecond))
	if actual := string(proxyRoundTrip(t, endpoint, []byte("x"))); actual != "one:x" {
		t.Fatalf("initial backend = %q", actual)
	}
	health[1].Store(false)
	health[2].Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if actual := string(proxyRoundTrip(t, endpoint, []byte("x"))); actual == "two:x" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("proxy did not fail over after backend health changed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls := inspectRunner.callCount(); calls != len(config.Members) {
		t.Fatalf("fast readiness loop executed %d container inspect subprocesses, want startup-only %d", calls, len(config.Members))
	}
	stopTestHAProxy(t, cancel, serveErrors)
	first.stop(t)
	second.stop(t)
}

func TestHAProxySlowIdentityRevalidationQuarantinesMismatchedBackend(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	closeListener(t, ports.proxy)
	first := startEchoBackend(t, ports.backends[0], []byte("one:"))
	second := startEchoBackend(t, ports.backends[1], []byte("two:"))
	closeListener(t, ports.backends[2])

	manager := newTestHAProxyManager(config, proxyHealth(true, true, false), 0)
	inspectRunner := manager.runner.(*haProxyInspectRunner)
	options := testHAProxyOptions(time.Hour)
	options.identityInterval = 10 * time.Millisecond
	endpoint, cancel, serveErrors := startTestHAProxy(t, manager, config.Name, options)
	inspectRunner.mismatchMember.Store(1)
	deadline := time.Now().Add(2 * time.Second)
	for inspectRunner.callCount() < len(config.Members)*2 {
		if time.Now().After(deadline) {
			t.Fatal("proxy did not perform slow identity revalidation")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	for index := 0; index < 4; index++ {
		if actual := string(proxyRoundTrip(t, endpoint, []byte("x"))); actual != "two:x" {
			t.Fatalf("untrusted backend remained eligible: %q", actual)
		}
	}
	stopTestHAProxy(t, cancel, serveErrors)
	first.stop(t)
	second.stop(t)
}

func TestHAProxyImmediatelyRetriesAnotherBackendAfterDialFailure(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	closeListener(t, ports.proxy)
	closeListener(t, ports.backends[0])
	second := startEchoBackend(t, ports.backends[1], []byte("two:"))
	closeListener(t, ports.backends[2])

	manager := newTestHAProxyManager(config, proxyHealth(true, true, false), 0)
	endpoint, cancel, serveErrors := startTestHAProxy(t, manager, config.Name, testHAProxyOptions(time.Hour))
	if actual := string(proxyRoundTrip(t, endpoint, []byte("x"))); actual != "two:x" {
		t.Fatalf("dial failover reached %q", actual)
	}
	stopTestHAProxy(t, cancel, serveErrors)
	second.stop(t)
}

func TestHAProxyReturnsErrorWithoutHealthyBackend(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	manager := newTestHAProxyManager(config, proxyHealth(false, false, false), 0)
	options := testHAProxyOptions(time.Hour)
	var listened atomic.Bool
	options.onListening = func(string) { listened.Store(true) }
	err := manager.serveHAProxy(context.Background(), config.Name, options)
	if err == nil || !strings.Contains(err.Error(), "no healthy API backends") {
		t.Fatalf("no-backend error = %v", err)
	}
	if listened.Load() {
		t.Fatal("proxy listened without a healthy backend")
	}
}

func TestHAProxyRejectsOccupiedEndpoint(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	manager := newTestHAProxyManager(config, proxyHealth(true, false, false), 0)
	err := manager.serveHAProxy(context.Background(), config.Name, testHAProxyOptions(time.Hour))
	if err == nil || !strings.Contains(err.Error(), "listen on HA proxy endpoint") {
		t.Fatalf("endpoint collision error = %v", err)
	}
}

func TestHAProxyRefusesMismatchedOwnershipBeforeListening(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	manager := newTestHAProxyManager(config, proxyHealth(true, true, true), 2)
	options := testHAProxyOptions(time.Hour)
	var listened atomic.Bool
	options.onListening = func(string) { listened.Store(true) }
	err := manager.serveHAProxy(context.Background(), config.Name, options)
	if err == nil || !strings.Contains(err.Error(), "not the expected APC server") {
		t.Fatalf("ownership error = %v", err)
	}
	if listened.Load() {
		t.Fatal("proxy listened before exact ownership validation")
	}
}

func TestHAProxyCancellationClosesListenerAndActiveConnections(t *testing.T) {
	ports := reserveHAProxyPorts(t)
	config := saveTestHAProxyConfig(t, ports)
	closeListener(t, ports.proxy)
	backend := startHoldingBackend(t, ports.backends[0])
	closeListener(t, ports.backends[1])
	closeListener(t, ports.backends[2])

	manager := newTestHAProxyManager(config, proxyHealth(true, false, false), 0)
	endpoint, cancel, serveErrors := startTestHAProxy(t, manager, config.Name, testHAProxyOptions(time.Hour))
	address := proxyEndpointAddress(t, endpoint)
	client, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("hold")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("backend did not receive active proxy connection")
	}

	cancel()
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not stop after cancellation")
	}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if _, err := client.Read(buffer); err == nil {
		t.Fatal("active client connection remained open after cancellation")
	}
	_ = client.Close()
	select {
	case <-backend.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream connection remained open after cancellation")
	}
	backend.stop(t)
}

type reservedHAProxyPorts struct {
	proxy    net.Listener
	backends []net.Listener
}

func reserveHAProxyPorts(t *testing.T) reservedHAProxyPorts {
	t.Helper()
	for base := 24000; base <= 60000; base += 4 {
		listeners := make([]net.Listener, 0, 4)
		for offset := 0; offset < 4; offset++ {
			listener, err := net.Listen("tcp", net.JoinHostPort(haProxyHost, strconv.Itoa(base+offset)))
			if err != nil {
				for _, opened := range listeners {
					_ = opened.Close()
				}
				listeners = nil
				break
			}
			listeners = append(listeners, listener)
		}
		if len(listeners) == 4 {
			for _, listener := range listeners {
				listener := listener
				t.Cleanup(func() { _ = listener.Close() })
			}
			return reservedHAProxyPorts{proxy: listeners[0], backends: listeners[1:]}
		}
	}
	t.Fatal("could not reserve four consecutive loopback ports")
	return reservedHAProxyPorts{}
}

func saveTestHAProxyConfig(t *testing.T, ports reservedHAProxyPorts) HAConfig {
	t.Helper()
	setHAConfigHome(t)
	config, err := DefaultHAConfig("ha-proxy-test")
	if err != nil {
		t.Fatal(err)
	}
	config.ListenAddress = haProxyHost
	for index := range config.Members {
		_, rawPort, splitErr := net.SplitHostPort(ports.backends[index].Addr().String())
		if splitErr != nil {
			t.Fatal(splitErr)
		}
		config.Members[index].HostAPIPort, err = strconv.Atoi(rawPort)
		if err != nil {
			t.Fatal(err)
		}
	}
	config, err = normalizeHAConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveHAConfig(config); err != nil {
		t.Fatal(err)
	}
	return config
}

type haProxyInspectRunner struct {
	mu             sync.Mutex
	config         HAConfig
	mismatchMember atomic.Int32
	calls          [][]string
}

func (r *haProxyInspectRunner) Run(_ context.Context, _ string, arguments ...string) ([]byte, []byte, error) {
	r.mu.Lock()
	r.calls = append(r.calls, append([]string(nil), arguments...))
	r.mu.Unlock()
	if len(arguments) != 2 || arguments[0] != "inspect" {
		return nil, []byte("unexpected command"), fmt.Errorf("unexpected command: %v", arguments)
	}
	var selected *HAMember
	for index := range r.config.Members {
		if HAContainerName(r.config.Name, r.config.Members[index].ID) == arguments[1] {
			selected = &r.config.Members[index]
			break
		}
	}
	if selected == nil {
		return nil, []byte("container not found"), errors.New("not found")
	}
	record := configuredHAContainer(r.config, *selected, "running")
	if int32(selected.ID) == r.mismatchMember.Load() {
		record.Configuration.Labels["apc.dev/cluster"] = "foreign"
	}
	data, err := json.Marshal([]haContainerInspect{record})
	return data, nil, err
}

func (r *haProxyInspectRunner) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func newTestHAProxyManager(config HAConfig, health map[int]*atomic.Bool, mismatchMember int) *Manager {
	manager := NewManager("container")
	runner := &haProxyInspectRunner{config: config}
	runner.mismatchMember.Store(int32(mismatchMember))
	manager.runner = runner
	manager.probeHAAPI = func(ctx context.Context, _ HAConfig, member HAMember) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			return health[member.ID].Load()
		}
	}
	return manager
}

func proxyHealth(first, second, third bool) map[int]*atomic.Bool {
	health := map[int]*atomic.Bool{1: {}, 2: {}, 3: {}}
	health[1].Store(first)
	health[2].Store(second)
	health[3].Store(third)
	return health
}

func testHAProxyOptions(interval time.Duration) haProxyOptions {
	options := defaultHAProxyOptions()
	options.healthInterval = interval
	options.healthTimeout = 250 * time.Millisecond
	options.identityInterval = time.Hour
	options.identityTimeout = 250 * time.Millisecond
	options.dialTimeout = 250 * time.Millisecond
	options.maxClients = 16
	return options
}

func startTestHAProxy(t *testing.T, manager *Manager, name string, options haProxyOptions) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	serveErrors := make(chan error, 1)
	options.onListening = func(endpoint string) { ready <- endpoint }
	go func() { serveErrors <- manager.serveHAProxy(ctx, name, options) }()
	select {
	case endpoint := <-ready:
		return endpoint, cancel, serveErrors
	case err := <-serveErrors:
		cancel()
		t.Fatalf("proxy failed before listening: %v", err)
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("proxy did not begin listening")
	}
	return "", cancel, serveErrors
}

func stopTestHAProxy(t *testing.T, cancel context.CancelFunc, serveErrors <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("proxy shutdown error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not stop")
	}
}

func proxyRoundTrip(t *testing.T, endpoint string, payload []byte) []byte {
	t.Helper()
	connection, err := net.DialTimeout("tcp", proxyEndpointAddress(t, endpoint), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write(payload); err != nil {
		t.Fatal(err)
	}
	if tcp, ok := connection.(*net.TCPConn); ok {
		if err := tcp.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	response, err := io.ReadAll(connection)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func proxyEndpointAddress(t *testing.T, endpoint string) string {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Host
}

type echoBackend struct {
	listener net.Listener
	prefix   []byte
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func startEchoBackend(t *testing.T, listener net.Listener, prefix []byte) *echoBackend {
	t.Helper()
	backend := &echoBackend{listener: listener, prefix: append([]byte(nil), prefix...)}
	backend.wg.Add(1)
	go func() {
		defer backend.wg.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			backend.wg.Add(1)
			go func() {
				defer backend.wg.Done()
				defer connection.Close()
				payload, _ := io.ReadAll(connection)
				_, _ = connection.Write(append(append([]byte(nil), backend.prefix...), payload...))
			}()
		}
	}()
	return backend
}

func (b *echoBackend) stop(t *testing.T) {
	t.Helper()
	b.stopOnce.Do(func() { _ = b.listener.Close() })
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("echo backend did not stop")
	}
}

type holdingBackend struct {
	listener net.Listener
	accepted chan struct{}
	closed   chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func startHoldingBackend(t *testing.T, listener net.Listener) *holdingBackend {
	t.Helper()
	backend := &holdingBackend{listener: listener, accepted: make(chan struct{}), closed: make(chan struct{})}
	backend.wg.Add(1)
	go func() {
		defer backend.wg.Done()
		connection, err := listener.Accept()
		if err != nil {
			close(backend.closed)
			return
		}
		close(backend.accepted)
		_, _ = io.Copy(io.Discard, connection)
		_ = connection.Close()
		close(backend.closed)
	}()
	return backend
}

func (b *holdingBackend) stop(t *testing.T) {
	t.Helper()
	b.stopOnce.Do(func() { _ = b.listener.Close() })
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("holding backend did not stop")
	}
}

func closeListener(t *testing.T, listener net.Listener) {
	t.Helper()
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}
