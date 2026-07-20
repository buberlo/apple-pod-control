package cluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	haProxyHost             = "127.0.0.1"
	haProxyHealthInterval   = time.Second
	haProxyHealthTimeout    = 4 * time.Second
	haProxyIdentityInterval = 30 * time.Second
	haProxyIdentityTimeout  = 10 * time.Second
	haProxyDialTimeout      = 2 * time.Second
	haProxyMaxClients       = 128
	haProxyMinUnprivileged  = 1024
)

// HAProxyEndpoint returns the deterministic loopback URL reserved for a saved
// HA cluster. The proxy port is immediately below the smallest published
// member API port, keeping it stable while remaining distinct from every
// backend.
func HAProxyEndpoint(name string) (string, error) {
	config, err := loadHAConfig(name)
	if err != nil {
		return "", err
	}
	return haProxyEndpoint(config)
}

func haProxyEndpoint(config HAConfig) (string, error) {
	if len(config.Members) == 0 {
		return "", fmt.Errorf("HA proxy requires at least one configured API member")
	}
	minimum := 65536
	memberPorts := make(map[int]struct{}, len(config.Members))
	for _, member := range config.Members {
		if member.HostAPIPort < 1 || member.HostAPIPort > 65535 {
			return "", fmt.Errorf("HA member %d has invalid API port %d", member.ID, member.HostAPIPort)
		}
		if _, duplicate := memberPorts[member.HostAPIPort]; duplicate {
			return "", fmt.Errorf("HA member API port %d is duplicated", member.HostAPIPort)
		}
		memberPorts[member.HostAPIPort] = struct{}{}
		if member.HostAPIPort < minimum {
			minimum = member.HostAPIPort
		}
	}
	proxyPort := minimum - 1
	if proxyPort < haProxyMinUnprivileged || proxyPort > 65535 {
		return "", fmt.Errorf("derived HA proxy port %d is invalid or privileged", proxyPort)
	}
	if _, collision := memberPorts[proxyPort]; collision {
		return "", fmt.Errorf("derived HA proxy port %d collides with a member API port", proxyPort)
	}
	return "https://" + net.JoinHostPort(haProxyHost, strconv.Itoa(proxyPort)), nil
}

// ServeHAProxy validates the saved topology and blocks while serving a local
// TLS-pass-through endpoint. Kubernetes TLS is never terminated by APC.
func ServeHAProxy(ctx context.Context, name string) error {
	return NewManager("container").ServeHAProxy(ctx, name)
}

// ServeHAProxy is the Manager-backed variant used by lifecycle integration and
// tests. Cancellation is a clean shutdown and returns nil.
func (m *Manager) ServeHAProxy(ctx context.Context, name string) error {
	return m.serveHAProxy(ctx, name, defaultHAProxyOptions())
}

type haProxyOptions struct {
	healthInterval   time.Duration
	healthTimeout    time.Duration
	identityInterval time.Duration
	identityTimeout  time.Duration
	dialTimeout      time.Duration
	maxClients       int
	listen           func(context.Context, string) (net.Listener, error)
	dial             func(context.Context, string) (net.Conn, error)
	onListening      func(string)
}

func defaultHAProxyOptions() haProxyOptions {
	return haProxyOptions{
		healthInterval:   haProxyHealthInterval,
		healthTimeout:    haProxyHealthTimeout,
		identityInterval: haProxyIdentityInterval,
		identityTimeout:  haProxyIdentityTimeout,
		dialTimeout:      haProxyDialTimeout,
		maxClients:       haProxyMaxClients,
		listen: func(ctx context.Context, address string) (net.Listener, error) {
			return (&net.ListenConfig{}).Listen(ctx, "tcp", address)
		},
		dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
	}
}

type haProxyBackend struct {
	member  HAMember
	address string
}

func (m *Manager) serveHAProxy(ctx context.Context, name string, options haProxyOptions) error {
	if ctx == nil {
		return fmt.Errorf("HA proxy context is nil")
	}
	if ctx.Err() != nil {
		return nil
	}
	options = normalizeHAProxyOptions(options)
	config, err := loadHAConfig(name)
	if err != nil {
		return err
	}
	endpoint, err := haProxyEndpoint(config)
	if err != nil {
		return err
	}
	listenAddress, err := haProxyAddress(endpoint)
	if err != nil {
		return err
	}
	preflightCtx, cancelPreflight := context.WithTimeout(ctx, haRuntimeOperationTimeout)
	backends, err := m.validateHAProxyBackends(preflightCtx, config)
	cancelPreflight()
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	pool := newHAProxyPool(backends)
	health := m.probeHAProxyReadiness(ctx, config, backends, options.healthTimeout)
	pool.replaceHealth(health)
	if ctx.Err() != nil {
		return nil
	}
	if pool.healthyCount() == 0 {
		return fmt.Errorf("HA proxy %q has no healthy API backends", name)
	}

	listener, err := options.listen(ctx, listenAddress)
	if err != nil {
		return fmt.Errorf("listen on HA proxy endpoint %s: %w", endpoint, err)
	}
	if options.onListening != nil {
		options.onListening(endpoint)
	}

	serveCtx, cancelServe := context.WithCancel(ctx)
	connections := newHAProxyConnections()
	cancelCleanupDone := make(chan struct{})
	stopOnCancel := context.AfterFunc(serveCtx, func() {
		defer close(cancelCleanupDone)
		_ = listener.Close()
		connections.closeAll()
	})

	var healthWG sync.WaitGroup
	healthWG.Add(1)
	go func() {
		defer healthWG.Done()
		m.monitorHAProxyHealth(serveCtx, config, pool, options)
	}()

	clients := make(chan struct{}, options.maxClients)
	var clientWG sync.WaitGroup
	var serveErr error
	for {
		client, acceptErr := listener.Accept()
		if acceptErr != nil {
			if serveCtx.Err() == nil {
				serveErr = fmt.Errorf("accept HA proxy connection: %w", acceptErr)
			}
			break
		}
		select {
		case clients <- struct{}{}:
			connections.add(client)
			clientWG.Add(1)
			go func() {
				defer clientWG.Done()
				defer func() { <-clients }()
				m.handleHAProxyClient(serveCtx, client, pool, connections, options)
			}()
		default:
			_ = client.Close()
		}
	}
	cancelServe()
	_ = listener.Close()
	connections.closeAll()
	if !stopOnCancel() {
		<-cancelCleanupDone
	}
	clientWG.Wait()
	healthWG.Wait()
	return serveErr
}

func normalizeHAProxyOptions(options haProxyOptions) haProxyOptions {
	defaults := defaultHAProxyOptions()
	if options.healthInterval <= 0 {
		options.healthInterval = defaults.healthInterval
	}
	if options.healthTimeout <= 0 {
		options.healthTimeout = defaults.healthTimeout
	}
	if options.identityInterval <= 0 {
		options.identityInterval = defaults.identityInterval
	}
	if options.identityTimeout <= 0 {
		options.identityTimeout = defaults.identityTimeout
	}
	if options.dialTimeout <= 0 {
		options.dialTimeout = defaults.dialTimeout
	}
	if options.maxClients <= 0 {
		options.maxClients = defaults.maxClients
	}
	if options.listen == nil {
		options.listen = defaults.listen
	}
	if options.dial == nil {
		options.dial = defaults.dial
	}
	return options
}

func haProxyAddress(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "" {
		return "", fmt.Errorf("invalid HA proxy endpoint %q", endpoint)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || host != haProxyHost {
		return "", fmt.Errorf("HA proxy endpoint must use loopback host %s", haProxyHost)
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < haProxyMinUnprivileged || parsedPort > 65535 {
		return "", fmt.Errorf("HA proxy endpoint has invalid port %q", port)
	}
	return parsed.Host, nil
}

func (m *Manager) validateHAProxyBackends(ctx context.Context, config HAConfig) ([]haProxyBackend, error) {
	backends := make([]haProxyBackend, 0, len(config.Members))
	for _, member := range config.Members {
		record, err := m.inspectHAContainer(ctx, HAContainerName(config.Name, member.ID))
		if err != nil {
			return nil, err
		}
		if err := validateHAContainer(record, config, member); err != nil {
			return nil, err
		}
		endpoint, err := url.Parse(member.apiEndpoint(config.ListenAddress))
		if err != nil || endpoint.Host == "" {
			return nil, fmt.Errorf("HA member %d has invalid API endpoint", member.ID)
		}
		backends = append(backends, haProxyBackend{member: member, address: endpoint.Host})
	}
	sort.Slice(backends, func(i, j int) bool { return backends[i].member.ID < backends[j].member.ID })
	return backends, nil
}

func (m *Manager) probeHAProxyReadiness(ctx context.Context, config HAConfig, backends []haProxyBackend, timeout time.Duration) map[int]bool {
	type result struct {
		id      int
		healthy bool
	}
	results := make(chan result, len(backends))
	for _, backend := range backends {
		backend := backend
		go func() {
			probeCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			results <- result{id: backend.member.ID, healthy: m.probeHAAPI(probeCtx, config, backend.member)}
		}()
	}
	health := make(map[int]bool, len(backends))
	for range backends {
		result := <-results
		health[result.id] = result.healthy
	}
	return health
}

func (m *Manager) validateHAProxyBackendIdentities(ctx context.Context, config HAConfig, backends []haProxyBackend, timeout time.Duration) map[int]bool {
	type result struct {
		id      int
		trusted bool
	}
	results := make(chan result, len(backends))
	for _, backend := range backends {
		backend := backend
		go func() {
			validationCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			record, err := m.inspectHAContainer(validationCtx, HAContainerName(config.Name, backend.member.ID))
			trusted := err == nil && validateHAContainer(record, config, backend.member) == nil && strings.EqualFold(record.Status.State, "running")
			results <- result{id: backend.member.ID, trusted: trusted}
		}()
	}
	identity := make(map[int]bool, len(backends))
	for range backends {
		result := <-results
		identity[result.id] = result.trusted
	}
	return identity
}

func (m *Manager) monitorHAProxyHealth(ctx context.Context, config HAConfig, pool *haProxyPool, options haProxyOptions) {
	healthTicker := time.NewTicker(options.healthInterval)
	defer healthTicker.Stop()
	identityTicker := time.NewTicker(options.identityInterval)
	defer identityTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-healthTicker.C:
			health := m.probeHAProxyReadiness(ctx, config, pool.backendsSnapshot(), options.healthTimeout)
			pool.replaceHealth(health)
		case <-identityTicker.C:
			identity := m.validateHAProxyBackendIdentities(ctx, config, pool.backendsSnapshot(), options.identityTimeout)
			pool.replaceIdentity(identity)
		}
	}
}

func (m *Manager) handleHAProxyClient(ctx context.Context, client net.Conn, pool *haProxyPool, connections *haProxyConnections, options haProxyOptions) {
	defer connections.remove(client)
	defer client.Close()
	for _, backend := range pool.candidates() {
		dialCtx, cancel := context.WithTimeout(ctx, options.dialTimeout)
		upstream, err := options.dial(dialCtx, backend.address)
		cancel()
		if err != nil {
			pool.markUnhealthy(backend.member.ID)
			continue
		}
		connections.add(upstream)
		relayHAProxy(ctx, client, upstream)
		connections.remove(upstream)
		_ = upstream.Close()
		return
	}
}

func relayHAProxy(ctx context.Context, left, right net.Conn) {
	done := make(chan struct{}, 2)
	copyStream := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		if writer, ok := destination.(interface{ CloseWrite() error }); ok {
			_ = writer.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyStream(left, right)
	go copyStream(right, left)
	for completed := 0; completed < 2; completed++ {
		select {
		case <-done:
		case <-ctx.Done():
			_ = left.Close()
			_ = right.Close()
			<-done
			if completed == 0 {
				<-done
			}
			return
		}
	}
}

type haProxyPool struct {
	mu       sync.Mutex
	backends []haProxyBackend
	healthy  map[int]bool
	trusted  map[int]bool
	next     int
}

func newHAProxyPool(backends []haProxyBackend) *haProxyPool {
	pool := &haProxyPool{
		backends: append([]haProxyBackend(nil), backends...),
		healthy:  make(map[int]bool, len(backends)),
		trusted:  make(map[int]bool, len(backends)),
	}
	for _, backend := range backends {
		pool.trusted[backend.member.ID] = true
	}
	return pool
}

func (p *haProxyPool) backendsSnapshot() []haProxyBackend {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]haProxyBackend(nil), p.backends...)
}

func (p *haProxyPool) replaceHealth(health map[int]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthy = make(map[int]bool, len(health))
	for id, healthy := range health {
		p.healthy[id] = healthy
	}
}

func (p *haProxyPool) replaceIdentity(identity map[int]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trusted = make(map[int]bool, len(identity))
	for id, trusted := range identity {
		p.trusted[id] = trusted
	}
}

func (p *haProxyPool) healthyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, backend := range p.backends {
		if p.healthy[backend.member.ID] && p.trusted[backend.member.ID] {
			count++
		}
	}
	return count
}

func (p *haProxyPool) candidates() []haProxyBackend {
	p.mu.Lock()
	defer p.mu.Unlock()
	healthy := make([]haProxyBackend, 0, len(p.backends))
	for _, backend := range p.backends {
		if p.healthy[backend.member.ID] && p.trusted[backend.member.ID] {
			healthy = append(healthy, backend)
		}
	}
	if len(healthy) == 0 {
		return nil
	}
	start := p.next % len(healthy)
	p.next = (p.next + 1) % len(healthy)
	ordered := make([]haProxyBackend, 0, len(healthy))
	ordered = append(ordered, healthy[start:]...)
	ordered = append(ordered, healthy[:start]...)
	return ordered
}

func (p *haProxyPool) markUnhealthy(id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthy[id] = false
}

type haProxyConnections struct {
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	closed      bool
}

func newHAProxyConnections() *haProxyConnections {
	return &haProxyConnections{connections: make(map[net.Conn]struct{})}
}

func (c *haProxyConnections) add(connection net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = connection.Close()
		return
	}
	c.connections[connection] = struct{}{}
}

func (c *haProxyConnections) remove(connection net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.connections, connection)
}

func (c *haProxyConnections) closeAll() {
	c.mu.Lock()
	c.closed = true
	connections := make([]net.Conn, 0, len(c.connections))
	for connection := range c.connections {
		connections = append(connections, connection)
	}
	c.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}
