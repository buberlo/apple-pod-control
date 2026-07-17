package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
	containerruntime "github.com/buberlo/apple-pod-control/internal/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	ServerAddress     string
	NodeID            string
	AdvertiseAddress  string
	Labels            map[string]string
	HeartbeatInterval time.Duration
	ObserveInterval   time.Duration
	CommandTimeout    time.Duration
	TLSCAFile         string
	TLSCertFile       string
	TLSKeyFile        string
	TLSServerName     string
}

type Agent struct {
	config  Config
	runtime containerruntime.Runtime
	logger  *slog.Logger

	mu      sync.RWMutex
	managed map[string]*managedWorkload
}

type managedWorkload struct {
	command          *apcv1.WorkloadCommand
	startedAt        time.Time
	restartCount     int32
	livenessFailures int32
}

func New(config Config, containerRuntime containerruntime.Runtime, logger *slog.Logger) (*Agent, error) {
	if config.ServerAddress == "" {
		return nil, fmt.Errorf("server address is required")
	}
	if config.NodeID == "" {
		hostname, _ := os.Hostname()
		config.NodeID = strings.ToLower(strings.ReplaceAll(hostname, ".", "-"))
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.ObserveInterval == 0 {
		config.ObserveInterval = 2 * time.Second
	}
	if config.CommandTimeout == 0 {
		config.CommandTimeout = 10 * time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	if containerRuntime == nil {
		return nil, fmt.Errorf("container runtime is required")
	}
	return &Agent{config: config, runtime: containerRuntime, logger: logger, managed: make(map[string]*managedWorkload)}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	backoff := time.Second
	for ctx.Err() == nil {
		err := a.connect(ctx)
		if ctx.Err() != nil {
			return nil
		}
		a.logger.Warn("control plane connection lost", "error", err, "retryIn", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	return nil
}

func (a *Agent) connect(ctx context.Context) error {
	transportCredentials, err := a.transportCredentials()
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(a.config.ServerAddress, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer connection.Close()
	stream, err := apcv1.NewAgentControlClient(connection).Connect(ctx)
	if err != nil {
		return fmt.Errorf("open control stream: %w", err)
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outbound := make(chan *apcv1.AgentMessage, 128)
	errorsChannel := make(chan error, 2)
	go a.sendLoop(sessionCtx, stream, outbound, errorsChannel)
	commands := make(chan *apcv1.WorkloadCommand, 64)
	go a.receiveLoop(sessionCtx, stream, commands, errorsChannel)

	outbound <- &apcv1.AgentMessage{Type: apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_REGISTER, NodeId: a.config.NodeID, Node: a.nodeInfo(sessionCtx)}
	a.logger.Info("connected to control plane", "address", a.config.ServerAddress, "node", a.config.NodeID)

	heartbeat := time.NewTicker(a.config.HeartbeatInterval)
	defer heartbeat.Stop()
	observe := time.NewTicker(a.config.ObserveInterval)
	defer observe.Stop()
	observeDone := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errorsChannel:
			return err
		case command := <-commands:
			go a.handleCommand(sessionCtx, command, outbound)
		case <-heartbeat.C:
			a.enqueue(outbound, &apcv1.AgentMessage{Type: apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_HEARTBEAT, NodeId: a.config.NodeID})
		case <-observe.C:
			select {
			case observeDone <- struct{}{}:
				go func() {
					defer func() { <-observeDone }()
					observations := a.observe(sessionCtx)
					a.enqueue(outbound, &apcv1.AgentMessage{Type: apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_STATUS, NodeId: a.config.NodeID, Workloads: observations})
				}()
			default:
			}
		}
	}
}

func (a *Agent) sendLoop(ctx context.Context, stream grpc.BidiStreamingClient[apcv1.AgentMessage, apcv1.ControlMessage], outbound <-chan *apcv1.AgentMessage, errorsChannel chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-outbound:
			if err := stream.Send(message); err != nil {
				a.sendError(errorsChannel, fmt.Errorf("send agent message: %w", err))
				return
			}
		}
	}
}

func (a *Agent) receiveLoop(ctx context.Context, stream grpc.BidiStreamingClient[apcv1.AgentMessage, apcv1.ControlMessage], commands chan<- *apcv1.WorkloadCommand, errorsChannel chan<- error) {
	for {
		message, err := stream.Recv()
		if err != nil {
			if ctx.Err() == nil {
				a.sendError(errorsChannel, fmt.Errorf("receive control message: %w", err))
			}
			return
		}
		if message.Command == nil {
			continue
		}
		select {
		case commands <- message.Command:
		case <-ctx.Done():
			return
		}
	}
}

func (a *Agent) handleCommand(ctx context.Context, command *apcv1.WorkloadCommand, outbound chan<- *apcv1.AgentMessage) {
	commandCtx, cancel := context.WithTimeout(ctx, a.config.CommandTimeout)
	defer cancel()
	var err error
	switch command.Operation {
	case apcv1.CommandOperation_COMMAND_OPERATION_START:
		err = a.runtime.Start(commandCtx, command)
		if err == nil {
			a.mu.Lock()
			a.managed[command.WorkloadId] = &managedWorkload{command: command, startedAt: time.Now()}
			a.mu.Unlock()
		}
	case apcv1.CommandOperation_COMMAND_OPERATION_STOP:
		err = a.runtime.Stop(commandCtx, command)
		if err == nil {
			a.mu.Lock()
			delete(a.managed, command.WorkloadId)
			a.mu.Unlock()
		}
	default:
		err = fmt.Errorf("unsupported command operation %s", command.Operation)
	}
	errorText := ""
	if err != nil {
		errorText = err.Error()
		a.logger.Error("workload command failed", "command", command.CommandId, "workload", command.WorkloadId, "error", err)
	} else {
		a.logger.Info("workload command completed", "operation", command.Operation.String(), "workload", command.WorkloadId)
	}
	a.enqueue(outbound, &apcv1.AgentMessage{Type: apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_ACK, NodeId: a.config.NodeID, CommandId: command.CommandId, Error: errorText})
}

func (a *Agent) observe(ctx context.Context) []*apcv1.WorkloadObservation {
	a.mu.RLock()
	items := make([]*managedWorkload, 0, len(a.managed))
	for _, item := range a.managed {
		items = append(items, item)
	}
	a.mu.RUnlock()
	result := make([]*apcv1.WorkloadObservation, 0, len(items))
	for _, item := range items {
		observeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		observation, err := a.runtime.Observe(observeCtx, item.command)
		cancel()
		if err != nil {
			observation.Ready = false
			if !errors.Is(err, containerruntime.ErrNotFound) {
				observation.State = "Unknown"
				observation.Message = err.Error()
			}
		} else if observation.State == "Running" {
			observation.Ready = a.checkReadiness(ctx, item, observation.Address, &observation)
			a.checkLiveness(ctx, item, observation.Address, &observation)
		}
		result = append(result, &apcv1.WorkloadObservation{
			WorkloadId: item.command.WorkloadId, ContainerName: item.command.ContainerName,
			State: observation.State, Ready: observation.Ready, Message: observation.Message,
			Address: observation.Address, RestartCount: item.restartCount,
		})
	}
	return result
}

func (a *Agent) checkReadiness(ctx context.Context, item *managedWorkload, address string, observation *containerruntime.Observation) bool {
	probe := item.command.Readiness
	if probe == nil || probe.Type == "" {
		return true
	}
	if time.Since(item.startedAt) < time.Duration(probe.InitialDelaySeconds)*time.Second {
		observation.Message = "readiness probe initial delay"
		return false
	}
	if err := a.runtime.Probe(ctx, item.command, probe, address); err != nil {
		observation.Message = "readiness probe failed: " + err.Error()
		return false
	}
	return true
}

func (a *Agent) checkLiveness(ctx context.Context, item *managedWorkload, address string, observation *containerruntime.Observation) {
	probe := item.command.Liveness
	if probe == nil || probe.Type == "" || time.Since(item.startedAt) < time.Duration(probe.InitialDelaySeconds)*time.Second {
		return
	}
	if err := a.runtime.Probe(ctx, item.command, probe, address); err == nil {
		item.livenessFailures = 0
		return
	} else {
		item.livenessFailures++
		observation.Message = "liveness probe failed: " + err.Error()
	}
	threshold := probe.FailureThreshold
	if threshold == 0 {
		threshold = 3
	}
	if item.livenessFailures < threshold {
		return
	}
	restartCtx, cancel := context.WithTimeout(ctx, a.config.CommandTimeout)
	defer cancel()
	if err := a.runtime.Stop(restartCtx, item.command); err != nil {
		observation.State = "Failed"
		observation.Message = "liveness restart stop failed: " + err.Error()
		return
	}
	if err := a.runtime.Start(restartCtx, item.command); err != nil {
		observation.State = "Failed"
		observation.Message = "liveness restart start failed: " + err.Error()
		return
	}
	item.restartCount++
	item.livenessFailures = 0
	item.startedAt = time.Now()
	observation.Ready = false
	observation.Message = "restarted after liveness probe failure"
}

func (a *Agent) nodeInfo(ctx context.Context) *apcv1.NodeInfo {
	hostname, _ := os.Hostname()
	return &apcv1.NodeInfo{
		Id: a.config.NodeID, Hostname: hostname, Address: a.config.AdvertiseAddress,
		Architecture: runtime.GOARCH, CpuCount: int32(runtime.NumCPU()), MemoryBytes: memoryBytes(),
		Labels: a.config.Labels, RuntimeVersion: a.runtime.Version(ctx),
	}
}

func memoryBytes() int64 {
	output, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0
	}
	value, _ := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	return value
}

func (a *Agent) transportCredentials() (credentials.TransportCredentials, error) {
	if a.config.TLSCAFile == "" {
		return insecure.NewCredentials(), nil
	}
	caData, err := os.ReadFile(a.config.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("TLS CA file contains no certificates")
	}
	configuration := &tls.Config{RootCAs: roots, ServerName: a.config.TLSServerName, MinVersion: tls.VersionTLS13}
	if a.config.TLSCertFile != "" || a.config.TLSKeyFile != "" {
		certificate, err := tls.LoadX509KeyPair(a.config.TLSCertFile, a.config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load agent TLS certificate: %w", err)
		}
		configuration.Certificates = []tls.Certificate{certificate}
	}
	return credentials.NewTLS(configuration), nil
}

func (a *Agent) enqueue(outbound chan<- *apcv1.AgentMessage, message *apcv1.AgentMessage) {
	select {
	case outbound <- message:
	default:
		a.logger.Warn("dropping agent message because outbound queue is full", "type", message.Type.String())
	}
}

func (a *Agent) sendError(channel chan<- error, err error) {
	select {
	case channel <- err:
	default:
	}
}
