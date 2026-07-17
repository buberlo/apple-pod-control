package control

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
	"github.com/buberlo/apple-pod-control/internal/model"
	"github.com/buberlo/apple-pod-control/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AgentService struct {
	apcv1.UnimplementedAgentControlServer
	store      *store.Store
	sessions   *Sessions
	reconciler *Reconciler
	logger     *slog.Logger
}

func NewAgentService(database *store.Store, sessions *Sessions, reconciler *Reconciler, logger *slog.Logger) *AgentService {
	return &AgentService{store: database, sessions: sessions, reconciler: reconciler, logger: logger}
}

func (s *AgentService) Connect(stream grpc.BidiStreamingServer[apcv1.AgentMessage, apcv1.ControlMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Unavailable, "receive registration: %v", err)
	}
	if first.Type != apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_REGISTER || first.Node == nil || first.NodeId == "" || first.Node.Id != first.NodeId {
		return status.Error(codes.InvalidArgument, "first message must register a matching node")
	}
	node := model.Node{
		ID: first.Node.Id, Hostname: first.Node.Hostname, Address: first.Node.Address,
		Architecture: first.Node.Architecture, CPUCount: int(first.Node.CpuCount), MemoryBytes: first.Node.MemoryBytes,
		Labels: first.Node.Labels, RuntimeVersion: first.Node.RuntimeVersion, State: "Ready", LastSeen: time.Now().UTC(),
	}
	if node.Architecture != "arm64" {
		return status.Errorf(codes.FailedPrecondition, "node architecture %q is unsupported; Apple Silicon arm64 is required", node.Architecture)
	}
	if err := s.store.UpsertNode(stream.Context(), node); err != nil {
		return status.Errorf(codes.Internal, "store node: %v", err)
	}
	current, remove := s.sessions.Add(node.ID)
	defer remove()
	s.logger.Info("node connected", "node", node.ID, "hostname", node.Hostname, "runtime", node.RuntimeVersion)
	s.reconciler.Wake()

	messages := make(chan *apcv1.AgentMessage, 32)
	errorsChannel := make(chan error, 1)
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					errorsChannel <- err
				} else {
					errorsChannel <- nil
				}
				return
			}
			select {
			case messages <- message:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case err := <-errorsChannel:
			if err == nil {
				return nil
			}
			return status.Errorf(codes.Unavailable, "agent stream: %v", err)
		case command := <-current.commands:
			if err := stream.Send(command); err != nil {
				return status.Errorf(codes.Unavailable, "send command: %v", err)
			}
		case message := <-messages:
			if message.NodeId != node.ID {
				return status.Error(codes.PermissionDenied, "message node ID does not match registered node")
			}
			if err := s.handleMessage(stream.Context(), message); err != nil {
				s.logger.Warn("agent message rejected", "node", node.ID, "type", message.Type.String(), "error", err)
			}
		}
	}
}

func (s *AgentService) handleMessage(ctx context.Context, message *apcv1.AgentMessage) error {
	switch message.Type {
	case apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_HEARTBEAT:
		return s.store.TouchNode(ctx, message.NodeId, time.Now())
	case apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_STATUS:
		for _, observation := range message.Workloads {
			if err := s.store.UpdateWorkloadObservation(ctx, observation.WorkloadId, observation.State, observation.Ready, observation.Message, observation.Address, int(observation.RestartCount)); err != nil && err != store.ErrNotFound {
				return err
			}
		}
		s.reconciler.Wake()
		return nil
	case apcv1.AgentMessageType_AGENT_MESSAGE_TYPE_ACK:
		command, err := s.sessions.Resolve(message.NodeId, message.CommandId)
		if err != nil {
			return err
		}
		return s.reconciler.HandleAck(ctx, command, message.Error)
	default:
		return fmt.Errorf("unexpected agent message type %s", message.Type)
	}
}
