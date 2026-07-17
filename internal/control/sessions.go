package control

import (
	"fmt"
	"sync"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
)

type session struct {
	nodeID   string
	commands chan *apcv1.ControlMessage
}

type pendingCommand struct {
	nodeID  string
	command *apcv1.WorkloadCommand
}

type Sessions struct {
	mu         sync.RWMutex
	sessions   map[string]*session
	pending    map[string]pendingCommand
	pendingKey map[string]string
}

func NewSessions() *Sessions {
	return &Sessions{
		sessions: make(map[string]*session), pending: make(map[string]pendingCommand),
		pendingKey: make(map[string]string),
	}
}

func (s *Sessions) Add(nodeID string) (*session, func()) {
	s.mu.Lock()
	if previous := s.sessions[nodeID]; previous != nil {
		delete(s.sessions, nodeID)
	}
	current := &session{nodeID: nodeID, commands: make(chan *apcv1.ControlMessage, 128)}
	s.sessions[nodeID] = current
	s.mu.Unlock()
	return current, func() {
		s.mu.Lock()
		if s.sessions[nodeID] == current {
			delete(s.sessions, nodeID)
			for id, pending := range s.pending {
				if pending.nodeID == nodeID {
					delete(s.pendingKey, operationKey(pending.command))
					delete(s.pending, id)
				}
			}
		}
		s.mu.Unlock()
	}
}

func (s *Sessions) Connected(nodeID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[nodeID] != nil
}

func (s *Sessions) Dispatch(nodeID string, command *apcv1.WorkloadCommand) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.sessions[nodeID]
	if current == nil {
		return false
	}
	key := operationKey(command)
	if _, exists := s.pendingKey[key]; exists {
		return true
	}
	select {
	case current.commands <- &apcv1.ControlMessage{Command: command}:
		s.pending[command.CommandId] = pendingCommand{nodeID: nodeID, command: command}
		s.pendingKey[key] = command.CommandId
		return true
	default:
		return false
	}
}

func (s *Sessions) Resolve(nodeID, commandID string) (*apcv1.WorkloadCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, exists := s.pending[commandID]
	if !exists {
		return nil, fmt.Errorf("unknown command %q", commandID)
	}
	if pending.nodeID != nodeID {
		return nil, fmt.Errorf("command %q belongs to another node", commandID)
	}
	delete(s.pending, commandID)
	delete(s.pendingKey, operationKey(pending.command))
	return pending.command, nil
}

func operationKey(command *apcv1.WorkloadCommand) string {
	return command.WorkloadId + "/" + command.Operation.String()
}
