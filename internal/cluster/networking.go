package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// SetNetworkPolicy recreates only the disposable server envelope with K3s's
// NetworkPolicy controller enabled or disabled. Persistent cluster data is not
// modified; a failed transition is rolled back to the previous configuration.
func (m *Manager) SetNetworkPolicy(ctx context.Context, name string, enabled bool, timeout time.Duration) (State, error) {
	config, err := loadClusterConfig(name)
	if err != nil {
		return State{}, err
	}
	if config.EnableNetworkPolicy == enabled {
		return m.Status(ctx, name)
	}
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	previous := config
	if err := m.DeleteServer(ctx, name, true); err != nil {
		return State{}, err
	}
	config.EnableNetworkPolicy = enabled
	config.StartupTimeout = timeout
	state, transitionErr := m.Create(ctx, config)
	if transitionErr == nil {
		return state, nil
	}
	cleanupErr := m.DeleteServer(ctx, name, true)
	if cleanupErr != nil {
		return State{}, errors.Join(fmt.Errorf("reconfigure NetworkPolicy controller: %w", transitionErr), fmt.Errorf("prepare rollback: %w", cleanupErr))
	}
	previous.StartupTimeout = timeout
	_, rollbackErr := m.Create(ctx, previous)
	if rollbackErr != nil {
		return State{}, errors.Join(fmt.Errorf("reconfigure NetworkPolicy controller: %w", transitionErr), fmt.Errorf("automatic rollback failed: %w", rollbackErr))
	}
	return State{}, fmt.Errorf("reconfigure NetworkPolicy controller: %w; previous configuration was restored", transitionErr)
}
