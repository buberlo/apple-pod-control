package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"
)

const (
	haRuntimeAddressRetryLimit   = haMemberCount
	haRuntimeAddressProbePeriod  = 50 * time.Millisecond
	haRuntimeAddressProbeTimeout = 5 * time.Second
	haRuntimeIPCollisionMarker   = "APC_HA_RUNTIME_IP_COLLISION="
)

type haRuntimeIPCollision struct {
	address       string
	runtimeMember HAMember
	stableMember  HAMember
}

func (collision *haRuntimeIPCollision) Error() string {
	return fmt.Sprintf(
		"HA member %d runtime IPv4 %s overlaps the declared stable IP of member %d",
		collision.runtimeMember.ID,
		collision.address,
		collision.stableMember.ID,
	)
}

func haRuntimeIPCollisionForAddress(config HAConfig, runtimeMember HAMember, address string) *haRuntimeIPCollision {
	parsed, err := netip.ParseAddr(address)
	if err != nil || !parsed.Is4() {
		return nil
	}
	for _, stableMember := range config.Members {
		if stableMember.ID == runtimeMember.ID || stableMember.StableIP != parsed.String() {
			continue
		}
		return &haRuntimeIPCollision{
			address:       parsed.String(),
			runtimeMember: runtimeMember,
			stableMember:  stableMember,
		}
	}
	return nil
}

func haContainerRuntimeIPv4(record haContainerInspect) (string, error) {
	if len(record.Status.Networks) != 1 {
		return "", fmt.Errorf("runtime reported %d network attachments, expected exactly one", len(record.Status.Networks))
	}
	prefix, err := netip.ParsePrefix(record.Status.Networks[0].IPv4Address)
	if err != nil || !prefix.Addr().Is4() {
		return "", fmt.Errorf("runtime reported invalid IPv4 address %q", record.Status.Networks[0].IPv4Address)
	}
	return prefix.Addr().String(), nil
}

func haRuntimeIPCollisionFromOutput(output []byte, config HAConfig, member HAMember) *haRuntimeIPCollision {
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, haRuntimeIPCollisionMarker) {
			continue
		}
		address := strings.TrimSpace(strings.TrimPrefix(line, haRuntimeIPCollisionMarker))
		if collision := haRuntimeIPCollisionForAddress(config, member, address); collision != nil {
			return collision
		}
	}
	return nil
}

func validateHACurrentRuntimeIPReservations(config HAConfig, records map[int]haContainerInspect) error {
	ids := make([]int, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		record := records[id]
		if !strings.EqualFold(record.Status.State, "running") || len(record.Status.Networks) == 0 {
			continue
		}
		member := memberByID(config.Members, id)
		address, err := haContainerRuntimeIPv4(record)
		if err != nil {
			return fmt.Errorf("inspect HA member %d runtime address: %w", id, err)
		}
		collision := haRuntimeIPCollisionForAddress(config, member, address)
		if collision == nil {
			continue
		}
		return fmt.Errorf(
			"%w; apple/container 1.0 allocates runtime IPv4 independently of the requested MAC and cannot reserve IPv4 with container run; recycle member %d while all three members are healthy, or use a validated HA restore when quorum is already degraded",
			collision,
			collision.runtimeMember.ID,
		)
	}
	return nil
}

func (m *Manager) validateHAPeerRuntimeIPReservations(ctx context.Context, config HAConfig, target HAMember) error {
	probeCtx, cancel := context.WithTimeout(ctx, haRuntimeAddressProbeTimeout)
	defer cancel()
	records := make(map[int]haContainerInspect, len(config.Members)-1)
	for _, member := range config.Members {
		if member.ID == target.ID {
			continue
		}
		record, err := m.inspectHAContainer(probeCtx, HAContainerName(config.Name, member.ID))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			if probeCtx.Err() != nil {
				return fmt.Errorf("inspect peer runtime addresses before starting HA member %d: %w", target.ID, probeCtx.Err())
			}
			return err
		}
		if err := validateHAContainer(record, config, member); err != nil {
			return err
		}
		records[member.ID] = record
	}
	return validateHACurrentRuntimeIPReservations(config, records)
}

func (m *Manager) waitHAServerRuntimeAddress(ctx context.Context, config HAConfig, member HAMember) error {
	probeCtx, cancel := context.WithTimeout(ctx, haRuntimeAddressProbeTimeout)
	defer cancel()
	ticker := time.NewTicker(haRuntimeAddressProbePeriod)
	defer ticker.Stop()

	name := HAContainerName(config.Name, member.ID)
	for {
		record, err := m.inspectHAContainer(probeCtx, name)
		if err == nil {
			if validationErr := validateHAContainer(record, config, member); validationErr != nil {
				return validationErr
			}
			if len(record.Status.Networks) == 1 {
				address, addressErr := haContainerRuntimeIPv4(record)
				if addressErr != nil {
					return fmt.Errorf("inspect newly started HA member %d: %w", member.ID, addressErr)
				}
				if collision := haRuntimeIPCollisionForAddress(config, member, address); collision != nil {
					return collision
				}
				return nil
			}
			if strings.EqualFold(record.Status.State, "stopped") {
				stdout, stderr, logErr := m.runner.Run(probeCtx, m.binary, "logs", "-n", "20", name)
				if logErr == nil {
					logs := append(append([]byte(nil), stdout...), stderr...)
					if collision := haRuntimeIPCollisionFromOutput(logs, config, member); collision != nil {
						return collision
					}
				}
				return fmt.Errorf("newly started HA member %d stopped before reporting a runtime IPv4 address; inspect it with container logs %s", member.ID, name)
			}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}

		select {
		case <-probeCtx.Done():
			return fmt.Errorf("HA member %d did not report a runtime IPv4 address within %s: %w", member.ID, haRuntimeAddressProbeTimeout, probeCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) deleteExactHAServerEnvelope(ctx context.Context, config HAConfig, member HAMember) error {
	name := HAContainerName(config.Name, member.ID)
	record, err := m.inspectHAContainer(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateHAContainer(record, config, member); err != nil {
		return err
	}
	if strings.EqualFold(record.Status.State, "running") {
		stopErr := m.runHABounded(ctx, fmt.Sprintf("stop conflicting HA server member %d", member.ID), "stop", name)
		if stopErr != nil {
			refreshed, inspectErr := m.inspectHAContainer(ctx, name)
			if inspectErr != nil {
				return errors.Join(stopErr, inspectErr)
			}
			if err := validateHAContainer(refreshed, config, member); err != nil {
				return errors.Join(stopErr, err)
			}
			if !strings.EqualFold(refreshed.Status.State, "stopped") {
				return stopErr
			}
		}
	}
	record, err = m.inspectHAContainer(ctx, name)
	if err != nil {
		return err
	}
	if err := validateHAContainer(record, config, member); err != nil {
		return err
	}
	if !strings.EqualFold(record.Status.State, "stopped") {
		return fmt.Errorf("refusing to delete conflicting HA member %d because its exact envelope is %s", member.ID, record.Status.State)
	}
	return m.runHABounded(ctx, fmt.Sprintf("delete conflicting HA server member %d", member.ID), "delete", name)
}

func (m *Manager) runHAServerEnvelope(ctx context.Context, config HAConfig, member HAMember, operation string, runTimeout time.Duration) error {
	attempted := make([]string, 0, haRuntimeAddressRetryLimit)
	for attempt := 1; attempt <= haRuntimeAddressRetryLimit; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", operation, err)
		}
		if err := m.validateHAPeerRuntimeIPReservations(ctx, config, member); err != nil {
			return fmt.Errorf("%s preflight found a runtime/stable IP collision before creating member %d: %w", operation, member.ID, err)
		}
		runCtx, cancel := context.WithTimeout(ctx, runTimeout)
		_, stderr, runErr := m.runner.Run(runCtx, m.binary, HAServerRunArguments(config, member)...)
		contextErr := runCtx.Err()
		cancel()
		if contextErr != nil {
			return fmt.Errorf("%s timed out after %s: %w", operation, runTimeout, contextErr)
		}

		var collision *haRuntimeIPCollision
		if runErr != nil {
			collision = haRuntimeIPCollisionFromOutput(stderr, config, member)
			if collision == nil {
				return commandError(operation, stderr, runErr)
			}
		} else {
			addressErr := m.waitHAServerRuntimeAddress(ctx, config, member)
			if addressErr == nil {
				return nil
			}
			if !errors.As(addressErr, &collision) {
				return addressErr
			}
		}

		attempted = append(attempted, collision.address)
		if err := m.deleteExactHAServerEnvelope(ctx, config, member); err != nil {
			return errors.Join(
				fmt.Errorf("%s: %w", operation, collision),
				fmt.Errorf("remove the exact conflicting envelope before retry: %w", err),
			)
		}
	}
	return fmt.Errorf(
		"%s could not obtain a non-reserved runtime IPv4 for HA member %d after %d attempts (allocated: %s); apple/container 1.0 does not support fixed IPv4 on container run, and the conflicting exact envelopes were removed; retry after checking the dedicated network for foreign attachments",
		operation,
		member.ID,
		haRuntimeAddressRetryLimit,
		strings.Join(attempted, ", "),
	)
}
