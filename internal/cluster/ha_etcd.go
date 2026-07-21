package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	haEtcdLocalProbeTimeout = 5 * time.Second
	haEtcdOutputSeparator   = "APC_ETCD_METRICS_BEGIN"
)

var haEtcdLocalProbeScript = "set -eu; wget -q -T 3 -O - http://127.0.0.1:2381/health; printf '\\n" + haEtcdOutputSeparator + "\\n'; wget -q -T 3 -O - http://127.0.0.1:2381/metrics"

type HAEtcdMemberTopology struct {
	MemberID      int      `json:"memberID" yaml:"memberID"`
	NodeName      string   `json:"nodeName" yaml:"nodeName"`
	ServerID      string   `json:"serverID" yaml:"serverID"`
	PeerServerIDs []string `json:"peerServerIDs" yaml:"peerServerIDs"`
	HealthSuccess bool     `json:"healthSuccess" yaml:"healthSuccess"`
	HasLeader     bool     `json:"hasLeader" yaml:"hasLeader"`
	IsLeader      bool     `json:"isLeader" yaml:"isLeader"`
	IsLearner     bool     `json:"isLearner" yaml:"isLearner"`
}

type HAEtcdTopology struct {
	Members []HAEtcdMemberTopology `json:"members" yaml:"members"`
}

// validateHAEtcdTopology probes every member from its own VM loopback. It
// validates actual embedded-etcd membership rather than inferring quorum from
// Kubernetes API readiness. Callers use it while holding the HA operation lock
// before any quorum-sensitive mutation.
func (m *Manager) validateHAEtcdTopology(ctx context.Context, config HAConfig) (HAEtcdTopology, error) {
	result := HAEtcdTopology{Members: make([]HAEtcdMemberTopology, 0, len(config.Members))}
	serverOwners := make(map[string]int, len(config.Members))
	for _, member := range config.Members {
		state, err := m.probeHAEtcdMember(ctx, config, member)
		if err != nil {
			return HAEtcdTopology{}, err
		}
		if owner, duplicate := serverOwners[state.ServerID]; duplicate {
			return HAEtcdTopology{}, fmt.Errorf("HA etcd members %d and %d report duplicate server ID %s", owner, member.ID, state.ServerID)
		}
		serverOwners[state.ServerID] = member.ID
		result.Members = append(result.Members, state)
	}

	if len(result.Members) != haMemberCount || len(serverOwners) != haMemberCount {
		return HAEtcdTopology{}, fmt.Errorf("HA etcd topology requires exactly %d unique members", haMemberCount)
	}
	leaders := 0
	for _, member := range result.Members {
		if member.IsLeader {
			leaders++
		}
		expected := make([]string, 0, haMemberCount-1)
		for serverID := range serverOwners {
			if serverID != member.ServerID {
				expected = append(expected, serverID)
			}
		}
		sort.Strings(expected)
		if !equalStringSlices(member.PeerServerIDs, expected) {
			return HAEtcdTopology{}, fmt.Errorf("HA etcd member %d reports peer IDs %v, expected exactly %v", member.MemberID, member.PeerServerIDs, expected)
		}
	}
	if leaders != 1 {
		return HAEtcdTopology{}, fmt.Errorf("HA etcd topology reports %d leaders, expected exactly one", leaders)
	}
	sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].MemberID < result.Members[j].MemberID })
	return result, nil
}

// validateHAEtcdRepairQuorum proves that the two non-target members form a
// live voting majority before APC replaces an unhealthy target. The target is
// deliberately not probed: its local etcd endpoint may be the failure being
// repaired. Mutual peer evidence and agreement on the third server ID prevent
// two unrelated or divergent members from being treated as a safe quorum.
func (m *Manager) validateHAEtcdRepairQuorum(ctx context.Context, config HAConfig, targetID int) (HAEtcdTopology, error) {
	target := memberByID(config.Members, targetID)
	if target.ID == 0 {
		return HAEtcdTopology{}, fmt.Errorf("HA repair target member %d is not declared", targetID)
	}
	result := HAEtcdTopology{Members: make([]HAEtcdMemberTopology, 0, haMemberCount-1)}
	for _, member := range config.Members {
		if member.ID == targetID {
			continue
		}
		state, err := m.probeHAEtcdMember(ctx, config, member)
		if err != nil {
			return HAEtcdTopology{}, fmt.Errorf("non-target etcd voter %d is not healthy for repair: %w", member.ID, err)
		}
		result.Members = append(result.Members, state)
	}
	if len(result.Members) != haMemberCount-1 {
		return HAEtcdTopology{}, fmt.Errorf("HA repair requires exactly two non-target etcd voters")
	}
	left, right := result.Members[0], result.Members[1]
	if left.ServerID == right.ServerID {
		return HAEtcdTopology{}, fmt.Errorf("non-target etcd voters report duplicate server ID %s", left.ServerID)
	}
	// At most one non-target voter may be leader. Zero is valid when the target
	// is still the elected leader: because both healthy voters have has_leader=1
	// and their exact peer sets agree on one unique third ID, that target is the
	// only possible shared leader. Two local leaders is divergent/split-brain.
	if leaders := boolCount(left.IsLeader, right.IsLeader); leaders > 1 {
		return HAEtcdTopology{}, fmt.Errorf("non-target etcd voters report %d leaders, expected at most one in the proven majority", leaders)
	}
	leftTargetID, err := repairTargetServerID(left, right.ServerID)
	if err != nil {
		return HAEtcdTopology{}, err
	}
	rightTargetID, err := repairTargetServerID(right, left.ServerID)
	if err != nil {
		return HAEtcdTopology{}, err
	}
	if leftTargetID != rightTargetID {
		return HAEtcdTopology{}, fmt.Errorf("non-target etcd voters disagree on target member %d server ID: %s versus %s", targetID, leftTargetID, rightTargetID)
	}
	if leftTargetID == left.ServerID || leftTargetID == right.ServerID {
		return HAEtcdTopology{}, fmt.Errorf("non-target etcd voters do not identify a unique third server ID for target member %d", targetID)
	}
	sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].MemberID < result.Members[j].MemberID })
	return result, nil
}

func (m *Manager) probeHAEtcdMember(ctx context.Context, config HAConfig, member HAMember) (HAEtcdMemberTopology, error) {
	probeCtx, cancel := context.WithTimeout(ctx, haEtcdLocalProbeTimeout)
	stdout, stderr, err := m.runner.Run(probeCtx, m.binary,
		"exec", HAContainerName(config.Name, member.ID), "/bin/sh", "-c", haEtcdLocalProbeScript,
	)
	contextErr := probeCtx.Err()
	cancel()
	if err != nil {
		if contextErr != nil {
			return HAEtcdMemberTopology{}, fmt.Errorf("probe local etcd state on HA member %d: %w", member.ID, contextErr)
		}
		return HAEtcdMemberTopology{}, commandError(fmt.Sprintf("probe local etcd state on HA member %d", member.ID), stderr, err)
	}
	state, err := parseHAEtcdLocalState(stdout, member)
	if err != nil {
		return HAEtcdMemberTopology{}, fmt.Errorf("validate local etcd state on HA member %d: %w", member.ID, err)
	}
	return state, nil
}

func repairTargetServerID(member HAEtcdMemberTopology, otherVoterID string) (string, error) {
	if len(member.PeerServerIDs) != haMemberCount-1 || !containsString(member.PeerServerIDs, otherVoterID) {
		return "", fmt.Errorf("non-target etcd voter %d does not report the other voter %s in its exact peer set %v", member.MemberID, otherVoterID, member.PeerServerIDs)
	}
	for _, peerID := range member.PeerServerIDs {
		if peerID != otherVoterID {
			return peerID, nil
		}
	}
	return "", fmt.Errorf("non-target etcd voter %d does not report a distinct target peer", member.MemberID)
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func parseHAEtcdLocalState(output []byte, member HAMember) (HAEtcdMemberTopology, error) {
	parts := bytes.SplitN(output, []byte("\n"+haEtcdOutputSeparator+"\n"), 2)
	if len(parts) != 2 {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd probe did not return its health/metrics separator")
	}
	healthSuccess, err := parseHAEtcdHealth(parts[0])
	if err != nil {
		return HAEtcdMemberTopology{}, err
	}
	state := HAEtcdMemberTopology{MemberID: member.ID, NodeName: member.NodeName, HealthSuccess: healthSuccess}
	metrics := make(map[string]float64)
	peerIDs := make(map[string]struct{}, haMemberCount-1)
	scanner := bufio.NewScanner(bytes.NewReader(parts[1]))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		metricWithLabels := fields[0]
		metricName := metricWithLabels
		if index := strings.IndexByte(metricName, '{'); index >= 0 {
			metricName = metricName[:index]
		}
		value, valueErr := strconv.ParseFloat(fields[len(fields)-1], 64)
		if valueErr == nil {
			metrics[metricName] = value
		}
		if strings.HasPrefix(metricName, "etcd_network_peer_") {
			if rawID, ok := prometheusLabel(metricWithLabels, "To"); ok {
				canonical, canonicalErr := canonicalHAEtcdServerID(rawID)
				if canonicalErr != nil {
					return HAEtcdMemberTopology{}, fmt.Errorf("invalid etcd peer server ID %q", rawID)
				}
				peerIDs[canonical] = struct{}{}
			}
		}
		if metricName == "etcd_server_id" {
			if rawID, ok := prometheusLabel(metricWithLabels, "server_id"); ok {
				canonical, canonicalErr := canonicalHAEtcdServerID(rawID)
				if canonicalErr != nil {
					return HAEtcdMemberTopology{}, fmt.Errorf("invalid local etcd server ID %q", rawID)
				}
				state.ServerID = canonical
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return HAEtcdMemberTopology{}, fmt.Errorf("scan local etcd metrics: %w", err)
	}
	if state.ServerID == "" {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd metrics do not contain a valid etcd_server_id")
	}
	healthMetric, ok := metrics["etcd_server_health_success"]
	if !ok || healthMetric <= 0 || math.IsNaN(healthMetric) || math.IsInf(healthMetric, 0) || math.Trunc(healthMetric) != healthMetric {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd health success counter is not a positive finite integer")
	}
	hasLeader, ok := metrics["etcd_server_has_leader"]
	if !ok || hasLeader != 1 {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd member has no elected leader")
	}
	state.HasLeader = true
	isLeader, ok := metrics["etcd_server_is_leader"]
	if !ok || (isLeader != 0 && isLeader != 1) {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd metrics do not expose a valid leader role")
	}
	state.IsLeader = isLeader == 1
	learner, ok := metrics["etcd_server_is_learner"]
	if !ok {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd metrics do not expose learner state")
	}
	if learner != 0 {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd member is still a learner")
	}
	state.IsLearner = false
	if !state.HealthSuccess {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd health endpoint did not report success")
	}
	for id := range peerIDs {
		if id != state.ServerID {
			state.PeerServerIDs = append(state.PeerServerIDs, id)
		}
	}
	sort.Strings(state.PeerServerIDs)
	if len(state.PeerServerIDs) != haMemberCount-1 {
		return HAEtcdMemberTopology{}, fmt.Errorf("local etcd member reports %d unique peers, expected exactly %d", len(state.PeerServerIDs), haMemberCount-1)
	}
	return state, nil
}

func parseHAEtcdHealth(data []byte) (bool, error) {
	var document struct {
		Health json.RawMessage `json:"health"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(data), &document); err != nil {
		return false, fmt.Errorf("decode local etcd health: %w", err)
	}
	var boolean bool
	if err := json.Unmarshal(document.Health, &boolean); err == nil {
		return boolean, nil
	}
	var text string
	if err := json.Unmarshal(document.Health, &text); err == nil {
		return strings.EqualFold(text, "true"), nil
	}
	return false, fmt.Errorf("local etcd health response has an invalid health value")
}

func prometheusLabel(metric, name string) (string, bool) {
	start := strings.IndexByte(metric, '{')
	end := strings.LastIndexByte(metric, '}')
	if start < 0 || end <= start {
		return "", false
	}
	for _, label := range strings.Split(metric[start+1:end], ",") {
		key, value, ok := strings.Cut(label, "=")
		if !ok || strings.TrimSpace(key) != name {
			continue
		}
		unquoted, err := strconv.Unquote(strings.TrimSpace(value))
		if err != nil {
			return "", false
		}
		return unquoted, true
	}
	return "", false
}

func canonicalHAEtcdServerID(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 16 {
		return "", errors.New("invalid etcd server ID")
	}
	number, err := strconv.ParseUint(value, 16, 64)
	if err != nil || number == 0 {
		return "", errors.New("invalid etcd server ID")
	}
	return strconv.FormatUint(number, 16), nil
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
