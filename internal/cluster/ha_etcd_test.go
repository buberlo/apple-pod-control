package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

var fakeHAEtcdServerIDs = []string{
	"cf135a7c4f0f5a49",
	"b03c094a200b8a6d",
	"875e6b3201ac44fe",
}

func fakeHAEtcdProbeOutput(memberID int) []byte {
	return fakeHAEtcdProbeOutputWith(memberID, nil)
}

func fakeHAEtcdProbeOutputWith(memberID int, mutate func(string) string) []byte {
	serverID := fakeHAEtcdServerIDs[memberID-1]
	var output strings.Builder
	output.WriteString(`{"health":"true"}` + "\n" + haEtcdOutputSeparator + "\n")
	fmt.Fprintf(&output, "etcd_server_id{server_id=%q} 1\n", serverID)
	output.WriteString("etcd_server_health_success 1\n")
	output.WriteString("etcd_server_has_leader 1\n")
	if memberID == 1 {
		output.WriteString("etcd_server_is_leader 1\n")
	} else {
		output.WriteString("etcd_server_is_leader 0\n")
	}
	output.WriteString("etcd_server_is_learner 0\n")
	for index, peerID := range fakeHAEtcdServerIDs {
		if index+1 == memberID {
			continue
		}
		fmt.Fprintf(&output, "etcd_network_peer_round_trip_time_seconds_count{To=%q} 5\n", peerID)
	}
	value := output.String()
	if mutate != nil {
		value = mutate(value)
	}
	return []byte(value)
}

func TestValidateHAEtcdRepairQuorumPermitsUnreachableTarget(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	probed := make(map[int]int)
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		member := memberForHAContainer(t, config, arguments[1])
		probed[member.ID]++
		if member.ID == 2 {
			return nil, []byte("target etcd unavailable"), errors.New("exit 1")
		}
		return fakeHAEtcdProbeOutput(member.ID), nil, nil
	}}
	manager := NewManager("container")
	manager.runner = runner

	topology, err := manager.validateHAEtcdRepairQuorum(context.Background(), config, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(topology.Members) != 2 || probed[1] != 1 || probed[3] != 1 || probed[2] != 0 {
		t.Fatalf("repair topology = %+v, probes = %#v", topology, probed)
	}
}

func TestValidateHAEtcdRepairQuorumPermitsTargetAsCurrentLeader(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		member := memberForHAContainer(t, config, arguments[1])
		if member.ID == 1 {
			t.Fatal("repair quorum must not probe the target leader")
		}
		return fakeHAEtcdProbeOutput(member.ID), nil, nil
	}}
	manager := NewManager("container")
	manager.runner = runner
	if _, err := manager.validateHAEtcdRepairQuorum(context.Background(), config, 1); err != nil {
		t.Fatal(err)
	}
}

func TestValidateHAEtcdRepairQuorumRejectsDivergentVoters(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(int, string) string
		wantDetail string
	}{
		{
			name: "different target identity",
			mutate: func(memberID int, value string) string {
				if memberID == 3 {
					return strings.Replace(value, fakeHAEtcdServerIDs[1], "1111111111111111", 1)
				}
				return value
			},
			wantDetail: "disagree on target",
		},
		{
			name: "two leaders",
			mutate: func(memberID int, value string) string {
				if memberID == 3 {
					return strings.Replace(value, "etcd_server_is_leader 0", "etcd_server_is_leader 1", 1)
				}
				return value
			},
			wantDetail: "2 leaders",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			config := liveHAConfig(t)
			runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
				member := memberForHAContainer(t, config, arguments[1])
				return fakeHAEtcdProbeOutputWith(member.ID, func(value string) string {
					return test.mutate(member.ID, value)
				}), nil, nil
			}}
			manager := NewManager("container")
			manager.runner = runner
			_, err := manager.validateHAEtcdRepairQuorum(context.Background(), config, 2)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestValidateHAEtcdTopologyAcceptsExactHealthyVotingMembers(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		if len(arguments) == 5 && arguments[0] == "exec" && arguments[2] == "/bin/sh" && arguments[4] == haEtcdLocalProbeScript {
			member := memberForHAContainer(t, config, arguments[1])
			return fakeHAEtcdProbeOutput(member.ID), nil, nil
		}
		return nil, []byte("unexpected"), errors.New("unexpected command")
	}}
	manager := NewManager("container")
	manager.runner = runner

	topology, err := manager.validateHAEtcdTopology(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(topology.Members) != haMemberCount {
		t.Fatalf("members = %#v", topology.Members)
	}
	for _, member := range topology.Members {
		if !member.HealthSuccess || !member.HasLeader || member.IsLearner || len(member.PeerServerIDs) != 2 {
			t.Fatalf("invalid validated member = %+v", member)
		}
	}
}

func TestValidateHAEtcdTopologyAcceptsAccumulatedHealthSuccessCounter(t *testing.T) {
	setHAConfigHome(t)
	config := liveHAConfig(t)
	runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
		member := memberForHAContainer(t, config, arguments[1])
		return fakeHAEtcdProbeOutputWith(member.ID, func(value string) string {
			return strings.Replace(value, "etcd_server_health_success 1", "etcd_server_health_success 42", 1)
		}), nil, nil
	}}
	manager := NewManager("container")
	manager.runner = runner
	if _, err := manager.validateHAEtcdTopology(context.Background(), config); err != nil {
		t.Fatal(err)
	}
}

func TestValidateHAEtcdTopologyRejectsDivergenceLearnerAndLeaderLoss(t *testing.T) {
	tests := []struct {
		name       string
		memberID   int
		mutate     func(string) string
		wantDetail string
	}{
		{
			name:     "divergent peers",
			memberID: 1,
			mutate: func(value string) string {
				return strings.Replace(value, fakeHAEtcdServerIDs[2], "1111111111111111", 1)
			},
			wantDetail: "expected exactly",
		},
		{
			name:     "learner",
			memberID: 2,
			mutate: func(value string) string {
				return strings.Replace(value, "etcd_server_is_learner 0", "etcd_server_is_learner 1", 1)
			},
			wantDetail: "learner",
		},
		{
			name:     "no leader",
			memberID: 3,
			mutate: func(value string) string {
				return strings.Replace(value, "etcd_server_has_leader 1", "etcd_server_has_leader 0", 1)
			},
			wantDetail: "no elected leader",
		},
		{
			name:     "health metric failure",
			memberID: 1,
			mutate: func(value string) string {
				return strings.Replace(value, "etcd_server_health_success 1", "etcd_server_health_success 0", 1)
			},
			wantDetail: "health success counter",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setHAConfigHome(t)
			config := liveHAConfig(t)
			runner := &haTestRunner{handler: func(arguments []string) ([]byte, []byte, error) {
				member := memberForHAContainer(t, config, arguments[1])
				mutate := (func(string) string)(nil)
				if member.ID == test.memberID {
					mutate = test.mutate
				}
				return fakeHAEtcdProbeOutputWith(member.ID, mutate), nil, nil
			}}
			manager := NewManager("container")
			manager.runner = runner
			_, err := manager.validateHAEtcdTopology(context.Background(), config)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}
