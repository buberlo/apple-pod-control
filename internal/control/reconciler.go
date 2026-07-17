package control

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
	"github.com/buberlo/apple-pod-control/internal/model"
	"github.com/buberlo/apple-pod-control/internal/store"
)

type Reconciler struct {
	store    *store.Store
	sessions *Sessions
	logger   *slog.Logger
	interval time.Duration
	wake     chan struct{}
}

func NewReconciler(database *store.Store, sessions *Sessions, logger *slog.Logger, interval time.Duration) *Reconciler {
	if interval == 0 {
		interval = 2 * time.Second
	}
	return &Reconciler{store: database, sessions: sessions, logger: logger, interval: interval, wake: make(chan struct{}, 1)}
}

func (r *Reconciler) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.Reconcile(ctx); err != nil && ctx.Err() == nil {
			r.logger.Error("reconciliation failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

func (r *Reconciler) Reconcile(ctx context.Context) error {
	if err := r.store.MarkStaleNodes(ctx, time.Now().Add(-15*time.Second)); err != nil {
		return err
	}
	deployments, err := r.store.ListDeployments(ctx, "")
	if err != nil {
		return err
	}
	workloads, err := r.store.ListWorkloads(ctx)
	if err != nil {
		return err
	}
	byKey := make(map[string]model.Deployment, len(deployments))
	for _, deployment := range deployments {
		byKey[deployment.Key()] = deployment
	}

	for index := range workloads {
		workload := workloads[index]
		_, desired := byKey[workload.Namespace+"/"+workload.Deployment]
		if !desired || workload.State == "Failed" {
			if err := r.retire(ctx, workload); err != nil {
				return err
			}
		}
	}

	for _, deployment := range deployments {
		if err := r.reconcileDeployment(ctx, deployment, workloads); err != nil {
			return fmt.Errorf("reconcile deployment %s: %w", deployment.Key(), err)
		}
	}

	workloads, err = r.store.ListWorkloads(ctx)
	if err != nil {
		return err
	}
	nodes, err := r.store.ListNodes(ctx)
	if err != nil {
		return err
	}
	return r.schedule(ctx, byKey, nodes, workloads)
}

func (r *Reconciler) reconcileDeployment(ctx context.Context, deployment model.Deployment, all []model.Workload) error {
	var current, old []model.Workload
	for _, workload := range all {
		if workload.Namespace != deployment.Metadata.Namespace || workload.Deployment != deployment.Metadata.Name || workload.State == "Stopping" {
			continue
		}
		if workload.Generation == deployment.Metadata.Generation {
			current = append(current, workload)
		} else {
			old = append(old, workload)
		}
	}
	desired := deployment.Spec.Replicas
	if len(old) == 0 {
		for len(current) < desired {
			workload := newWorkload(deployment, nextReplica(current))
			if err := r.store.CreateWorkload(ctx, workload); err != nil {
				return err
			}
			current = append(current, workload)
		}
		for len(current) > desired {
			workload := current[len(current)-1]
			if err := r.retire(ctx, workload); err != nil {
				return err
			}
			current = current[:len(current)-1]
		}
		return nil
	}

	if deployment.Spec.Strategy.Type == "Recreate" {
		for _, workload := range old {
			if err := r.retire(ctx, workload); err != nil {
				return err
			}
		}
		return nil
	}

	maxSurge := deployment.Spec.Strategy.RollingUpdate.MaxSurge
	maxUnavailable := deployment.Spec.Strategy.RollingUpdate.MaxUnavailable
	maxTotal := desired + maxSurge
	for len(current)+len(old) < maxTotal && len(current) < desired {
		workload := newWorkload(deployment, nextReplica(current))
		if err := r.store.CreateWorkload(ctx, workload); err != nil {
			return err
		}
		current = append(current, workload)
	}
	ready := readyCount(current) + readyCount(old)
	minimumAvailable := desired - maxUnavailable
	for len(old) > 0 && (ready > minimumAvailable || readyCount(current) >= desired) {
		victim := old[len(old)-1]
		if victim.Ready {
			ready--
		}
		if err := r.retire(ctx, victim); err != nil {
			return err
		}
		old = old[:len(old)-1]
		if len(current)+len(old) <= desired {
			break
		}
	}
	return nil
}

func (r *Reconciler) retire(ctx context.Context, workload model.Workload) error {
	if workload.NodeID == "" {
		return r.store.DeleteWorkload(ctx, workload.ID)
	}
	if workload.State != "Stopping" {
		if err := r.store.SetWorkloadState(ctx, workload.ID, "Stopping", "terminating obsolete workload"); err != nil {
			return err
		}
	}
	command := &apcv1.WorkloadCommand{
		CommandId: uuid.NewString(), Operation: apcv1.CommandOperation_COMMAND_OPERATION_STOP,
		WorkloadId: workload.ID, ContainerName: workload.ContainerName,
	}
	r.sessions.Dispatch(workload.NodeID, command)
	return nil
}

func (r *Reconciler) schedule(ctx context.Context, deployments map[string]model.Deployment, nodes []model.Node, workloads []model.Workload) error {
	allocation := buildAllocation(deployments, workloads)
	for index := range workloads {
		workload := workloads[index]
		deployment, exists := deployments[workload.Namespace+"/"+workload.Deployment]
		if !exists || workload.Generation != deployment.Metadata.Generation || workload.State == "Stopping" {
			continue
		}
		if workload.State == "Assigned" || (workload.State == "Starting" && time.Since(workload.UpdatedAt) > 15*time.Second) {
			if r.sessions.Connected(workload.NodeID) {
				r.sessions.Dispatch(workload.NodeID, startCommand(deployment, workload))
			}
			continue
		}
		if workload.State != "Pending" || workload.NodeID != "" {
			continue
		}
		node := selectNode(deployment, nodes, workloads, deployments, allocation, r.sessions)
		if node == nil {
			_ = r.store.SetWorkloadState(ctx, workload.ID, "Pending", "Unschedulable: no ready node satisfies selectors, resources, and host ports")
			continue
		}
		if err := r.store.AssignWorkload(ctx, workload.ID, node.ID); err != nil {
			return err
		}
		workload.NodeID = node.ID
		workload.State = "Assigned"
		workloads[index] = workload
		requestCPU, requestMemory := requestedResources(deployment)
		allocation[node.ID] = resourceAllocation{cpu: allocation[node.ID].cpu + requestCPU, memory: allocation[node.ID].memory + requestMemory, count: allocation[node.ID].count + 1}
		r.sessions.Dispatch(node.ID, startCommand(deployment, workload))
	}
	return nil
}

func (r *Reconciler) HandleAck(ctx context.Context, command *apcv1.WorkloadCommand, errorText string) error {
	if errorText != "" {
		if command.Operation == apcv1.CommandOperation_COMMAND_OPERATION_START {
			return r.store.UnassignWorkload(ctx, command.WorkloadId, "start failed: "+errorText)
		}
		return r.store.SetWorkloadState(ctx, command.WorkloadId, "Failed", "stop failed: "+errorText)
	}
	switch command.Operation {
	case apcv1.CommandOperation_COMMAND_OPERATION_START:
		if err := r.store.SetWorkloadState(ctx, command.WorkloadId, "Starting", "container started; awaiting observation"); err != nil {
			return err
		}
	case apcv1.CommandOperation_COMMAND_OPERATION_STOP:
		if err := r.store.DeleteWorkload(ctx, command.WorkloadId); err != nil {
			return err
		}
	}
	r.Wake()
	return nil
}

func newWorkload(deployment model.Deployment, replica int) model.Workload {
	id := uuid.NewString()
	suffix := strings.ReplaceAll(id[:8], "-", "")
	return model.Workload{
		ID: id, Namespace: deployment.Metadata.Namespace, Deployment: deployment.Metadata.Name,
		Generation: deployment.Metadata.Generation, Replica: replica,
		ContainerName: fmt.Sprintf("%s-%s-%s", deployment.Metadata.Name, strconv.FormatInt(deployment.Metadata.Generation, 36), suffix),
		Labels:        deployment.Spec.Template.Metadata.Labels, State: "Pending",
	}
}

func startCommand(deployment model.Deployment, workload model.Workload) *apcv1.WorkloadCommand {
	container := deployment.Container()
	environment := make(map[string]string, len(container.Env))
	for _, variable := range container.Env {
		environment[variable.Name] = variable.Value
	}
	ports := make([]*apcv1.Port, 0, len(container.Ports))
	for _, port := range container.Ports {
		ports = append(ports, &apcv1.Port{Name: port.Name, ContainerPort: int32(port.ContainerPort), HostPort: int32(port.HostPort), HostIp: port.HostIP, Protocol: strings.ToLower(port.Protocol)})
	}
	return &apcv1.WorkloadCommand{
		CommandId: uuid.NewString(), Operation: apcv1.CommandOperation_COMMAND_OPERATION_START,
		WorkloadId: workload.ID, ContainerName: workload.ContainerName, Image: container.Image,
		Environment: environment, Ports: ports, Arguments: container.Args,
		Cpus: int32(container.Resources.Limits.CPU), Memory: container.Resources.Limits.Memory,
		Readiness: probeMessage(container.ReadinessProbe), Liveness: probeMessage(container.LivenessProbe), Architecture: "arm64",
	}
}

func probeMessage(probe *model.Probe) *apcv1.HealthCheck {
	if probe == nil {
		return nil
	}
	message := &apcv1.HealthCheck{
		InitialDelaySeconds: int32(probe.InitialDelaySeconds), IntervalSeconds: int32(probe.PeriodSeconds),
		TimeoutSeconds: int32(probe.TimeoutSeconds), FailureThreshold: int32(probe.FailureThreshold),
	}
	switch {
	case probe.HTTPGet != nil:
		message.Type, message.Path, message.Port = "http", probe.HTTPGet.Path, int32(probe.HTTPGet.Port)
	case probe.TCPSocket != nil:
		message.Type, message.Port = "tcp", int32(probe.TCPSocket.Port)
	case probe.Exec != nil:
		message.Type, message.Command = "exec", probe.Exec.Command
	}
	return message
}

type resourceAllocation struct {
	cpu    int
	memory int64
	count  int
}

func buildAllocation(deployments map[string]model.Deployment, workloads []model.Workload) map[string]resourceAllocation {
	result := make(map[string]resourceAllocation)
	for _, workload := range workloads {
		if workload.NodeID == "" || workload.State == "Stopping" {
			continue
		}
		deployment, exists := deployments[workload.Namespace+"/"+workload.Deployment]
		if !exists {
			continue
		}
		cpu, memory := requestedResources(deployment)
		used := result[workload.NodeID]
		result[workload.NodeID] = resourceAllocation{cpu: used.cpu + cpu, memory: used.memory + memory, count: used.count + 1}
	}
	return result
}

func selectNode(deployment model.Deployment, nodes []model.Node, workloads []model.Workload, deployments map[string]model.Deployment, allocation map[string]resourceAllocation, sessions *Sessions) *model.Node {
	cpu, memory := requestedResources(deployment)
	candidates := make([]model.Node, 0, len(nodes))
	for _, node := range nodes {
		if node.State != "Ready" || node.Architecture != "arm64" || !sessions.Connected(node.ID) || !labelsMatch(deployment.Spec.Template.Spec.NodeSelector, node.Labels) {
			continue
		}
		used := allocation[node.ID]
		if node.CPUCount > 0 && used.cpu+cpu > node.CPUCount {
			continue
		}
		if node.MemoryBytes > 0 && used.memory+memory > node.MemoryBytes {
			continue
		}
		if hostPortConflict(node.ID, deployment, workloads, deployments) {
			continue
		}
		candidates = append(candidates, node)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := allocation[candidates[i].ID], allocation[candidates[j].ID]
		if left.count == right.count {
			return candidates[i].ID < candidates[j].ID
		}
		return left.count < right.count
	})
	if len(candidates) == 0 {
		return nil
	}
	return &candidates[0]
}

func hostPortConflict(nodeID string, desired model.Deployment, workloads []model.Workload, deployments map[string]model.Deployment) bool {
	desiredPorts := staticHostPorts(desired)
	if len(desiredPorts) == 0 {
		return false
	}
	for _, workload := range workloads {
		if workload.NodeID != nodeID || workload.State == "Stopping" {
			continue
		}
		existing, found := deployments[workload.Namespace+"/"+workload.Deployment]
		if !found {
			continue
		}
		for port := range staticHostPorts(existing) {
			if _, conflict := desiredPorts[port]; conflict {
				return true
			}
		}
	}
	return false
}

func staticHostPorts(deployment model.Deployment) map[string]struct{} {
	result := map[string]struct{}{}
	for _, port := range deployment.Container().Ports {
		if port.HostPort > 0 {
			result[fmt.Sprintf("%s:%d/%s", port.HostIP, port.HostPort, port.Protocol)] = struct{}{}
		}
	}
	return result
}

func requestedResources(deployment model.Deployment) (int, int64) {
	resources := deployment.Container().Resources.Requests
	return resources.CPU, parseMemory(resources.Memory)
}

func parseMemory(value string) int64 {
	bytes, _ := model.ParseMemoryBytes(value)
	return bytes
}

func labelsMatch(selector, labels map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func readyCount(workloads []model.Workload) int {
	count := 0
	for _, workload := range workloads {
		if workload.Ready {
			count++
		}
	}
	return count
}

func nextReplica(workloads []model.Workload) int {
	used := make(map[int]struct{}, len(workloads))
	for _, workload := range workloads {
		used[workload.Replica] = struct{}{}
	}
	for replica := 0; ; replica++ {
		if _, exists := used[replica]; !exists {
			return replica
		}
	}
}
