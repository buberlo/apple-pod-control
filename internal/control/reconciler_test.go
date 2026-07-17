package control

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/buberlo/apple-pod-control/internal/model"
	"github.com/buberlo/apple-pod-control/internal/store"
)

func TestReconcilerCreatesAndSchedulesDesiredReplicas(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment := controlTestDeployment()
	stored, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertNode(ctx, model.Node{ID: "mac-mini", Hostname: "mac-mini", Architecture: "arm64", CPUCount: 8, MemoryBytes: 16 << 30, Labels: map[string]string{"zone": "desk"}, State: "Ready", LastSeen: time.Now()}); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessions()
	current, remove := sessions.Add("mac-mini")
	defer remove()
	reconciler := NewReconciler(database, sessions, slog.Default(), time.Second)
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	workloads, err := database.ListWorkloads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workloads) != stored.Spec.Replicas {
		t.Fatalf("workloads = %d, want %d", len(workloads), stored.Spec.Replicas)
	}
	for _, workload := range workloads {
		if workload.NodeID != "mac-mini" || workload.State != "Assigned" {
			t.Fatalf("unexpected workload: %#v", workload)
		}
	}
	if len(current.commands) != stored.Spec.Replicas {
		t.Fatalf("commands = %d, want %d", len(current.commands), stored.Spec.Replicas)
	}
}

func TestRollingUpdateKeepsAvailableOldReplica(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment := controlTestDeployment()
	stored, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertNode(ctx, model.Node{ID: "node", Hostname: "node", Architecture: "arm64", CPUCount: 8, MemoryBytes: 16 << 30, Labels: map[string]string{"zone": "desk"}, State: "Ready", LastSeen: time.Now()}); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessions()
	_, remove := sessions.Add("node")
	defer remove()
	reconciler := NewReconciler(database, sessions, slog.Default(), time.Second)
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	old, _ := database.ListWorkloads(ctx)
	for _, workload := range old {
		if err := database.UpdateWorkloadObservation(ctx, workload.ID, "Running", true, "", "192.0.2.1", 0); err != nil {
			t.Fatal(err)
		}
	}
	oldRevision := stored.TemplateRevision()
	deployment.Spec.Template.Spec.Containers[0].Image = "api:v2"
	updated, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || updated.Metadata.Generation != stored.Metadata.Generation+1 {
		t.Fatalf("update: %#v, %v", updated, err)
	}
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	workloads, _ := database.ListWorkloads(ctx)
	activeOld := 0
	newRevision := 0
	for _, workload := range workloads {
		if workload.Revision == oldRevision && workload.State != "Stopping" {
			activeOld++
		}
		if workload.Revision == updated.TemplateRevision() {
			newRevision++
		}
	}
	if activeOld == 0 {
		t.Fatalf("rolling update retired every available old replica: %#v", workloads)
	}
	if newRevision == 0 {
		t.Fatalf("rolling update did not create a new generation: %#v", workloads)
	}
}

func TestScaleKeepsExistingTemplateRevision(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment := controlTestDeployment()
	stored, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil {
		t.Fatal(err)
	}
	reconciler := NewReconciler(database, NewSessions(), slog.Default(), time.Second)
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := database.ListWorkloads(ctx)
	deployment.Spec.Replicas = 3
	scaled, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil {
		t.Fatal(err)
	}
	if scaled.Metadata.Generation == stored.Metadata.Generation {
		t.Fatal("scale should increment Deployment generation")
	}
	if scaled.TemplateRevision() != stored.TemplateRevision() {
		t.Fatal("scale changed the pod template revision")
	}
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := database.ListWorkloads(ctx)
	if len(after) != 3 {
		t.Fatalf("scaled workloads = %d, want 3", len(after))
	}
	for _, existing := range before {
		found := false
		for _, current := range after {
			if current.ID == existing.ID && current.State != "Stopping" {
				found = true
			}
		}
		if !found {
			t.Fatalf("scale replaced existing workload %s", existing.ID)
		}
	}
}

func TestStartFailureEntersBackoff(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment, _, err := database.UpsertDeployment(ctx, controlTestDeployment())
	if err != nil {
		t.Fatal(err)
	}
	workload := newWorkload(deployment, 0)
	workload.NodeID = "node"
	workload.State = "Assigned"
	if err := database.CreateWorkload(ctx, workload); err != nil {
		t.Fatal(err)
	}
	reconciler := NewReconciler(database, NewSessions(), slog.Default(), time.Second)
	command := startCommand(deployment, workload)
	if err := reconciler.HandleAck(ctx, command, "port already in use"); err != nil {
		t.Fatal(err)
	}
	workloads, err := database.ListWorkloads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if workloads[0].State != "Backoff" || workloads[0].NodeID != "" {
		t.Fatalf("unexpected failed workload: %#v", workloads[0])
	}
}

func TestStoppingWorkloadKeepsHostPortReserved(t *testing.T) {
	deployment := controlTestDeployment()
	deployment.Spec.Template.Spec.Containers[0].Ports = []model.ContainerPort{{ContainerPort: 80, HostPort: 18080, Protocol: "TCP"}}
	if err := deployment.DefaultAndValidate(); err != nil {
		t.Fatal(err)
	}
	workload := newWorkload(deployment, 0)
	workload.NodeID = "node"
	workload.State = "Stopping"
	deployments := map[string]model.Deployment{deployment.Key(): deployment}
	if !hostPortConflict("node", deployment, []model.Workload{workload}, deployments) {
		t.Fatal("stopping workload released its host port before deletion acknowledgement")
	}
}

func TestStoppingWorkloadIsRedrivenAfterControlPlaneRestart(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment, _, err := database.UpsertDeployment(ctx, controlTestDeployment())
	if err != nil {
		t.Fatal(err)
	}
	workload := newWorkload(deployment, 0)
	workload.NodeID = "node"
	workload.State = "Stopping"
	if err := database.CreateWorkload(ctx, workload); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessions()
	current, remove := sessions.Add("node")
	defer remove()
	reconciler := NewReconciler(database, sessions, slog.Default(), time.Second)
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(current.commands) != 1 {
		t.Fatalf("expected one re-driven stop command, got %d", len(current.commands))
	}
	if command := <-current.commands; command.Command.GetOperation().String() != "COMMAND_OPERATION_STOP" {
		t.Fatalf("expected stop command, got %s", command.Command.GetOperation())
	}
}

func TestUnknownWorkloadIsAdoptedAfterAgentReconnect(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment, _, err := database.UpsertDeployment(ctx, controlTestDeployment())
	if err != nil {
		t.Fatal(err)
	}
	workload := newWorkload(deployment, 0)
	workload.NodeID = "node"
	workload.State = "Running"
	workload.Ready = true
	if err := database.CreateWorkload(ctx, workload); err != nil {
		t.Fatal(err)
	}
	if err := database.MarkNodeWorkloadsUnknown(ctx, "node", "agent reconnected"); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessions()
	current, remove := sessions.Add("node")
	defer remove()
	reconciler := NewReconciler(database, sessions, slog.Default(), time.Second)
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(current.commands) != 1 {
		t.Fatalf("expected one adoption start command, got %d", len(current.commands))
	}
	if command := <-current.commands; command.Command.GetOperation().String() != "COMMAND_OPERATION_START" {
		t.Fatalf("expected start command, got %s", command.Command.GetOperation())
	}
}

func controlTestDeployment() model.Deployment {
	return model.Deployment{
		APIVersion: model.APIVersion, Kind: model.Kind, Metadata: model.ObjectMeta{Name: "api", Namespace: "default"},
		Spec: model.DeploymentSpec{Replicas: 2, Selector: model.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			Template: model.PodTemplateSpec{Metadata: model.TemplateMeta{Labels: map[string]string{"app": "api"}},
				Spec: model.PodSpec{NodeSelector: map[string]string{"zone": "desk"}, Containers: []model.Container{{Name: "api", Image: "api:test", Resources: model.ResourceRequirements{Requests: model.ResourceList{CPU: 1, Memory: "512M"}, Limits: model.ResourceList{CPU: 1, Memory: "512M"}}}}}}},
	}
}
