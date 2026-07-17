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
	newGeneration := 0
	for _, workload := range workloads {
		if workload.Generation == stored.Metadata.Generation && workload.State != "Stopping" {
			activeOld++
		}
		if workload.Generation == updated.Metadata.Generation {
			newGeneration++
		}
	}
	if activeOld == 0 {
		t.Fatalf("rolling update retired every available old replica: %#v", workloads)
	}
	if newGeneration == 0 {
		t.Fatalf("rolling update did not create a new generation: %#v", workloads)
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
