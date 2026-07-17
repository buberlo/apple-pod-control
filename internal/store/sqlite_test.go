package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buberlo/apple-pod-control/internal/model"
)

func TestDeploymentRoundTripAndGeneration(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	deployment := storeTestDeployment()
	stored, created, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || !created || stored.Metadata.Generation != 1 || stored.Metadata.UID == "" {
		t.Fatalf("first upsert: stored=%#v created=%t err=%v", stored, created, err)
	}
	storedAgain, created, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || created || storedAgain.Metadata.Generation != 1 {
		t.Fatalf("idempotent upsert: stored=%#v created=%t err=%v", storedAgain, created, err)
	}
	deployment.Spec.Replicas = 3
	updated, _, err := database.UpsertDeployment(ctx, deployment)
	if err != nil || updated.Metadata.Generation != 2 {
		t.Fatalf("updated generation: stored=%#v err=%v", updated, err)
	}
	read, err := database.GetDeployment(ctx, "default", "web")
	if err != nil || read.Spec.Replicas != 3 {
		t.Fatalf("get: %#v, %v", read, err)
	}
}

func storeTestDeployment() model.Deployment {
	return model.Deployment{
		APIVersion: model.APIVersion, Kind: model.Kind, Metadata: model.ObjectMeta{Name: "web"},
		Spec: model.DeploymentSpec{Replicas: 2, Selector: model.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: model.PodTemplateSpec{Metadata: model.TemplateMeta{Labels: map[string]string{"app": "web"}},
				Spec: model.PodSpec{Containers: []model.Container{{Name: "web", Image: "nginx:alpine"}}}}},
	}
}
