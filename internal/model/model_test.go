package model

import "testing"

func TestDeploymentDefaultAndValidate(t *testing.T) {
	deployment := validDeployment()
	if err := deployment.DefaultAndValidate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if deployment.Metadata.Namespace != DefaultNamespace {
		t.Fatalf("namespace = %q", deployment.Metadata.Namespace)
	}
	container := deployment.Container()
	if container.Ports[0].Protocol != "TCP" {
		t.Fatalf("protocol = %q", container.Ports[0].Protocol)
	}
	if container.Resources.Requests.CPU != 1 || container.Resources.Limits.Memory != "512M" {
		t.Fatalf("unexpected resource defaults: %#v", container.Resources)
	}
	if deployment.Spec.Strategy.Type != "RollingUpdate" || deployment.Spec.Strategy.RollingUpdate.MaxSurge != 1 {
		t.Fatalf("unexpected strategy: %#v", deployment.Spec.Strategy)
	}
}

func TestDeploymentRejectsSelectorMismatch(t *testing.T) {
	deployment := validDeployment()
	deployment.Spec.Template.Metadata.Labels["app"] = "other"
	if err := deployment.DefaultAndValidate(); err == nil {
		t.Fatal("expected selector validation error")
	}
}

func TestDeploymentRejectsMultipleContainers(t *testing.T) {
	deployment := validDeployment()
	deployment.Spec.Template.Spec.Containers = append(deployment.Spec.Template.Spec.Containers, Container{Name: "sidecar", Image: "busybox"})
	if err := deployment.DefaultAndValidate(); err == nil {
		t.Fatal("expected multiple-container validation error")
	}
}

func TestDeploymentRejectsInvalidMemoryAndRequestsAboveLimits(t *testing.T) {
	deployment := validDeployment()
	deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Memory = "lots"
	if err := deployment.DefaultAndValidate(); err == nil {
		t.Fatal("expected invalid memory validation error")
	}
	deployment = validDeployment()
	deployment.Spec.Template.Spec.Containers[0].Resources.Requests.CPU = 4
	deployment.Spec.Template.Spec.Containers[0].Resources.Limits.CPU = 2
	if err := deployment.DefaultAndValidate(); err == nil {
		t.Fatal("expected request above limit validation error")
	}
}

func validDeployment() Deployment {
	return Deployment{
		APIVersion: APIVersion, Kind: Kind, Metadata: ObjectMeta{Name: "web"},
		Spec: DeploymentSpec{
			Replicas: 2, Selector: LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: PodTemplateSpec{
				Metadata: TemplateMeta{Labels: map[string]string{"app": "web"}},
				Spec:     PodSpec{Containers: []Container{{Name: "web", Image: "nginx:alpine", Ports: []ContainerPort{{ContainerPort: 80}}}}},
			},
		},
	}
}
