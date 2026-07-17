package model

import (
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	APIVersion       = "apc.dev/v1alpha1"
	Kind             = "Deployment"
	DefaultNamespace = "default"
)

var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
var memoryQuantity = regexp.MustCompile(`^([1-9][0-9]*)([KMGTPE]?I?)?$`)

// Deployment deliberately mirrors the familiar Kubernetes apps/v1 shape while
// keeping the implementation independent from the large Kubernetes Go modules.
type Deployment struct {
	APIVersion string           `json:"apiVersion" yaml:"apiVersion"`
	Kind       string           `json:"kind" yaml:"kind"`
	Metadata   ObjectMeta       `json:"metadata" yaml:"metadata"`
	Spec       DeploymentSpec   `json:"spec" yaml:"spec"`
	Status     DeploymentStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

type ObjectMeta struct {
	Name            string            `json:"name" yaml:"name"`
	Namespace       string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	UID             string            `json:"uid,omitempty" yaml:"uid,omitempty"`
	ResourceVersion string            `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
	Generation      int64             `json:"generation,omitempty" yaml:"generation,omitempty"`
	Labels          map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`
	CreatedAt       time.Time         `json:"creationTimestamp,omitempty" yaml:"creationTimestamp,omitempty"`
	UpdatedAt       time.Time         `json:"updatedAt,omitempty" yaml:"updatedAt,omitempty"`
}

type DeploymentSpec struct {
	Replicas int                `json:"replicas" yaml:"replicas"`
	Selector LabelSelector      `json:"selector" yaml:"selector"`
	Template PodTemplateSpec    `json:"template" yaml:"template"`
	Strategy DeploymentStrategy `json:"strategy,omitempty" yaml:"strategy,omitempty"`
}

type LabelSelector struct {
	MatchLabels map[string]string `json:"matchLabels" yaml:"matchLabels"`
}

type PodTemplateSpec struct {
	Metadata TemplateMeta `json:"metadata" yaml:"metadata"`
	Spec     PodSpec      `json:"spec" yaml:"spec"`
}

type TemplateMeta struct {
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`
}

type PodSpec struct {
	Containers    []Container       `json:"containers" yaml:"containers"`
	NodeSelector  map[string]string `json:"nodeSelector,omitempty" yaml:"nodeSelector,omitempty"`
	RestartPolicy string            `json:"restartPolicy,omitempty" yaml:"restartPolicy,omitempty"`
}

type Container struct {
	Name           string               `json:"name" yaml:"name"`
	Image          string               `json:"image" yaml:"image"`
	Args           []string             `json:"args,omitempty" yaml:"args,omitempty"`
	Env            []EnvVar             `json:"env,omitempty" yaml:"env,omitempty"`
	Ports          []ContainerPort      `json:"ports,omitempty" yaml:"ports,omitempty"`
	Resources      ResourceRequirements `json:"resources,omitempty" yaml:"resources,omitempty"`
	ReadinessProbe *Probe               `json:"readinessProbe,omitempty" yaml:"readinessProbe,omitempty"`
	LivenessProbe  *Probe               `json:"livenessProbe,omitempty" yaml:"livenessProbe,omitempty"`
}

type EnvVar struct {
	Name  string `json:"name" yaml:"name"`
	Value string `json:"value" yaml:"value"`
}

type ContainerPort struct {
	Name          string `json:"name,omitempty" yaml:"name,omitempty"`
	ContainerPort int    `json:"containerPort" yaml:"containerPort"`
	HostPort      int    `json:"hostPort,omitempty" yaml:"hostPort,omitempty"`
	HostIP        string `json:"hostIP,omitempty" yaml:"hostIP,omitempty"`
	Protocol      string `json:"protocol,omitempty" yaml:"protocol,omitempty"`
}

type ResourceRequirements struct {
	Requests ResourceList `json:"requests,omitempty" yaml:"requests,omitempty"`
	Limits   ResourceList `json:"limits,omitempty" yaml:"limits,omitempty"`
}

type ResourceList struct {
	CPU    int    `json:"cpu,omitempty" yaml:"cpu,omitempty"`
	Memory string `json:"memory,omitempty" yaml:"memory,omitempty"`
}

type Probe struct {
	HTTPGet             *HTTPGetAction   `json:"httpGet,omitempty" yaml:"httpGet,omitempty"`
	TCPSocket           *TCPSocketAction `json:"tcpSocket,omitempty" yaml:"tcpSocket,omitempty"`
	Exec                *ExecAction      `json:"exec,omitempty" yaml:"exec,omitempty"`
	InitialDelaySeconds int              `json:"initialDelaySeconds,omitempty" yaml:"initialDelaySeconds,omitempty"`
	PeriodSeconds       int              `json:"periodSeconds,omitempty" yaml:"periodSeconds,omitempty"`
	TimeoutSeconds      int              `json:"timeoutSeconds,omitempty" yaml:"timeoutSeconds,omitempty"`
	FailureThreshold    int              `json:"failureThreshold,omitempty" yaml:"failureThreshold,omitempty"`
}

type HTTPGetAction struct {
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
	Port int    `json:"port" yaml:"port"`
}

type TCPSocketAction struct {
	Port int `json:"port" yaml:"port"`
}

type ExecAction struct {
	Command []string `json:"command" yaml:"command"`
}

type DeploymentStrategy struct {
	Type          string                `json:"type,omitempty" yaml:"type,omitempty"`
	RollingUpdate RollingUpdateStrategy `json:"rollingUpdate,omitempty" yaml:"rollingUpdate,omitempty"`
}

type RollingUpdateStrategy struct {
	MaxUnavailable int `json:"maxUnavailable,omitempty" yaml:"maxUnavailable,omitempty"`
	MaxSurge       int `json:"maxSurge,omitempty" yaml:"maxSurge,omitempty"`
}

type DeploymentStatus struct {
	ObservedGeneration  int64       `json:"observedGeneration" yaml:"observedGeneration"`
	Replicas            int         `json:"replicas" yaml:"replicas"`
	UpdatedReplicas     int         `json:"updatedReplicas" yaml:"updatedReplicas"`
	ReadyReplicas       int         `json:"readyReplicas" yaml:"readyReplicas"`
	AvailableReplicas   int         `json:"availableReplicas" yaml:"availableReplicas"`
	UnavailableReplicas int         `json:"unavailableReplicas" yaml:"unavailableReplicas"`
	Conditions          []Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

type Condition struct {
	Type               string    `json:"type" yaml:"type"`
	Status             string    `json:"status" yaml:"status"`
	Reason             string    `json:"reason,omitempty" yaml:"reason,omitempty"`
	Message            string    `json:"message,omitempty" yaml:"message,omitempty"`
	LastTransitionTime time.Time `json:"lastTransitionTime" yaml:"lastTransitionTime"`
}

type Node struct {
	ID             string            `json:"id"`
	Hostname       string            `json:"hostname"`
	Address        string            `json:"address"`
	Architecture   string            `json:"architecture"`
	CPUCount       int               `json:"cpuCount"`
	MemoryBytes    int64             `json:"memoryBytes"`
	Labels         map[string]string `json:"labels,omitempty"`
	RuntimeVersion string            `json:"runtimeVersion,omitempty"`
	State          string            `json:"state"`
	LastSeen       time.Time         `json:"lastSeen"`
}

// Workload is APC's lightweight Pod record. Each apple/container VM currently
// hosts exactly one OCI container, so a workload maps one-to-one to a VM.
type Workload struct {
	ID            string            `json:"id"`
	Namespace     string            `json:"namespace"`
	Deployment    string            `json:"deployment"`
	Generation    int64             `json:"generation"`
	Replica       int               `json:"replica"`
	NodeID        string            `json:"nodeId,omitempty"`
	ContainerName string            `json:"containerName"`
	Labels        map[string]string `json:"labels,omitempty"`
	State         string            `json:"state"`
	Ready         bool              `json:"ready"`
	Message       string            `json:"message,omitempty"`
	Address       string            `json:"address,omitempty"`
	RestartCount  int               `json:"restartCount"`
	CreatedAt     time.Time         `json:"createdAt"`
	UpdatedAt     time.Time         `json:"updatedAt"`
}

func (d *Deployment) DefaultAndValidate() error {
	if d.APIVersion == "" {
		d.APIVersion = APIVersion
	}
	if d.Kind == "" {
		d.Kind = Kind
	}
	if d.Metadata.Namespace == "" {
		d.Metadata.Namespace = DefaultNamespace
	}
	if d.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if d.Kind != Kind {
		return fmt.Errorf("kind must be %q", Kind)
	}
	if !dnsLabel.MatchString(d.Metadata.Name) || !dnsLabel.MatchString(d.Metadata.Namespace) {
		return fmt.Errorf("metadata.name and metadata.namespace must be lowercase DNS labels")
	}
	if d.Spec.Replicas < 0 || d.Spec.Replicas > 1000 {
		return fmt.Errorf("spec.replicas must be between 0 and 1000")
	}
	if len(d.Spec.Template.Spec.Containers) != 1 {
		return fmt.Errorf("spec.template.spec.containers must contain exactly one container")
	}
	container := &d.Spec.Template.Spec.Containers[0]
	if !dnsLabel.MatchString(container.Name) {
		return fmt.Errorf("container.name must be a lowercase DNS label")
	}
	if strings.TrimSpace(container.Image) == "" {
		return fmt.Errorf("container.image is required")
	}
	if len(d.Spec.Selector.MatchLabels) == 0 {
		return fmt.Errorf("spec.selector.matchLabels is required")
	}
	for key, value := range d.Spec.Selector.MatchLabels {
		if d.Spec.Template.Metadata.Labels[key] != value {
			return fmt.Errorf("selector %s=%s does not match template labels", key, value)
		}
	}
	if d.Spec.Template.Spec.RestartPolicy == "" {
		d.Spec.Template.Spec.RestartPolicy = "Always"
	}
	if d.Spec.Template.Spec.RestartPolicy != "Always" {
		return fmt.Errorf("only restartPolicy Always is supported for Deployments")
	}
	if d.Spec.Strategy.Type == "" {
		d.Spec.Strategy.Type = "RollingUpdate"
	}
	if d.Spec.Strategy.Type != "RollingUpdate" && d.Spec.Strategy.Type != "Recreate" {
		return fmt.Errorf("strategy.type must be RollingUpdate or Recreate")
	}
	if d.Spec.Strategy.Type == "RollingUpdate" {
		if d.Spec.Strategy.RollingUpdate.MaxUnavailable == 0 && d.Spec.Strategy.RollingUpdate.MaxSurge == 0 {
			d.Spec.Strategy.RollingUpdate.MaxUnavailable = 1
			d.Spec.Strategy.RollingUpdate.MaxSurge = 1
		}
		if d.Spec.Strategy.RollingUpdate.MaxUnavailable < 0 || d.Spec.Strategy.RollingUpdate.MaxSurge < 0 {
			return fmt.Errorf("rollingUpdate values cannot be negative")
		}
	}
	if container.Resources.Limits.CPU < 0 || container.Resources.Requests.CPU < 0 {
		return fmt.Errorf("resource CPU values cannot be negative")
	}
	if container.Resources.Requests.CPU == 0 {
		container.Resources.Requests.CPU = 1
	}
	if container.Resources.Requests.Memory == "" {
		container.Resources.Requests.Memory = "512M"
	}
	if container.Resources.Limits.CPU == 0 {
		container.Resources.Limits.CPU = container.Resources.Requests.CPU
	}
	if container.Resources.Limits.Memory == "" {
		container.Resources.Limits.Memory = container.Resources.Requests.Memory
	}
	requestedMemory, err := ParseMemoryBytes(container.Resources.Requests.Memory)
	if err != nil {
		return fmt.Errorf("resources.requests.memory: %w", err)
	}
	limitedMemory, err := ParseMemoryBytes(container.Resources.Limits.Memory)
	if err != nil {
		return fmt.Errorf("resources.limits.memory: %w", err)
	}
	if container.Resources.Requests.CPU > container.Resources.Limits.CPU || requestedMemory > limitedMemory {
		return fmt.Errorf("resource requests cannot exceed limits")
	}
	seenEnv := map[string]struct{}{}
	for i, env := range container.Env {
		if strings.TrimSpace(env.Name) == "" || strings.Contains(env.Name, "=") {
			return fmt.Errorf("env[%d].name is invalid", i)
		}
		if _, exists := seenEnv[env.Name]; exists {
			return fmt.Errorf("duplicate environment variable %q", env.Name)
		}
		seenEnv[env.Name] = struct{}{}
	}
	seenPorts := map[string]struct{}{}
	for i := range container.Ports {
		port := &container.Ports[i]
		if port.Protocol == "" {
			port.Protocol = "TCP"
		}
		port.Protocol = strings.ToUpper(port.Protocol)
		if port.Protocol != "TCP" && port.Protocol != "UDP" {
			return fmt.Errorf("ports[%d].protocol must be TCP or UDP", i)
		}
		if port.ContainerPort < 1 || port.ContainerPort > 65535 || port.HostPort < 0 || port.HostPort > 65535 {
			return fmt.Errorf("ports[%d] contains an invalid port", i)
		}
		if port.HostIP != "" && net.ParseIP(port.HostIP) == nil {
			return fmt.Errorf("ports[%d].hostIP is invalid", i)
		}
		key := fmt.Sprintf("%s/%d", port.Protocol, port.HostPort)
		if port.HostPort != 0 {
			if _, exists := seenPorts[key]; exists {
				return fmt.Errorf("duplicate host port %s", key)
			}
			seenPorts[key] = struct{}{}
		}
	}
	if err := validateProbe("readinessProbe", container.ReadinessProbe); err != nil {
		return err
	}
	if err := validateProbe("livenessProbe", container.LivenessProbe); err != nil {
		return err
	}
	return nil
}

func validateProbe(name string, probe *Probe) error {
	if probe == nil {
		return nil
	}
	actions := 0
	if probe.HTTPGet != nil {
		actions++
		if probe.HTTPGet.Port < 1 || probe.HTTPGet.Port > 65535 {
			return fmt.Errorf("%s.httpGet.port is invalid", name)
		}
		if probe.HTTPGet.Path == "" {
			probe.HTTPGet.Path = "/"
		}
	}
	if probe.TCPSocket != nil {
		actions++
		if probe.TCPSocket.Port < 1 || probe.TCPSocket.Port > 65535 {
			return fmt.Errorf("%s.tcpSocket.port is invalid", name)
		}
	}
	if probe.Exec != nil {
		actions++
		if len(probe.Exec.Command) == 0 {
			return fmt.Errorf("%s.exec.command is required", name)
		}
	}
	if actions != 1 {
		return fmt.Errorf("%s must define exactly one of httpGet, tcpSocket, or exec", name)
	}
	if probe.PeriodSeconds == 0 {
		probe.PeriodSeconds = 10
	}
	if probe.TimeoutSeconds == 0 {
		probe.TimeoutSeconds = 2
	}
	if probe.FailureThreshold == 0 {
		probe.FailureThreshold = 3
	}
	if probe.PeriodSeconds < 1 || probe.TimeoutSeconds < 1 || probe.FailureThreshold < 1 || probe.InitialDelaySeconds < 0 {
		return fmt.Errorf("%s timing values are invalid", name)
	}
	return nil
}

func (d Deployment) Container() Container { return d.Spec.Template.Spec.Containers[0] }

func (d Deployment) Key() string { return d.Metadata.Namespace + "/" + d.Metadata.Name }

func ParseMemoryBytes(value string) (int64, error) {
	value = strings.ToUpper(strings.TrimSpace(value))
	parts := memoryQuantity.FindStringSubmatch(value)
	if parts == nil {
		return 0, fmt.Errorf("invalid memory quantity %q (use e.g. 512M or 1G)", value)
	}
	number, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory quantity %q", value)
	}
	factors := map[string]int64{"": 1, "K": 1 << 10, "KI": 1 << 10, "M": 1 << 20, "MI": 1 << 20, "G": 1 << 30, "GI": 1 << 30, "T": 1 << 40, "TI": 1 << 40, "P": 1 << 50, "PI": 1 << 50}
	factor, exists := factors[parts[2]]
	if !exists || number > (1<<63-1)/factor {
		return 0, fmt.Errorf("memory quantity %q is too large", value)
	}
	return number * factor, nil
}

func SortedMapPairs(values map[string]string) [][2]string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]string{key, values[key]})
	}
	return pairs
}
