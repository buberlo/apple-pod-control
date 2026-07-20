package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

type DiagnosticStatus string

const (
	DiagnosticPass DiagnosticStatus = "PASS"
	DiagnosticWarn DiagnosticStatus = "WARN"
	DiagnosticFail DiagnosticStatus = "FAIL"
)

type DiagnosticResult struct {
	Name        string           `json:"name" yaml:"name"`
	Status      DiagnosticStatus `json:"status" yaml:"status"`
	Detail      string           `json:"detail" yaml:"detail"`
	Remediation string           `json:"remediation,omitempty" yaml:"remediation,omitempty"`
}

type DiagnosticReport struct {
	Cluster   string             `json:"cluster" yaml:"cluster"`
	Namespace string             `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Results   []DiagnosticResult `json:"results" yaml:"results"`
}

type DiagnoseOptions struct {
	Image        string
	Timeout      time.Duration
	ProbeTimeout time.Duration
	Keep         bool
	SkipEgress   bool
}

func (r DiagnosticReport) FailureCount() int {
	count := 0
	for _, result := range r.Results {
		if result.Status == DiagnosticFail {
			count++
		}
	}
	return count
}

func (r DiagnosticReport) WarningCount() int {
	count := 0
	for _, result := range r.Results {
		if result.Status == DiagnosticWarn {
			count++
		}
	}
	return count
}

func (r DiagnosticReport) PassedCount() int {
	return len(r.Results) - r.FailureCount() - r.WarningCount()
}

func (r DiagnosticReport) WriteText(writer io.Writer) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "CHECK\tSTATUS\tDETAIL"); err != nil {
		return err
	}
	for _, result := range r.Results {
		detail := result.Detail
		if result.Remediation != "" {
			detail += "; " + result.Remediation
		}
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", result.Name, result.Status, detail); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(writer, "\nSummary: %d passed, %d warnings, %d failed\n", r.PassedCount(), r.WarningCount(), r.FailureCount())
	return err
}

type diagnosticNode struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Unschedulable bool `json:"unschedulable"`
	} `json:"spec"`
	Status struct {
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		Addresses []struct {
			Type    string `json:"type"`
			Address string `json:"address"`
		} `json:"addresses"`
	} `json:"status"`
}

type diagnosticPod struct {
	Name string
	Node string
	IP   string
}

func (m *Manager) Diagnose(ctx context.Context, name string, options DiagnoseOptions) (report DiagnosticReport, err error) {
	report.Cluster = name
	if !dnsLabel.MatchString(name) {
		return report, fmt.Errorf("cluster name must be a lowercase DNS label")
	}
	if options.Image == "" {
		options.Image = "docker.io/library/nginx:alpine"
	}
	if options.Timeout == 0 {
		options.Timeout = 2 * time.Minute
	}
	if options.ProbeTimeout == 0 {
		options.ProbeTimeout = 8 * time.Second
	}
	if options.Timeout < options.ProbeTimeout {
		return report, fmt.Errorf("diagnostic timeout must be greater than or equal to probe timeout")
	}
	diagnosticCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	add := func(check string, status DiagnosticStatus, detail, remediation string) {
		report.Results = append(report.Results, DiagnosticResult{Name: check, Status: status, Detail: detail, Remediation: remediation})
	}

	haConfig, haConfigErr := loadHAConfig(name)
	switch {
	case haConfigErr == nil:
		haState, statusErr := m.StatusHA(diagnosticCtx, name)
		if statusErr != nil {
			add("control-plane", DiagnosticFail, conciseError(statusErr), "run: apc cluster ha status "+name)
			return report, nil
		}
		for _, member := range haState.Members {
			memberName := member.NodeName
			if memberName == "" {
				memberName = "member-" + strconv.Itoa(member.ID)
			}
			if strings.EqualFold(member.RuntimeState, "running") {
				add("ha-runtime/"+memberName, DiagnosticPass, member.Container+" is running", "")
			} else {
				add("ha-runtime/"+memberName, DiagnosticFail, member.Container+" is "+member.RuntimeState, "run: apc cluster ha start "+name)
			}
			if member.APIReady {
				add("host-api/"+memberName, DiagnosticPass, member.APIEndpoint+" is Ready", "")
			} else {
				add("host-api/"+memberName, DiagnosticFail, member.APIEndpoint+" is not Ready", "verify the published API port, TLS credentials and member logs")
			}
			if member.NodeReady {
				detail := memberName + " is Kubernetes Ready"
				if member.K3sVersion != "" {
					detail += " (" + member.K3sVersion + ")"
				}
				add("ha-node/"+memberName, DiagnosticPass, detail, "")
			} else {
				add("ha-node/"+memberName, DiagnosticFail, memberName+" is not Kubernetes Ready", "inspect K3s and kubelet logs")
			}
		}
		quorumDetail := fmt.Sprintf("%d/%d Ready node/API pairs; quorum requires %d", haState.ReadyMembers, len(haState.Members), haState.Quorum)
		if haState.Healthy {
			add("etcd-quorum", DiagnosticPass, quorumDetail, "")
		} else {
			add("etcd-quorum", DiagnosticFail, quorumDetail, "restore at least "+strconv.Itoa(haState.Quorum)+" HA members before running workload probes")
			return report, nil
		}
		etcdTopology, topologyErr := m.validateHAEtcdTopology(diagnosticCtx, haConfig)
		if topologyErr != nil {
			add("etcd-topology", DiagnosticFail, conciseError(topologyErr), "repair embedded-etcd membership before creating diagnostic workloads or stopping a member")
			return report, nil
		}
		add("etcd-topology", DiagnosticPass, fmt.Sprintf("%d unique healthy voting members with exact peer topology", len(etcdTopology.Members)), "")
	case !errors.Is(haConfigErr, os.ErrNotExist):
		add("control-plane", DiagnosticFail, conciseError(haConfigErr), "repair the protected HA cluster configuration")
		return report, nil
	default:
		state, statusErr := m.Status(diagnosticCtx, name)
		if statusErr != nil {
			add("control-plane", DiagnosticFail, conciseError(statusErr), "run: apc cluster status "+name)
			return report, nil
		}
		if !strings.EqualFold(state.RuntimeState, "running") {
			add("control-plane", DiagnosticFail, "Apple VM is "+state.RuntimeState, "run: apc cluster start "+name)
			return report, nil
		}
		if state.NodeReady {
			add("control-plane", DiagnosticPass, fmt.Sprintf("%s is Kubernetes Ready (%s)", state.NodeName, state.K3sVersion), "")
		} else {
			add("control-plane", DiagnosticFail, state.NodeName+" is not Kubernetes Ready", "inspect K3s and kubelet logs")
		}

		if endpoint, parseErr := url.Parse(state.APIEndpoint); parseErr != nil || endpoint.Host == "" {
			add("host-api", DiagnosticFail, "invalid published API endpoint "+state.APIEndpoint, "recreate the server node")
		} else {
			probeCtx, probeCancel := context.WithTimeout(diagnosticCtx, options.ProbeTimeout)
			dialErr := m.dialTCP(probeCtx, endpoint.Host)
			probeCancel()
			if dialErr != nil {
				add("host-api", DiagnosticFail, conciseError(dialErr), "verify the published API port and host firewall")
			} else {
				add("host-api", DiagnosticPass, endpoint.Host+" accepts TCP connections", "")
			}
		}
	}

	nodesOutput, nodesError := m.diagnosticKubectl(diagnosticCtx, name, "get", "nodes", "-o", "json")
	if nodesError != nil {
		add("nodes", DiagnosticFail, conciseError(nodesError), "run: apc get nodes -o wide")
		return report, nil
	}
	var nodeList struct {
		Items []diagnosticNode `json:"items"`
	}
	if decodeErr := json.Unmarshal(nodesOutput, &nodeList); decodeErr != nil {
		add("nodes", DiagnosticFail, "decode Kubernetes node list: "+decodeErr.Error(), "")
		return report, nil
	}
	readyNodes := make([]diagnosticNode, 0, len(nodeList.Items))
	internalIPOwners := make(map[string]string, len(nodeList.Items))
	for _, node := range nodeList.Items {
		ready := nodeReady(node)
		detail := nodeAddressSummary(node)
		if node.Spec.Unschedulable {
			detail += ", cordoned"
		}
		if ready {
			add("node/"+node.Metadata.Name, DiagnosticPass, "Ready, "+detail, "")
			readyNodes = append(readyNodes, node)
		} else {
			add("node/"+node.Metadata.Name, DiagnosticFail, "NotReady, "+detail, "inspect node conditions and kubelet logs")
		}
		for _, address := range node.Status.Addresses {
			if address.Type != "InternalIP" || net.ParseIP(address.Address) == nil {
				continue
			}
			if owner, duplicate := internalIPOwners[address.Address]; duplicate {
				add("node-internal-ip/"+address.Address, DiagnosticFail, owner+" and "+node.Metadata.Name+" advertise the same InternalIP", "recreate one APC node envelope before scheduling workloads")
			} else {
				internalIPOwners[address.Address] = node.Metadata.Name
			}
		}
	}
	if len(readyNodes) == 0 {
		add("probe-workloads", DiagnosticFail, "no Ready nodes available", "restore at least one node")
		return report, nil
	}

	suffix, suffixErr := diagnosticSuffix()
	if suffixErr != nil {
		return report, suffixErr
	}
	report.Namespace = "default"
	resourcePrefix := "apc-doctor-" + suffix
	resources := make([]string, 0, len(readyNodes)+1)
	add("probe-scope", DiagnosticPass, report.Namespace+" namespace, run "+resourcePrefix, "")
	defer func() {
		if options.Keep {
			add("cleanup", DiagnosticWarn, strings.Join(resources, ", ")+" retained by request", "delete the listed resources after inspection")
			return
		}
		if len(resources) == 0 {
			add("cleanup", DiagnosticPass, "no probe resources to delete", "")
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		arguments := []string{"delete", "-n", report.Namespace, "--ignore-not-found=true", "--wait=true", "--timeout=12s", "--force", "--grace-period=0"}
		arguments = append(arguments, resources...)
		if _, cleanupErr := m.diagnosticKubectl(cleanupCtx, name, arguments...); cleanupErr != nil {
			add("cleanup", DiagnosticWarn, conciseError(cleanupErr), "delete "+strings.Join(resources, " ")+" manually")
		} else {
			add("cleanup", DiagnosticPass, "probe Pods and Service deleted", "")
		}
	}()

	pods := make([]diagnosticPod, 0, len(readyNodes))
	for index, node := range readyNodes {
		podName := fmt.Sprintf("%s-%d", resourcePrefix, index)
		// Register the exact name before creation. A timed-out kubectl request may
		// have reached the API server even when its caller observed an error.
		resources = append(resources, "pod/"+podName)
		overrides, marshalErr := json.Marshal(map[string]any{
			"apiVersion": "v1",
			"spec": map[string]any{
				"nodeName":                      node.Metadata.Name,
				"terminationGracePeriodSeconds": 0,
			},
		})
		if marshalErr != nil {
			return report, marshalErr
		}
		_, runErr := m.diagnosticKubectl(diagnosticCtx, name,
			"run", podName, "-n", report.Namespace,
			"--image", options.Image,
			"--image-pull-policy", "IfNotPresent",
			"--restart", "Never",
			"--labels", "apc.dev/diagnostic="+suffix+",apc.dev/probe="+podName,
			"--overrides", string(overrides),
		)
		if runErr != nil {
			add("probe-pod/"+node.Metadata.Name, DiagnosticFail, conciseError(runErr), "inspect image availability and node admission")
			continue
		}
		waitTimeout := options.Timeout / 2
		if waitTimeout > time.Minute {
			waitTimeout = time.Minute
		}
		_, waitErr := m.diagnosticKubectl(diagnosticCtx, name,
			"wait", "-n", report.Namespace, "--for=condition=Ready", "pod/"+podName,
			"--timeout", waitTimeout.String(),
		)
		if waitErr != nil {
			add("probe-pod/"+node.Metadata.Name, DiagnosticFail, conciseError(waitErr), "inspect the probe Pod events")
			continue
		}
		ipOutput, ipErr := m.diagnosticKubectl(diagnosticCtx, name,
			"get", "pod", podName, "-n", report.Namespace, "-o", "jsonpath={.status.podIP}",
		)
		podIP := strings.TrimSpace(string(ipOutput))
		if ipErr != nil || net.ParseIP(podIP) == nil {
			add("probe-pod/"+node.Metadata.Name, DiagnosticFail, "Pod has no valid IP", "inspect CNI initialization")
			continue
		}
		add("probe-pod/"+node.Metadata.Name, DiagnosticPass, podName+" Ready at "+podIP, "")
		pods = append(pods, diagnosticPod{Name: podName, Node: node.Metadata.Name, IP: podIP})
	}
	if len(pods) == 0 {
		return report, nil
	}

	for _, pod := range pods {
		if probeErr := m.podProbe(diagnosticCtx, name, options.ProbeTimeout, report.Namespace, pod.Name, "true"); probeErr != nil {
			add("kubelet-exec/"+pod.Node, DiagnosticFail, conciseError(probeErr), "verify TCP 10250 and K3s remotedialer connectivity")
		} else {
			add("kubelet-exec/"+pod.Node, DiagnosticPass, "exec succeeded", "")
		}
		if probeErr := m.podProbe(diagnosticCtx, name, options.ProbeTimeout, report.Namespace, pod.Name,
			"nslookup", "kubernetes.default.svc.cluster.local"); probeErr != nil {
			add("dns/"+pod.Node, DiagnosticFail, conciseError(probeErr), "verify CoreDNS and Pod-to-Service routing")
		} else {
			add("dns/"+pod.Node, DiagnosticPass, "kubernetes.default.svc.cluster.local resolved", "")
		}
		if options.SkipEgress {
			add("egress/"+pod.Node, DiagnosticWarn, "skipped by request", "")
		} else if probeErr := m.podProbe(diagnosticCtx, name, options.ProbeTimeout, report.Namespace, pod.Name,
			"wget", "-q", "-T", strconv.Itoa(max(1, int(options.ProbeTimeout.Seconds())-1)), "-O", "/dev/null", "https://example.com/"); probeErr != nil {
			add("egress/"+pod.Node, DiagnosticFail, conciseError(probeErr), "verify Apple VM NAT, DNS and host routing; host tunnel default routes may block forwarded VM traffic")
		} else {
			add("egress/"+pod.Node, DiagnosticPass, "HTTPS egress succeeded", "")
		}
	}

	if len(pods) < 2 {
		add("cross-node-vxlan", DiagnosticWarn, "only one Ready probe Pod", "join another node for the VXLAN gate")
	} else {
		for _, source := range pods {
			for _, destination := range pods {
				if source.Node == destination.Node {
					continue
				}
				check := "pod-network/" + source.Node + "->" + destination.Node
				probeErr := m.podProbe(diagnosticCtx, name, options.ProbeTimeout, report.Namespace, source.Name,
					"wget", "-q", "-T", strconv.Itoa(max(1, int(options.ProbeTimeout.Seconds())-1)), "-O", "/dev/null", "http://"+destination.IP+"/")
				if probeErr != nil {
					add(check, DiagnosticFail, conciseError(probeErr), "verify bidirectional UDP 8472, VM-to-LAN routing and Flannel annotations")
				} else {
					add(check, DiagnosticPass, "HTTP reached "+destination.IP, "")
				}
			}
		}
	}

	resources = append(resources, "service/"+resourcePrefix)
	if _, exposeErr := m.diagnosticKubectl(diagnosticCtx, name, "expose", "pod", pods[0].Name, "-n", report.Namespace, "--name", resourcePrefix, "--port", "80", "--target-port", "80"); exposeErr != nil {
		add("clusterip", DiagnosticFail, conciseError(exposeErr), "inspect Service admission")
		return report, nil
	}
	serviceAddress := "http://" + resourcePrefix + "." + report.Namespace + ".svc.cluster.local/"
	for _, pod := range pods {
		check := "clusterip/" + pod.Node
		probeErr := m.podProbe(diagnosticCtx, name, options.ProbeTimeout, report.Namespace, pod.Name,
			"wget", "-q", "-T", strconv.Itoa(max(1, int(options.ProbeTimeout.Seconds())-1)), "-O", "/dev/null", serviceAddress)
		if probeErr != nil {
			add(check, DiagnosticFail, conciseError(probeErr), "verify CoreDNS, kube-proxy and cross-node Pod routing")
		} else {
			add(check, DiagnosticPass, "Service DNS and ClusterIP request succeeded", "")
		}
	}
	return report, nil
}

func (m *Manager) diagnosticKubectl(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	if _, haErr := loadHAConfig(name); haErr == nil {
		stdout, stderr, err := m.Kubectl(ctx, name, arguments...)
		if err != nil {
			return stdout, commandError("kubectl "+strings.Join(arguments, " "), stderr, err)
		}
		return stdout, nil
	} else if !errors.Is(haErr, os.ErrNotExist) {
		return nil, haErr
	}
	commandArguments := append([]string{"exec", ContainerName(name), "kubectl"}, arguments...)
	stdout, stderr, err := m.runner.Run(ctx, m.binary, commandArguments...)
	if err != nil {
		return stdout, commandError("kubectl "+strings.Join(arguments, " "), stderr, err)
	}
	return stdout, nil
}

func (m *Manager) podProbe(ctx context.Context, name string, timeout time.Duration, namespace, pod string, command ...string) error {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	arguments := []string{"--request-timeout=" + timeout.String(), "exec", "-n", namespace, pod, "--"}
	arguments = append(arguments, command...)
	_, err := m.diagnosticKubectl(probeCtx, name, arguments...)
	return err
}

func nodeReady(node diagnosticNode) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status == "True"
		}
	}
	return false
}

func nodeAddressSummary(node diagnosticNode) string {
	addresses := make([]string, 0, len(node.Status.Addresses))
	for _, address := range node.Status.Addresses {
		if address.Type == "InternalIP" || address.Type == "ExternalIP" {
			addresses = append(addresses, address.Type+"="+address.Address)
		}
	}
	if len(addresses) == 0 {
		return "no node IPs"
	}
	return strings.Join(addresses, ", ")
}

func diagnosticSuffix() (string, error) {
	buffer := make([]byte, 4)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate diagnostic namespace: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func conciseError(err error) string {
	if err == nil {
		return ""
	}
	value := strings.Join(strings.Fields(tail(err.Error(), 3)), " ")
	const maximumLength = 280
	if len(value) > maximumLength {
		value = value[:maximumLength-3] + "..."
	}
	return value
}
