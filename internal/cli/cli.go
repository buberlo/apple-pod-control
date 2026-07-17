package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/buberlo/apple-pod-control/internal/client"
	"github.com/buberlo/apple-pod-control/internal/model"
)

const Version = "v0.1.0"

type options struct {
	server             string
	namespace          string
	token              string
	caFile             string
	insecureSkipVerify bool
	requestTimeout     time.Duration
	out                io.Writer
	errOut             io.Writer
}

func NewCommand(out, errOut io.Writer) *cobra.Command {
	options := &options{namespace: model.DefaultNamespace, out: out, errOut: errOut, requestTimeout: 30 * time.Second}
	command := &cobra.Command{
		Use:           "apc",
		Short:         "Control apple/container workloads across Apple Silicon Macs",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	defaultServer := os.Getenv("APC_SERVER")
	if defaultServer == "" {
		defaultServer = "http://127.0.0.1:8080"
	}
	command.PersistentFlags().StringVar(&options.server, "server", defaultServer, "control plane API URL")
	command.PersistentFlags().StringVarP(&options.namespace, "namespace", "n", model.DefaultNamespace, "namespace scope")
	command.PersistentFlags().StringVar(&options.token, "token", os.Getenv("APC_TOKEN"), "bearer token (or APC_TOKEN)")
	command.PersistentFlags().StringVar(&options.caFile, "certificate-authority", "", "CA certificate for the API server")
	command.PersistentFlags().BoolVar(&options.insecureSkipVerify, "insecure-skip-tls-verify", false, "skip TLS certificate validation")
	command.PersistentFlags().DurationVar(&options.requestTimeout, "request-timeout", 30*time.Second, "request timeout")
	command.AddCommand(
		options.applyCommand(), options.getCommand(), options.describeCommand(), options.deleteCommand(),
		options.rolloutCommand(), options.scaleCommand(), options.versionCommand(),
	)
	return command
}

func (o *options) apiClient() (*client.Client, error) {
	return client.New(client.Config{Server: o.server, Token: o.token, CAFile: o.caFile, InsecureSkipVerify: o.insecureSkipVerify, Timeout: o.requestTimeout})
}

func (o *options) applyCommand() *cobra.Command {
	var filename string
	command := &cobra.Command{
		Use:   "apply -f FILENAME",
		Short: "Apply a declarative deployment specification",
		RunE: func(command *cobra.Command, _ []string) error {
			if filename == "" {
				return fmt.Errorf("-f is required")
			}
			deployments, err := readDeployments(filename)
			if err != nil {
				return err
			}
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			for _, deployment := range deployments {
				namespace := deployment.Metadata.Namespace
				if namespace == "" {
					namespace = o.namespace
				}
				stored, err := apiClient.Apply(command.Context(), namespace, deployment)
				if err != nil {
					return err
				}
				fmt.Fprintf(o.out, "deployment.apps/%s applied (generation %d)\n", stored.Metadata.Name, stored.Metadata.Generation)
			}
			return nil
		},
	}
	command.Flags().StringVarP(&filename, "filename", "f", "", "file, directory, or - for stdin")
	return command
}

func (o *options) getCommand() *cobra.Command {
	var outputFormat string
	command := &cobra.Command{
		Use:   "get RESOURCE [NAME]",
		Short: "Display deployments, pods, or nodes",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(command *cobra.Command, args []string) error {
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			resource, embeddedName := parseResource(args[0])
			name := embeddedName
			if len(args) == 2 {
				name = args[1]
			}
			switch normalizeResource(resource) {
			case "deployments":
				if name != "" {
					deployment, err := apiClient.GetDeployment(command.Context(), o.namespace, name)
					if err != nil {
						return err
					}
					return printObject(o.out, deployment, outputFormat)
				}
				deployments, err := apiClient.ListDeployments(command.Context(), o.namespace)
				if err != nil {
					return err
				}
				return printDeployments(o.out, deployments, outputFormat)
			case "pods":
				pods, err := apiClient.ListPods(command.Context(), o.namespace)
				if err != nil {
					return err
				}
				if name != "" {
					for _, pod := range pods {
						if pod.ContainerName == name || pod.ID == name {
							return printObject(o.out, pod, outputFormat)
						}
					}
					return fmt.Errorf("pods %q not found", name)
				}
				return printPods(o.out, pods, outputFormat)
			case "nodes":
				nodes, err := apiClient.ListNodes(command.Context())
				if err != nil {
					return err
				}
				if name != "" {
					for _, node := range nodes {
						if node.ID == name || node.Hostname == name {
							return printObject(o.out, node, outputFormat)
						}
					}
					return fmt.Errorf("nodes %q not found", name)
				}
				return printNodes(o.out, nodes, outputFormat)
			default:
				return fmt.Errorf("unsupported resource %q; use deployments, pods, or nodes", resource)
			}
		},
	}
	command.Flags().StringVarP(&outputFormat, "output", "o", "", "output format: json, yaml, or wide")
	return command
}

func (o *options) describeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "describe deployment NAME",
		Short: "Show details and current rollout state",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			if normalizeResource(args[0]) != "deployments" {
				return fmt.Errorf("describe currently supports deployments")
			}
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			deployment, err := apiClient.GetDeployment(command.Context(), o.namespace, args[1])
			if err != nil {
				return err
			}
			pods, err := apiClient.ListPods(command.Context(), o.namespace)
			if err != nil {
				return err
			}
			return describeDeployment(o.out, deployment, pods)
		},
	}
}

func (o *options) deleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete deployment NAME",
		Short: "Delete a deployment and terminate its pods",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			if normalizeResource(args[0]) != "deployments" {
				return fmt.Errorf("delete currently supports deployments")
			}
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			if err := apiClient.DeleteDeployment(command.Context(), o.namespace, args[1]); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "deployment.apps/%s deleted\n", args[1])
			return nil
		},
	}
}

func (o *options) rolloutCommand() *cobra.Command {
	rollout := &cobra.Command{Use: "rollout", Short: "Manage deployment rollouts"}
	var timeout time.Duration
	statusCommand := &cobra.Command{
		Use:   "status deployment/NAME",
		Short: "Wait for a deployment rollout to complete",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			resource, name := parseResource(args[0])
			if normalizeResource(resource) != "deployments" || name == "" {
				return fmt.Errorf("expected deployment/NAME")
			}
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(command.Context(), timeout)
			defer cancel()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				deployment, err := apiClient.GetDeployment(ctx, o.namespace, name)
				if err != nil {
					return err
				}
				if deployment.Status.ObservedGeneration == deployment.Metadata.Generation &&
					deployment.Status.UpdatedReplicas == deployment.Spec.Replicas &&
					deployment.Status.AvailableReplicas >= deployment.Spec.Replicas {
					fmt.Fprintf(o.out, "deployment %q successfully rolled out\n", name)
					return nil
				}
				fmt.Fprintf(o.out, "Waiting for deployment %q rollout: %d of %d updated replicas are available...\n", name, deployment.Status.AvailableReplicas, deployment.Spec.Replicas)
				select {
				case <-ctx.Done():
					return fmt.Errorf("rollout status timeout: %w", ctx.Err())
				case <-ticker.C:
				}
			}
		},
	}
	statusCommand.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "time to wait for rollout")
	rollout.AddCommand(statusCommand)
	return rollout
}

func (o *options) scaleCommand() *cobra.Command {
	var replicas int
	command := &cobra.Command{
		Use:   "scale deployment NAME --replicas=N",
		Short: "Set the desired replica count",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			if normalizeResource(args[0]) != "deployments" || replicas < 0 {
				return fmt.Errorf("a deployment and non-negative --replicas value are required")
			}
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			deployment, err := apiClient.GetDeployment(command.Context(), o.namespace, args[1])
			if err != nil {
				return err
			}
			deployment.Spec.Replicas = replicas
			stored, err := apiClient.Apply(command.Context(), o.namespace, deployment)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "deployment.apps/%s scaled to %d (generation %d)\n", stored.Metadata.Name, replicas, stored.Metadata.Generation)
			return nil
		},
	}
	command.Flags().IntVar(&replicas, "replicas", -1, "new desired replica count")
	return command
}

func (o *options) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print client and server versions",
		RunE: func(command *cobra.Command, _ []string) error {
			fmt.Fprintf(o.out, "Client Version: %s\n", Version)
			apiClient, err := o.apiClient()
			if err != nil {
				return err
			}
			serverVersion, err := apiClient.Version(command.Context())
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "Server Version: %v\n", serverVersion["gitVersion"])
			return nil
		},
	}
}

func readDeployments(path string) ([]model.Deployment, error) {
	var files []string
	switch {
	case path == "-":
		return decodeDeployments(os.Stdin, "stdin")
	default:
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w", err)
		}
		if !info.IsDir() {
			files = []string{path}
		} else {
			err := filepath.WalkDir(path, func(current string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".yaml") || strings.HasSuffix(entry.Name(), ".yml")) {
					files = append(files, current)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			sort.Strings(files)
		}
	}
	var deployments []model.Deployment
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		decoded, decodeErr := decodeDeployments(file, filename)
		_ = file.Close()
		if decodeErr != nil {
			return nil, decodeErr
		}
		deployments = append(deployments, decoded...)
	}
	if len(deployments) == 0 {
		return nil, fmt.Errorf("no Deployment manifests found")
	}
	return deployments, nil
}

func decodeDeployments(reader io.Reader, source string) ([]model.Deployment, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	var deployments []model.Deployment
	for document := 1; ; document++ {
		var deployment model.Deployment
		err := decoder.Decode(&deployment)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode %s document %d: %w", source, document, err)
		}
		if deployment.Kind == "" && deployment.Metadata.Name == "" {
			continue
		}
		deployments = append(deployments, deployment)
	}
	return deployments, nil
}

func printObject(writer io.Writer, value any, format string) error {
	switch format {
	case "", "yaml":
		data, err := yaml.Marshal(value)
		if err != nil {
			return err
		}
		_, err = writer.Write(data)
		return err
	case "json":
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	default:
		return fmt.Errorf("unsupported output format %q", format)
	}
}

func printDeployments(writer io.Writer, deployments []model.Deployment, format string) error {
	if format == "json" || format == "yaml" {
		return printObject(writer, map[string]any{"apiVersion": model.APIVersion, "kind": "DeploymentList", "items": deployments}, format)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NAME\tREADY\tUP-TO-DATE\tAVAILABLE\tAGE")
	for _, deployment := range deployments {
		fmt.Fprintf(table, "%s\t%d/%d\t%d\t%d\t%s\n", deployment.Metadata.Name, deployment.Status.ReadyReplicas, deployment.Spec.Replicas, deployment.Status.UpdatedReplicas, deployment.Status.AvailableReplicas, age(deployment.Metadata.CreatedAt))
	}
	return table.Flush()
}

func printPods(writer io.Writer, pods []model.Workload, format string) error {
	if format == "json" || format == "yaml" {
		return printObject(writer, map[string]any{"apiVersion": "v1", "kind": "PodList", "items": pods}, format)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	wide := format == "wide"
	if wide {
		fmt.Fprintln(table, "NAME\tREADY\tSTATUS\tRESTARTS\tAGE\tIP\tNODE")
	} else {
		fmt.Fprintln(table, "NAME\tREADY\tSTATUS\tRESTARTS\tAGE")
	}
	for _, pod := range pods {
		ready := "0/1"
		if pod.Ready {
			ready = "1/1"
		}
		if wide {
			fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", pod.ContainerName, ready, pod.State, pod.RestartCount, age(pod.CreatedAt), valueOr(pod.Address, "<none>"), valueOr(pod.NodeID, "<none>"))
		} else {
			fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\n", pod.ContainerName, ready, pod.State, pod.RestartCount, age(pod.CreatedAt))
		}
	}
	return table.Flush()
}

func printNodes(writer io.Writer, nodes []model.Node, format string) error {
	if format == "json" || format == "yaml" {
		return printObject(writer, map[string]any{"apiVersion": "v1", "kind": "NodeList", "items": nodes}, format)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NAME\tSTATUS\tCPUS\tMEMORY\tADDRESS\tARCH\tRUNTIME")
	for _, node := range nodes {
		fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n", node.ID, node.State, node.CPUCount, byteSize(node.MemoryBytes), valueOr(node.Address, "<none>"), node.Architecture, node.RuntimeVersion)
	}
	return table.Flush()
}

func describeDeployment(writer io.Writer, deployment model.Deployment, pods []model.Workload) error {
	container := deployment.Container()
	fmt.Fprintf(writer, "Name:\t%s\nNamespace:\t%s\nGeneration:\t%d\nStrategy:\t%s\nReplicas:\t%d desired | %d updated | %d ready | %d available\nImage:\t%s\n",
		deployment.Metadata.Name, deployment.Metadata.Namespace, deployment.Metadata.Generation, deployment.Spec.Strategy.Type,
		deployment.Spec.Replicas, deployment.Status.UpdatedReplicas, deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas, container.Image)
	if len(deployment.Spec.Template.Spec.NodeSelector) > 0 {
		fmt.Fprintf(writer, "Node-Selector:\t%s\n", formatLabels(deployment.Spec.Template.Spec.NodeSelector))
	}
	fmt.Fprintln(writer, "Conditions:")
	for _, condition := range deployment.Status.Conditions {
		fmt.Fprintf(writer, "  %s\t%s\t%s\n", condition.Type, condition.Status, condition.Reason)
	}
	fmt.Fprintln(writer, "Pods:")
	for _, pod := range pods {
		if pod.Deployment == deployment.Metadata.Name {
			fmt.Fprintf(writer, "  %s\t%s\tready=%t\tnode=%s\t%s\n", pod.ContainerName, pod.State, pod.Ready, valueOr(pod.NodeID, "<none>"), pod.Message)
		}
	}
	return nil
}

func parseResource(value string) (string, string) {
	parts := strings.SplitN(value, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return value, ""
}

func normalizeResource(value string) string {
	switch strings.ToLower(value) {
	case "deployment", "deploy", "deployments":
		return "deployments"
	case "pod", "po", "pods":
		return "pods"
	case "node", "no", "nodes":
		return "nodes"
	default:
		return strings.ToLower(value)
	}
}

func age(value time.Time) string {
	if value.IsZero() {
		return "<unknown>"
	}
	duration := time.Since(value)
	switch {
	case duration < time.Minute:
		return fmt.Sprintf("%ds", int(duration.Seconds()))
	case duration < time.Hour:
		return fmt.Sprintf("%dm", int(duration.Minutes()))
	case duration < 24*time.Hour:
		return fmt.Sprintf("%dh", int(duration.Hours()))
	default:
		return fmt.Sprintf("%dd", int(duration.Hours()/24))
	}
}

func byteSize(value int64) string {
	if value >= 1<<30 {
		return fmt.Sprintf("%.1fGi", float64(value)/float64(1<<30))
	}
	return fmt.Sprintf("%.0fMi", float64(value)/float64(1<<20))
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func formatLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+labels[key])
	}
	return strings.Join(values, ",")
}
