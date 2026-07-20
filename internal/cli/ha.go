package cli

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

const haMemberCount = 3

type haManager interface {
	CreateHA(context.Context, cluster.HAConfig) (cluster.HAState, error)
	StatusHA(context.Context, string) (cluster.HAState, error)
	StartHA(context.Context, string, time.Duration) (cluster.HAState, error)
	StopHA(context.Context, string) error
	DeleteHA(context.Context, string, bool) error
	SnapshotHA(context.Context, string, string) (cluster.HASnapshotResult, error)
	RestoreHA(context.Context, string, string, time.Duration) (cluster.HAState, error)
	RecoverHA(context.Context, string, string, string, time.Duration) (cluster.HAState, error)
	StopHAMember(context.Context, string, int, time.Duration) (cluster.HAState, error)
	StartHAMember(context.Context, string, int, time.Duration) (cluster.HAState, error)
	RestartHAMember(context.Context, string, int, time.Duration) (cluster.HAState, error)
	ServeHAProxy(context.Context, string) error
}

var newHAManager = func() haManager {
	return cluster.NewManager("container")
}

type haCreateOptions struct {
	networkName    string
	subnet         string
	stableIP       string
	apiPortBase    int
	image          string
	cpus           int
	memory         string
	volumeSize     string
	listen         string
	kubeconfig     string
	wait           time.Duration
	disableIngress bool
}

func defaultHACreateOptions() haCreateOptions {
	return haCreateOptions{
		subnet:         "192.168.96.0/24",
		stableIP:       "192.168.96.241",
		apiPortBase:    17443,
		image:          cluster.DefaultK3sImage,
		cpus:           2,
		memory:         "2G",
		volumeSize:     "8G",
		listen:         "127.0.0.1",
		wait:           3 * time.Minute,
		disableIngress: true,
	}
}

func (o *options) clusterHACommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "ha",
		Short: "Manage a three-server K3s cluster with embedded-etcd quorum",
	}
	command.AddCommand(
		o.clusterHACreateCommand(),
		o.clusterHAStatusCommand(),
		o.clusterHAStartCommand(),
		o.clusterHAStopCommand(),
		o.clusterHADeleteCommand(),
		o.clusterHASnapshotCommand(),
		o.clusterHARestoreCommand(),
		o.clusterHARecoverCommand(),
		o.clusterHAMemberCommand(),
		o.clusterHAProxyCommand(),
	)
	return command
}

func (o *options) clusterHACreateCommand() *cobra.Command {
	config := defaultHACreateOptions()
	command := &cobra.Command{
		Use:   "create NAME",
		Short: "Create or reconcile a local three-server ARM64 K3s cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			name := args[0]
			desired, err := haConfigForCreate(name, config)
			if err != nil {
				return err
			}
			state, err := newHAManager().CreateHA(command.Context(), desired)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s ready (%d/%d servers)\n", state.Name, state.ReadyMembers, haMemberCount)
			fmt.Fprintf(o.out, "kubeconfig: %s\n", state.Kubeconfig)
			return nil
		},
	}
	command.Flags().StringVar(&config.networkName, "network", "", "apple/container network; defaults to an APC-owned network for the cluster")
	command.Flags().StringVar(&config.subnet, "subnet", config.subnet, "private IPv4 subnet shared by the server VMs")
	command.Flags().StringVar(&config.stableIP, "stable-ip-start", config.stableIP, "first of three consecutive stable server IPv4 addresses")
	command.Flags().IntVar(&config.apiPortBase, "api-port-base", config.apiPortBase, "first of three consecutive host Kubernetes API ports")
	command.Flags().StringVar(&config.image, "image", config.image, "pinned ARM64 K3s OCI image")
	command.Flags().IntVar(&config.cpus, "cpus", config.cpus, "virtual CPUs allocated to each server")
	command.Flags().StringVar(&config.memory, "memory", config.memory, "memory allocated to each server")
	command.Flags().StringVar(&config.volumeSize, "volume-size", config.volumeSize, "persistent disk size allocated to each server")
	command.Flags().StringVar(&config.listen, "listen-address", config.listen, "host address used for Kubernetes API publishing")
	command.Flags().StringVar(&config.kubeconfig, "kubeconfig", "", "kubeconfig destination")
	command.Flags().DurationVar(&config.wait, "wait", config.wait, "maximum time to wait for all three Ready servers")
	command.Flags().BoolVar(&config.disableIngress, "disable-ingress", config.disableIngress, "disable bundled Traefik and ServiceLB")
	return command
}

func (o *options) clusterHAStatusCommand() *cobra.Command {
	var outputFormat string
	command := &cobra.Command{
		Use:   "status [NAME]",
		Short: "Show every embedded-etcd server and Kubernetes node",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			state, err := newHAManager().StatusHA(command.Context(), o.clusterName(args))
			if err != nil {
				return err
			}
			return printHAStatus(o.out, state, outputFormat)
		},
	}
	command.Flags().StringVarP(&outputFormat, "output", "o", "wide", "output format: wide, json, or yaml")
	return command
}

func (o *options) clusterHAStartCommand() *cobra.Command {
	var timeout time.Duration
	command := &cobra.Command{
		Use:   "start [NAME]",
		Short: "Start a stopped three-server K3s cluster",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			state, err := newHAManager().StartHA(command.Context(), o.clusterName(args), timeout)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s ready (%d/%d servers)\n", state.Name, state.ReadyMembers, haMemberCount)
			return nil
		},
	}
	command.Flags().DurationVar(&timeout, "wait", 3*time.Minute, "maximum time to wait for all three Ready servers")
	return command
}

func (o *options) clusterHAStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop [NAME]",
		Short: "Stop all three K3s servers without deleting their state",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			name := o.clusterName(args)
			if err := newHAManager().StopHA(command.Context(), name); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s stopped\n", name)
			return nil
		},
	}
}

func (o *options) clusterHADeleteCommand() *cobra.Command {
	var confirmed, keepData bool
	command := &cobra.Command{
		Use:   "delete [NAME]",
		Short: "Delete all HA server VMs and, by default, their data",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing destructive HA cluster deletion without --yes")
			}
			name := o.clusterName(args)
			if err := newHAManager().DeleteHA(command.Context(), name, keepData); err != nil {
				return err
			}
			if keepData {
				fmt.Fprintf(o.out, "cluster.apc.dev/%s VMs removed; data retained\n", name)
			} else {
				fmt.Fprintf(o.out, "cluster.apc.dev/%s deleted\n", name)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm deletion without an interactive prompt")
	command.Flags().BoolVar(&keepData, "keep-data", false, "retain the server data volumes and saved HA configuration")
	return command
}

func haConfigForCreate(name string, options haCreateOptions) (cluster.HAConfig, error) {
	config, err := cluster.DefaultHAConfig(name)
	if err != nil {
		return cluster.HAConfig{}, err
	}
	config.Subnet = options.subnet
	config.Image = options.image
	config.ListenAddress = options.listen
	config.CPUs = options.cpus
	config.Memory = options.memory
	config.VolumeSize = options.volumeSize
	config.StartupTimeout = options.wait
	config.DisableTraefik = options.disableIngress
	if options.networkName != "" {
		config.NetworkName = options.networkName
	}
	if options.kubeconfig != "" {
		config.KubeconfigPath = options.kubeconfig
	}
	config.Members, err = buildHAMembers(name, config.Members, options.subnet, options.stableIP, options.apiPortBase)
	if err != nil {
		return cluster.HAConfig{}, err
	}
	return config, nil
}

func buildHAMembers(name string, defaults []cluster.HAMember, subnet, stableIPStart string, apiPortBase int) ([]cluster.HAMember, error) {
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil || !prefix.Addr().Is4() {
		return nil, fmt.Errorf("HA subnet must be a valid IPv4 prefix")
	}
	prefix = prefix.Masked()
	address, err := netip.ParseAddr(stableIPStart)
	if err != nil || !address.Is4() {
		return nil, fmt.Errorf("HA stable IP start must be a valid IPv4 address")
	}
	if apiPortBase < 1 || apiPortBase > 65535-(haMemberCount-1) {
		return nil, fmt.Errorf("HA API port base must leave room for three consecutive TCP ports")
	}

	members := make([]cluster.HAMember, haMemberCount)
	for index := range members {
		if index < len(defaults) {
			members[index] = defaults[index]
		}
		memberID := index + 1
		if !address.IsValid() || !prefix.Contains(address) || address == prefix.Addr() || !prefix.Contains(address.Next()) || address.IsMulticast() || address.IsUnspecified() {
			return nil, fmt.Errorf("HA server address %q is not usable in subnet %q", address, prefix)
		}
		members[index].ID = memberID
		if members[index].NodeName == "" {
			members[index].NodeName = fmt.Sprintf("apc-%s-%d", name, memberID)
		}
		members[index].StableIP = address.String()
		members[index].MAC = fmt.Sprintf("02:ac:96:00:00:%02x", memberID)
		members[index].HostAPIPort = apiPortBase + index
		address = address.Next()
	}
	return members, nil
}

func printHAStatus(writer io.Writer, state cluster.HAState, format string) error {
	switch format {
	case "json", "yaml":
		return printObject(writer, state, format)
	case "", "wide":
	default:
		return fmt.Errorf("unsupported output format %q; use wide, json, or yaml", format)
	}

	members := append([]cluster.HAMemberState(nil), state.Members...)
	sort.SliceStable(members, func(left, right int) bool {
		if members[left].ID == members[right].ID {
			return members[left].NodeName < members[right].NodeName
		}
		return members[left].ID < members[right].ID
	})
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "CLUSTER\tMEMBER\tRUNTIME\tNODE\tNODE-READY\tAPI-READY\tINTERNAL-IP\tAPI")
	for _, member := range members {
		fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%t\t%t\t%s\t%s\n", state.Name, member.ID, member.RuntimeState, member.NodeName, member.NodeReady, member.APIReady, member.StableIP, member.APIEndpoint)
	}
	return table.Flush()
}
