package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/buberlo/apple-pod-control/internal/buildinfo"
	"github.com/buberlo/apple-pod-control/internal/cluster"
	"github.com/buberlo/apple-pod-control/internal/doctor"
	"github.com/buberlo/apple-pod-control/internal/firewall"
	"github.com/buberlo/apple-pod-control/internal/images"
	"github.com/buberlo/apple-pod-control/internal/launchd"
	"github.com/buberlo/apple-pod-control/internal/overlay"
)

var Version = buildinfo.Current()

type options struct {
	cluster string
	out     io.Writer
	errOut  io.Writer
}

func NewCommand(out, errOut io.Writer) *cobra.Command {
	options := &options{out: out, errOut: errOut}
	command := &cobra.Command{
		Use:           "apc",
		Short:         "Manage K3s clusters on Apple Silicon with apple/container",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	command.PersistentFlags().StringVar(&options.cluster, "cluster", "", "APC K3s cluster (or APC_CLUSTER)")
	command.AddCommand(
		options.versionCommand(), options.doctorCommand(),
		options.clusterCommand(), options.nodeCommand(), options.imageCommand(), options.systemCommand(), options.configCommand(), options.kubeconfigCommand(), options.kubectlCommand(), options.helmCommand(),
	)
	return command
}

func (o *options) systemCommand() *cobra.Command {
	command := &cobra.Command{Use: "system", Short: "Manage APC node supervision on macOS"}
	command.AddCommand(o.systemInstallCommand(), o.systemUninstallCommand(), o.systemStatusCommand(), o.systemSuperviseCommand(), o.systemFirewallCommand(), o.systemOverlayCommand())
	return command
}

func (o *options) systemOverlayCommand() *cobra.Command {
	command := &cobra.Command{Use: "overlay", Short: "Validate an authenticated host overlay for APC traffic"}
	command.AddCommand(o.systemOverlayCheckCommand())
	return command
}

func (o *options) systemOverlayCheckCommand() *cobra.Command {
	config := overlay.Config{Provider: "tailscale", Interface: "auto"}
	var outputFormat string
	command := &cobra.Command{
		Use:   "check",
		Short: "Verify local Tailscale identity, online peer and exact host route",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			status, err := overlay.NewChecker().Check(command.Context(), config)
			if err != nil {
				return err
			}
			switch outputFormat {
			case "", "wide":
				writer := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
				fmt.Fprintln(writer, "PROVIDER\tBACKEND\tINTERFACE\tLOCAL-IP\tPEER-IP\tPEER-ONLINE")
				fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%t\n", status.Provider, status.BackendState, status.Interface, status.LocalIP, status.PeerIP, status.PeerOnline)
				return writer.Flush()
			case "json", "yaml":
				return printObject(o.out, status, outputFormat)
			default:
				return fmt.Errorf("unsupported output format %q; use wide, json, or yaml", outputFormat)
			}
		},
	}
	command.Flags().StringVar(&config.Provider, "provider", "tailscale", "authenticated overlay provider")
	command.Flags().StringVar(&config.Interface, "interface", "auto", "expected interface, or auto to resolve it from the local overlay IP")
	command.Flags().StringVar(&config.LocalIP, "local-ip", "", "expected local Tailscale IPv4; auto-detected when omitted")
	command.Flags().StringVar(&config.PeerIP, "peer-ip", "", "online peer Tailscale IPv4")
	command.Flags().StringVarP(&outputFormat, "output", "o", "wide", "output format: wide, json, or yaml")
	_ = command.MarkFlagRequired("peer-ip")
	return command
}

func (o *options) systemFirewallCommand() *cobra.Command {
	command := &cobra.Command{Use: "firewall", Short: "Render or load peer-restricted macOS PF rules"}
	command.AddCommand(o.systemFirewallRenderCommand(), o.systemFirewallApplyCommand(), o.systemFirewallRemoveCommand(), o.systemFirewallInstallCommand(), o.systemFirewallUninstallCommand(), o.systemFirewallStatusCommand())
	return command
}

func (o *options) systemFirewallStatusCommand() *cobra.Command {
	var outputFormat string
	command := &cobra.Command{
		Use:   "status",
		Short: "Verify APC's root-owned PF installation and loaded anchor",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			status, err := firewall.Verify(command.Context(), o.clusterName(nil))
			if err != nil {
				return err
			}
			switch outputFormat {
			case "", "wide":
				writer := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
				fmt.Fprintln(writer, "CLUSTER\tDAEMON\tANCHOR\tRULES\tPF-REFERENCE")
				fmt.Fprintf(writer, "%s\t%s\t%s\t%d\t%t\n", status.Cluster, status.DaemonLabel, status.Anchor, status.RuleCount, status.ReferenceSet)
				return writer.Flush()
			case "json", "yaml":
				return printObject(o.out, status, outputFormat)
			default:
				return fmt.Errorf("unsupported output format %q; use wide, json, or yaml", outputFormat)
			}
		},
	}
	command.Flags().StringVarP(&outputFormat, "output", "o", "wide", "output format: wide, json, or yaml")
	return command
}

func (o *options) systemFirewallInstallCommand() *cobra.Command {
	config := firewall.Config{Role: "server", Interface: "en0", APIPort: cluster.DefaultAPIPort, VXLANPort: cluster.DefaultVXLANPort, KubeletPort: cluster.DefaultKubeletPort}
	var confirmed bool
	var executable string
	command := &cobra.Command{
		Use:   "install",
		Short: "Install and start a root-owned PF LaunchDaemon",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if !confirmed {
				return fmt.Errorf("refusing privileged firewall installation without --yes")
			}
			config.Cluster = o.clusterName(nil)
			if executable == "" {
				resolved, err := os.Executable()
				if err != nil {
					return fmt.Errorf("resolve APC executable: %w", err)
				}
				executable = resolved
			}
			path, err := firewall.Install(command.Context(), config, executable)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "firewall.apc.dev/%s installed at %s\n", config.Cluster, path)
			return nil
		},
	}
	bindFirewallFlags(command, &config)
	command.Flags().StringVar(&executable, "executable", "", "APC executable to copy into the privileged helper directory")
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm privileged helper and LaunchDaemon installation")
	return command
}

func (o *options) systemFirewallUninstallCommand() *cobra.Command {
	var confirmed bool
	command := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove APC's PF LaunchDaemon and release its PF reference",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if !confirmed {
				return fmt.Errorf("refusing privileged firewall removal without --yes")
			}
			name := o.clusterName(nil)
			if err := firewall.Uninstall(command.Context(), name); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "firewall.apc.dev/%s uninstalled\n", name)
			return nil
		},
	}
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm PF LaunchDaemon removal")
	return command
}

func (o *options) systemFirewallRenderCommand() *cobra.Command {
	config := firewall.Config{Role: "server", Interface: "en0", APIPort: cluster.DefaultAPIPort, VXLANPort: cluster.DefaultVXLANPort, KubeletPort: cluster.DefaultKubeletPort}
	command := &cobra.Command{
		Use:   "render",
		Short: "Print and validate PF rules without loading them",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config.Cluster = o.clusterName(nil)
			rules, err := firewall.Render(config)
			if err != nil {
				return err
			}
			if err := firewall.Validate(command.Context(), rules); err != nil {
				return err
			}
			_, err = o.out.Write(rules)
			return err
		},
	}
	bindFirewallFlags(command, &config)
	return command
}

func (o *options) systemFirewallApplyCommand() *cobra.Command {
	config := firewall.Config{Role: "server", Interface: "en0", APIPort: cluster.DefaultAPIPort, VXLANPort: cluster.DefaultVXLANPort, KubeletPort: cluster.DefaultKubeletPort}
	var confirmed bool
	command := &cobra.Command{
		Use:   "apply",
		Short: "Load peer-restricted rules into APC's macOS PF anchor",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if !confirmed {
				return fmt.Errorf("refusing host firewall reconfiguration without --yes")
			}
			config.Cluster = o.clusterName(nil)
			if err := firewall.Apply(command.Context(), config); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "firewall.apc.dev/%s loaded for %s peers\n", config.Cluster, config.Role)
			return nil
		},
	}
	bindFirewallFlags(command, &config)
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm PF anchor replacement")
	return command
}

func (o *options) systemFirewallRemoveCommand() *cobra.Command {
	var confirmed bool
	command := &cobra.Command{
		Use:   "remove",
		Short: "Flush APC's PF rules for one cluster",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if !confirmed {
				return fmt.Errorf("refusing host firewall removal without --yes")
			}
			name := o.clusterName(nil)
			if err := firewall.Remove(command.Context(), name); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "firewall.apc.dev/%s removed\n", name)
			return nil
		},
	}
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm PF anchor removal")
	return command
}

func bindFirewallFlags(command *cobra.Command, config *firewall.Config) {
	command.Flags().StringVar(&config.Role, "role", "server", "node role: server or agent")
	command.Flags().StringVar(&config.Interface, "interface", "en0", "trusted/encrypted host interface, or auto to resolve it from --local-ip")
	command.Flags().StringVar(&config.LocalIP, "local-ip", "", "this Mac's IPv4 address on the selected interface")
	command.Flags().StringSliceVar(&config.Peers, "peer", nil, "allowed peer IPv4 address (repeatable)")
	command.Flags().IntVar(&config.APIPort, "api-port", cluster.DefaultAPIPort, "published Kubernetes API port")
	command.Flags().IntVar(&config.VXLANPort, "vxlan-port", cluster.DefaultVXLANPort, "published Flannel VXLAN port")
	command.Flags().IntVar(&config.KubeletPort, "kubelet-port", cluster.DefaultKubeletPort, "published kubelet port")
}

func (o *options) systemInstallCommand() *cobra.Command {
	config := launchd.Config{Role: "server", Interval: 15 * time.Second}
	var unattended, confirmed bool
	var targetUser string
	command := &cobra.Command{
		Use:   "install",
		Short: "Install and start an APC launchd supervisor",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config.Cluster = o.clusterName(nil)
			if config.Executable == "" {
				executable, err := os.Executable()
				if err != nil {
					return fmt.Errorf("resolve APC executable: %w", err)
				}
				config.Executable = executable
			}
			manager, err := launchd.NewManager()
			if err != nil {
				return err
			}
			if unattended {
				if !confirmed {
					return fmt.Errorf("refusing privileged unattended service installation without --yes")
				}
				if targetUser == "" {
					return fmt.Errorf("--user is required with --unattended")
				}
				path, err := manager.InstallUnattended(command.Context(), config, targetUser)
				if err != nil {
					return err
				}
				fmt.Fprintf(o.out, "launchdaemon.apc.dev/%s-%s installed at %s\n", config.Role, config.Cluster, path)
				return nil
			}
			if targetUser != "" || confirmed {
				return fmt.Errorf("--user and --yes require --unattended")
			}
			path, err := manager.Install(command.Context(), config)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "launchagent.apc.dev/%s-%s installed at %s\n", config.Role, config.Cluster, path)
			return nil
		},
	}
	command.Flags().StringVar(&config.Role, "role", "server", "node role: server, agent, or ha")
	command.Flags().StringVar(&config.Executable, "executable", "", "stable APC executable path; defaults to the current binary")
	command.Flags().DurationVar(&config.Interval, "interval", 15*time.Second, "health reconciliation interval")
	command.Flags().BoolVar(&unattended, "unattended", false, "install a root-managed boot-persistent LaunchDaemon")
	command.Flags().StringVar(&targetUser, "user", "", "non-root account that runs the unattended supervisor")
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm privileged unattended service installation")
	return command
}

func (o *options) systemUninstallCommand() *cobra.Command {
	config := launchd.Config{Role: "server", Interval: 15 * time.Second}
	var unattended, confirmed bool
	var targetUser string
	command := &cobra.Command{
		Use:   "uninstall",
		Short: "Stop and remove an APC launchd supervisor",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config.Cluster = o.clusterName(nil)
			if unattended && !confirmed {
				return fmt.Errorf("refusing unattended LaunchDaemon removal without explicit confirmation")
			}
			if config.Executable == "" {
				executable, err := os.Executable()
				if err != nil {
					return fmt.Errorf("resolve APC executable: %w", err)
				}
				config.Executable = executable
			}
			manager, err := launchd.NewManager()
			if err != nil {
				return err
			}
			if unattended {
				if targetUser == "" {
					return fmt.Errorf("--user is required with --unattended")
				}
				if err := manager.UninstallUnattended(command.Context(), config, targetUser, confirmed); err != nil {
					return err
				}
				fmt.Fprintf(o.out, "launchdaemon.apc.dev/%s-%s removed\n", config.Role, config.Cluster)
				return nil
			}
			if targetUser != "" || confirmed {
				return fmt.Errorf("--user and --yes require --unattended")
			}
			if err := manager.Uninstall(command.Context(), config); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "launchagent.apc.dev/%s-%s removed\n", config.Role, config.Cluster)
			return nil
		},
	}
	command.Flags().StringVar(&config.Role, "role", "server", "node role: server, agent, or ha")
	command.Flags().StringVar(&config.Executable, "executable", "", "stable APC executable path; defaults to the current binary")
	command.Flags().DurationVar(&config.Interval, "interval", 15*time.Second, "health reconciliation interval")
	command.Flags().BoolVar(&unattended, "unattended", false, "remove a root-managed boot-persistent LaunchDaemon")
	command.Flags().StringVar(&targetUser, "user", "", "non-root account that runs the unattended supervisor")
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm privileged unattended service removal")
	return command
}

func (o *options) systemStatusCommand() *cobra.Command {
	config := launchd.Config{Role: "server", Interval: 15 * time.Second}
	var unattended bool
	var targetUser string
	command := &cobra.Command{
		Use:   "status",
		Short: "Show launchd state for an APC node supervisor",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config.Cluster = o.clusterName(nil)
			if config.Executable == "" {
				executable, err := os.Executable()
				if err != nil {
					return fmt.Errorf("resolve APC executable: %w", err)
				}
				config.Executable = executable
			}
			manager, err := launchd.NewManager()
			if err != nil {
				return err
			}
			if unattended && targetUser == "" {
				return fmt.Errorf("--user is required with --unattended")
			}
			if !unattended && targetUser != "" {
				return fmt.Errorf("--user requires --unattended")
			}
			var status []byte
			if unattended {
				status, err = manager.StatusUnattended(command.Context(), config, targetUser)
			} else {
				status, err = manager.Status(command.Context(), config)
			}
			if err != nil {
				return err
			}
			_, err = o.out.Write(status)
			return err
		},
	}
	command.Flags().StringVar(&config.Role, "role", "server", "node role: server, agent, or ha")
	command.Flags().StringVar(&config.Executable, "executable", "", "stable APC executable path; defaults to the current binary")
	command.Flags().DurationVar(&config.Interval, "interval", 15*time.Second, "health reconciliation interval")
	command.Flags().BoolVar(&unattended, "unattended", false, "inspect a root-managed boot-persistent LaunchDaemon")
	command.Flags().StringVar(&targetUser, "user", "", "non-root account that runs the unattended supervisor")
	return command
}

func (o *options) systemSuperviseCommand() *cobra.Command {
	config := cluster.SuperviseOptions{Role: "server", Interval: 15 * time.Second, Output: o.out}
	var logFile string
	command := &cobra.Command{
		Use:    "supervise",
		Short:  "Continuously reconcile one local APC node",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config.Name = o.clusterName(nil)
			if logFile == "" {
				return cluster.NewManager("container").Supervise(command.Context(), config)
			}
			// In unattended mode this process is already running as the target
			// account; only now may it open a path beneath that account's HOME.
			log, err := cluster.OpenSupervisorLog(logFile, config.Role, config.Name)
			if err != nil {
				return err
			}
			config.Output = log
			superviseErr := cluster.NewManager("container").Supervise(command.Context(), config)
			return errors.Join(superviseErr, log.Close())
		},
	}
	command.Flags().StringVar(&config.Role, "role", "server", "node role: server, agent, or ha")
	command.Flags().DurationVar(&config.Interval, "interval", 15*time.Second, "health reconciliation interval")
	command.Flags().StringVar(&logFile, "log-file", "", "exact bounded unattended log path")
	return command
}

func (o *options) imageCommand() *cobra.Command {
	command := &cobra.Command{Use: "image", Short: "Prefetch and distribute OCI images to APC K3s nodes"}
	command.AddCommand(o.imagePrefetchCommand(), o.imageSyncCommand())
	return command
}

func (o *options) imagePrefetchCommand() *cobra.Command {
	config := images.Options{Pull: true}
	command := &cobra.Command{
		Use:   "prefetch IMAGE [IMAGE...]",
		Short: "Pull images on this Mac and import them into the local K3s server",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			config.Cluster = o.clusterName(nil)
			config.Images = args
			config.Stdout = o.out
			config.Stderr = o.errOut
			result, err := images.NewManager().Transfer(command.Context(), config)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "prefetched %d image(s), %s archive, %d target(s)\n", len(result.Images), byteSize(result.ArchiveBytes), len(result.Targets))
			return nil
		},
	}
	command.Flags().StringVar(&config.Platform, "platform", images.DefaultPlatform, "OCI platform to pull and import")
	command.Flags().BoolVar(&config.Pull, "pull", true, "pull images into the Apple host store before export")
	return command
}

func (o *options) imageSyncCommand() *cobra.Command {
	config := images.Options{Pull: true}
	command := &cobra.Command{
		Use:   "sync IMAGE [IMAGE...] --peer USER@HOST",
		Short: "Stream images into the local server and remote K3s agents over SSH",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if len(config.Peers) == 0 {
				return fmt.Errorf("at least one --peer is required")
			}
			config.Cluster = o.clusterName(nil)
			config.Images = args
			config.Stdout = o.out
			config.Stderr = o.errOut
			result, err := images.NewManager().Transfer(command.Context(), config)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "synced %d image(s), %s archive, %d target(s)\n", len(result.Images), byteSize(result.ArchiveBytes), len(result.Targets))
			return nil
		},
	}
	command.Flags().StringSliceVar(&config.Peers, "peer", nil, "SSH peer receiving the agent image import (repeatable)")
	command.Flags().StringVar(&config.Platform, "platform", images.DefaultPlatform, "OCI platform to pull and import")
	command.Flags().BoolVar(&config.Pull, "pull", true, "pull images into the Apple host store before export")
	return command
}

func (o *options) doctorCommand() *cobra.Command {
	var role, listenAddress, peer, outputFormat string
	var apiPort, vxlanPort int
	command := &cobra.Command{
		Use:   "doctor",
		Short: "Check whether this Mac can host an APC Kubernetes node",
		RunE: func(command *cobra.Command, _ []string) error {
			report := doctor.Run(command.Context(), doctor.Options{
				Role: role, ListenAddress: listenAddress, APIPort: apiPort, FlannelPort: vxlanPort, Peer: peer,
			})
			var err error
			switch outputFormat {
			case "", "text":
				err = report.WriteText(o.out)
			case "json":
				err = report.WriteJSON(o.out)
			default:
				return fmt.Errorf("unsupported output format %q; use text or json", outputFormat)
			}
			if err != nil {
				return err
			}
			if report.FailureCount() > 0 {
				return fmt.Errorf("%d required checks failed", report.FailureCount())
			}
			return nil
		},
	}
	command.Flags().StringVar(&role, "role", "server", "node role: server or agent")
	command.Flags().StringVar(&listenAddress, "listen-address", "127.0.0.1", "host address reserved for published ports")
	command.Flags().IntVar(&apiPort, "api-port", cluster.DefaultAPIPort, "host port for the Kubernetes API")
	command.Flags().IntVar(&vxlanPort, "vxlan-port", cluster.DefaultVXLANPort, "host UDP port for Flannel VXLAN")
	command.Flags().StringVar(&peer, "peer", "", "optional peer hostname or IP whose SSH reachability is checked")
	command.Flags().StringVarP(&outputFormat, "output", "o", "text", "output format: text or json")
	return command
}

func (o *options) clusterCommand() *cobra.Command {
	command := &cobra.Command{Use: "cluster", Short: "Manage Kubernetes clusters hosted by apple/container"}
	command.AddCommand(
		o.clusterCreateCommand(), o.clusterStatusCommand(), o.clusterDoctorCommand(), o.clusterStartCommand(), o.clusterStopCommand(), o.clusterDeleteCommand(), o.clusterBackupCommand(), o.clusterRestoreCommand(), o.clusterUpgradeCommand(), o.clusterNetworkPolicyCommand(), o.clusterWriteJoinTokenCommand(), o.clusterHACommand(),
	)
	return command
}

func (o *options) clusterNetworkPolicyCommand() *cobra.Command {
	var confirmed bool
	var timeout time.Duration
	command := &cobra.Command{
		Use:   "network-policy (enable|disable) [NAME]",
		Short: "Enable or disable Kubernetes NetworkPolicy enforcement",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing server network reconfiguration without --yes")
			}
			enabled := false
			switch args[0] {
			case "enable":
				enabled = true
			case "disable":
			default:
				return fmt.Errorf("network-policy action must be enable or disable")
			}
			nameArgs := []string(nil)
			if len(args) == 2 {
				nameArgs = args[1:]
			}
			state, err := cluster.NewManager("container").SetNetworkPolicy(command.Context(), o.clusterName(nameArgs), enabled, timeout)
			if err != nil {
				return err
			}
			status := "disabled"
			if enabled {
				status = "enabled"
			}
			fmt.Fprintf(o.out, "networkpolicy.apc.dev/%s %s; node/%s Ready\n", state.Name, status, state.NodeName)
			return nil
		},
	}
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm recreation of the server VM envelope")
	command.Flags().DurationVar(&timeout, "wait", 2*time.Minute, "maximum time to wait for the recreated server")
	return command
}

func (o *options) clusterUpgradeCommand() *cobra.Command {
	var image, backupPath string
	var confirmed bool
	command := &cobra.Command{
		Use:   "upgrade [NAME] --image IMAGE@sha256:DIGEST",
		Short: "Upgrade a K3s server with automatic backup and rollback",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing cluster upgrade without --yes")
			}
			result, err := cluster.NewManager("container").UpgradeServer(command.Context(), o.clusterName(args), image, backupPath)
			if err != nil {
				return err
			}
			if !result.Changed {
				fmt.Fprintf(o.out, "cluster.apc.dev/%s already uses %s\n", result.State.Name, result.ToImage)
				return nil
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s upgraded and Ready (%s)\n", result.State.Name, result.State.K3sVersion)
			fmt.Fprintf(o.out, "rollback backup: %s\n", result.BackupPath)
			return nil
		},
	}
	command.Flags().StringVar(&image, "image", "", "immutable ARM64 K3s OCI image digest")
	command.Flags().StringVar(&backupPath, "backup", "", "pre-upgrade backup directory; defaults to APC's private backup root")
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm the server image replacement")
	_ = command.MarkFlagRequired("image")
	return command
}

func (o *options) clusterBackupCommand() *cobra.Command {
	var output string
	command := &cobra.Command{
		Use:   "backup [NAME] --output DIRECTORY",
		Short: "Create a consistent offline backup of a K3s server volume",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			result, err := cluster.NewManager("container").BackupServer(command.Context(), o.clusterName(args), output)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "backup.apc.dev created at %s (%s, sha256:%s)\n", result.Path, byteSize(result.Bytes), result.DataSHA256)
			return nil
		},
	}
	command.Flags().StringVarP(&output, "output", "o", "", "new private directory that will contain the backup")
	_ = command.MarkFlagRequired("output")
	return command
}

func (o *options) clusterRestoreCommand() *cobra.Command {
	var input string
	var confirmed bool
	command := &cobra.Command{
		Use:   "restore [NAME] --from DIRECTORY",
		Short: "Replace a K3s server volume with a validated APC backup",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing destructive cluster restore without --yes")
			}
			state, err := cluster.NewManager("container").RestoreServer(command.Context(), o.clusterName(args), input)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s restored and Ready (%s)\n", state.Name, state.K3sVersion)
			return nil
		},
	}
	command.Flags().StringVar(&input, "from", "", "APC backup directory")
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm replacement of the current server data")
	_ = command.MarkFlagRequired("from")
	return command
}

func (o *options) clusterDeleteCommand() *cobra.Command {
	var confirmed, keepData bool
	command := &cobra.Command{
		Use:   "delete [NAME]",
		Short: "Delete an APC K3s server and, by default, its data",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing destructive cluster deletion without --yes")
			}
			name := o.clusterName(args)
			if err := cluster.NewManager("container").DeleteServer(command.Context(), name, keepData); err != nil {
				return err
			}
			if keepData {
				fmt.Fprintf(o.out, "cluster.apc.dev/%s VM removed; data retained\n", name)
			} else {
				fmt.Fprintf(o.out, "cluster.apc.dev/%s deleted\n", name)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm deletion without an interactive prompt")
	command.Flags().BoolVar(&keepData, "keep-data", false, "retain the APC data volume and saved configuration")
	return command
}

func (o *options) clusterDoctorCommand() *cobra.Command {
	config := cluster.DiagnoseOptions{}
	var outputFormat string
	command := &cobra.Command{
		Use:   "doctor [NAME]",
		Short: "Run end-to-end Kubernetes and cross-node network diagnostics",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			report, err := cluster.NewManager("container").Diagnose(command.Context(), o.clusterName(args), config)
			if err != nil {
				return err
			}
			switch outputFormat {
			case "", "text":
				err = report.WriteText(o.out)
			case "json", "yaml":
				err = printObject(o.out, report, outputFormat)
			default:
				return fmt.Errorf("unsupported output format %q; use text, json, or yaml", outputFormat)
			}
			if err != nil {
				return err
			}
			if report.FailureCount() > 0 {
				return fmt.Errorf("%d cluster checks failed", report.FailureCount())
			}
			return nil
		},
	}
	command.Flags().StringVar(&config.Image, "image", "docker.io/library/nginx:alpine", "diagnostic Pod image")
	command.Flags().DurationVar(&config.Timeout, "timeout", 2*time.Minute, "overall diagnostic timeout")
	command.Flags().DurationVar(&config.ProbeTimeout, "probe-timeout", 8*time.Second, "timeout for each network probe")
	command.Flags().BoolVar(&config.Keep, "keep", false, "retain the diagnostic namespace for inspection")
	command.Flags().BoolVar(&config.SkipEgress, "skip-egress", false, "skip public HTTPS egress probes")
	command.Flags().StringVarP(&outputFormat, "output", "o", "text", "output format: text, json, or yaml")
	return command
}

func (o *options) clusterCreateCommand() *cobra.Command {
	config := cluster.Config{DisableTraefik: true, EnableNetworkPolicy: true}
	command := &cobra.Command{
		Use:   "create [NAME]",
		Short: "Create an isolated ARM64 K3s server node",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if len(args) == 1 {
				config.Name = args[0]
			}
			manager := cluster.NewManager("container")
			state, err := manager.Create(command.Context(), config)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s ready\n", state.Name)
			fmt.Fprintf(o.out, "node/%s Ready (%s)\n", state.NodeName, state.K3sVersion)
			fmt.Fprintf(o.out, "kubeconfig: %s\n", state.Kubeconfig)
			fmt.Fprintf(o.out, "export KUBECONFIG=%q\n", state.Kubeconfig)
			return nil
		},
	}
	command.Flags().StringVar(&config.NodeName, "node-name", "", "Kubernetes node name")
	command.Flags().StringVar(&config.Image, "image", cluster.DefaultK3sImage, "pinned K3s OCI image")
	command.Flags().IntVar(&config.CPUs, "cpus", 4, "virtual CPUs allocated to the node")
	command.Flags().StringVar(&config.Memory, "memory", "4G", "memory allocated to the node")
	command.Flags().StringVar(&config.ListenAddress, "listen-address", "127.0.0.1", "host address used for port publishing")
	command.Flags().StringVar(&config.AdvertiseAddress, "advertise-address", "", "LAN address advertised to other nodes")
	command.Flags().IntVar(&config.APIPort, "api-port", cluster.DefaultAPIPort, "host port for the Kubernetes API")
	command.Flags().IntVar(&config.VXLANPort, "vxlan-port", cluster.DefaultVXLANPort, "host UDP port for Flannel VXLAN")
	command.Flags().IntVar(&config.KubeletPort, "kubelet-port", cluster.DefaultKubeletPort, "host port for kubelet")
	command.Flags().DurationVar(&config.StartupTimeout, "wait", 2*time.Minute, "maximum time to wait for a Ready node")
	command.Flags().StringVar(&config.KubeconfigPath, "kubeconfig", "", "kubeconfig destination")
	command.Flags().BoolVar(&config.DisableTraefik, "disable-ingress", true, "disable bundled Traefik and ServiceLB on the managed node")
	command.Flags().BoolVar(&config.EnableNetworkPolicy, "enable-network-policy", true, "enable K3s NetworkPolicy enforcement")
	return command
}

func (o *options) clusterStatusCommand() *cobra.Command {
	var outputFormat string
	command := &cobra.Command{
		Use:   "status [NAME]",
		Short: "Show the K3s server and Kubernetes node state",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			name := o.clusterName(args)
			state, err := cluster.NewManager("container").Status(command.Context(), name)
			if err != nil {
				return err
			}
			if outputFormat == "json" || outputFormat == "yaml" {
				return printObject(o.out, state, outputFormat)
			}
			if outputFormat != "" && outputFormat != "wide" {
				return fmt.Errorf("unsupported output format %q; use json, yaml, or wide", outputFormat)
			}
			writer := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(writer, "NAME\tRUNTIME\tNODE\tREADY\tVERSION\tAPI")
			fmt.Fprintf(writer, "%s\t%s\t%s\t%t\t%s\t%s\n", state.Name, state.RuntimeState, state.NodeName, state.NodeReady, state.K3sVersion, state.APIEndpoint)
			return writer.Flush()
		},
	}
	command.Flags().StringVarP(&outputFormat, "output", "o", "", "output format: json, yaml, or wide")
	return command
}

func (o *options) clusterStartCommand() *cobra.Command {
	var timeout time.Duration
	command := &cobra.Command{
		Use:   "start [NAME]",
		Short: "Start a stopped APC K3s node",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			state, err := cluster.NewManager("container").Start(command.Context(), o.clusterName(args), timeout)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s ready\n", state.Name)
			return nil
		},
	}
	command.Flags().DurationVar(&timeout, "wait", 2*time.Minute, "maximum time to wait for a Ready node")
	return command
}

func (o *options) clusterStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop [NAME]",
		Short: "Stop an APC K3s node without deleting its state",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			name := o.clusterName(args)
			if err := cluster.NewManager("container").Stop(command.Context(), name); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s stopped\n", name)
			return nil
		},
	}
}

func (o *options) clusterWriteJoinTokenCommand() *cobra.Command {
	var outputPath string
	command := &cobra.Command{
		Use:   "write-join-token [NAME]",
		Short: "Write a K3s agent token to a protected file",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			name := o.clusterName(args)
			path, err := cluster.NewManager("container").WriteAgentToken(command.Context(), name, outputPath)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "join token written to %s (mode 0600)\n", path)
			return nil
		},
	}
	command.Flags().StringVarP(&outputPath, "output", "o", "", "protected output file; defaults to the cluster configuration directory")
	return command
}

func (o *options) nodeCommand() *cobra.Command {
	command := &cobra.Command{Use: "node", Short: "Manage K3s worker nodes on this Mac"}
	command.AddCommand(o.nodeJoinCommand(), o.nodeStatusCommand(), o.nodeStartCommand(), o.nodeStopCommand(), o.nodeRemoveCommand())
	return command
}

func (o *options) nodeRemoveCommand() *cobra.Command {
	var confirmed, keepData bool
	command := &cobra.Command{
		Use:   "remove [CLUSTER]",
		Short: "Remove the local K3s agent and, by default, its data",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing destructive node removal without --yes")
			}
			name := o.clusterName(args)
			if err := cluster.NewManager("container").DeleteAgent(command.Context(), name, keepData); err != nil {
				return err
			}
			if keepData {
				fmt.Fprintf(o.out, "node.apc.dev/%s VM removed; data retained\n", name)
			} else {
				fmt.Fprintf(o.out, "node.apc.dev/%s removed\n", name)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm removal without an interactive prompt")
	command.Flags().BoolVar(&keepData, "keep-data", false, "retain the APC data volume and saved configuration")
	return command
}

func (o *options) nodeStartCommand() *cobra.Command {
	var timeout time.Duration
	command := &cobra.Command{
		Use:   "start [CLUSTER]",
		Short: "Start a stopped K3s agent from its saved configuration",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			state, err := cluster.NewManager("container").StartAgent(command.Context(), o.clusterName(args), timeout)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "node.apc.dev/%s connected (%s)\n", state.NodeName, state.RuntimeState)
			return nil
		},
	}
	command.Flags().DurationVar(&timeout, "wait", 45*time.Second, "maximum time to wait for the agent connection")
	return command
}

func (o *options) nodeStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop [CLUSTER]",
		Short: "Stop the local K3s agent without deleting its state",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			name := o.clusterName(args)
			if err := cluster.NewManager("container").StopAgent(command.Context(), name); err != nil {
				return err
			}
			fmt.Fprintf(o.out, "node.apc.dev/%s stopped\n", name)
			return nil
		},
	}
}

func (o *options) nodeJoinCommand() *cobra.Command {
	config := cluster.AgentConfig{}
	command := &cobra.Command{
		Use:   "join [CLUSTER]",
		Short: "Join this Mac to an APC K3s cluster",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			config.Name = o.clusterName(args)
			state, err := cluster.NewManager("container").Join(command.Context(), config)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "node.apc.dev/%s connected (%s)\n", state.NodeName, state.RuntimeState)
			return nil
		},
	}
	command.Flags().StringVar(&config.NodeName, "node-name", "", "unique Kubernetes node name")
	command.Flags().StringVar(&config.ServerURL, "server-url", "", "K3s server URL, for example https://192.0.2.10:16443")
	command.Flags().StringVar(&config.TokenFile, "token-file", "", "path to a mode-0600 K3s agent token")
	command.Flags().StringVar(&config.Image, "image", cluster.DefaultK3sImage, "pinned K3s OCI image")
	command.Flags().IntVar(&config.CPUs, "cpus", 2, "virtual CPUs allocated to the node")
	command.Flags().StringVar(&config.Memory, "memory", "2G", "memory allocated to the node")
	command.Flags().StringVar(&config.ListenAddress, "listen-address", "0.0.0.0", "host address used for port publishing")
	command.Flags().StringVar(&config.AdvertiseAddress, "advertise-address", "", "this Mac's trusted-LAN address")
	command.Flags().IntVar(&config.VXLANPort, "vxlan-port", cluster.DefaultVXLANPort, "host UDP port for Flannel VXLAN")
	command.Flags().IntVar(&config.KubeletPort, "kubelet-port", cluster.DefaultKubeletPort, "host port for kubelet")
	command.Flags().DurationVar(&config.StartupTimeout, "wait", 45*time.Second, "maximum time to wait for the agent connection")
	return command
}

func (o *options) nodeStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [CLUSTER]",
		Short: "Show this Mac's K3s agent VM state",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			state, err := cluster.NewManager("container").AgentStatus(command.Context(), o.clusterName(args))
			if err != nil {
				return err
			}
			writer := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(writer, "CLUSTER\tCONTAINER\tRUNTIME\tADDRESS")
			fmt.Fprintf(writer, "%s\t%s\t%s\t%s\n", state.Name, state.Container, state.RuntimeState, state.Address)
			return writer.Flush()
		},
	}
}

func (o *options) kubeconfigCommand() *cobra.Command {
	command := &cobra.Command{Use: "kubeconfig", Short: "Locate APC-managed Kubernetes credentials"}
	command.AddCommand(&cobra.Command{
		Use:   "path [CLUSTER]",
		Short: "Print the kubeconfig path",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			path, err := cluster.NewManager("container").PrepareKubeconfig(command.Context(), o.clusterName(args))
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(o.out, path)
			return err
		},
	})
	return command
}

func (o *options) configCommand() *cobra.Command {
	command := &cobra.Command{Use: "config", Short: "Manage the active APC K3s cluster context"}
	command.AddCommand(
		&cobra.Command{
			Use:   "use-cluster NAME",
			Short: "Select the cluster used by kubectl-compatible APC commands",
			Args:  cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				if err := cluster.SetCurrentCluster(args[0]); err != nil {
					return err
				}
				fmt.Fprintf(o.out, "Switched to APC cluster %q.\n", args[0])
				return nil
			},
		},
		&cobra.Command{
			Use:   "current-cluster",
			Short: "Print the active APC K3s cluster",
			Args:  cobra.NoArgs,
			RunE: func(_ *cobra.Command, _ []string) error {
				name, err := cluster.CurrentCluster()
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(o.out, name)
				return err
			},
		},
		&cobra.Command{
			Use:   "get-clusters",
			Short: "List clusters with locally managed kubeconfigs",
			Args:  cobra.NoArgs,
			RunE: func(_ *cobra.Command, _ []string) error {
				names, err := cluster.ListClusters()
				if err != nil {
					return err
				}
				current, _ := cluster.CurrentCluster()
				writer := tabwriter.NewWriter(o.out, 0, 4, 2, ' ', 0)
				fmt.Fprintln(writer, "CURRENT\tNAME")
				for _, name := range names {
					marker := ""
					if name == current {
						marker = "*"
					}
					fmt.Fprintf(writer, "%s\t%s\n", marker, name)
				}
				return writer.Flush()
			},
		},
	)
	return command
}

func (o *options) kubectlCommand() *cobra.Command {
	return &cobra.Command{
		Use:                "kubectl CLUSTER -- COMMAND [ARG...]",
		Short:              "Run the K3s-bundled kubectl (bootstrap convenience)",
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			name := args[0]
			arguments := args[1:]
			if arguments[0] == "--" {
				arguments = arguments[1:]
			}
			if len(arguments) == 0 {
				return fmt.Errorf("kubectl command is required")
			}
			stdout, stderr, err := cluster.NewManager("container").Kubectl(command.Context(), name, arguments...)
			if len(stdout) > 0 {
				_, _ = o.out.Write(stdout)
			}
			if len(stderr) > 0 {
				_, _ = o.errOut.Write(stderr)
			}
			if err != nil {
				return fmt.Errorf("kubectl failed: %w", err)
			}
			return nil
		},
	}
}

func (o *options) clusterName(args []string) string {
	if len(args) > 0 && args[0] != "" {
		return args[0]
	}
	if o.cluster != "" {
		return o.cluster
	}
	if value := os.Getenv("APC_CLUSTER"); value != "" {
		return value
	}
	if current, err := cluster.CurrentCluster(); err == nil {
		return current
	}
	return "spike"
}

func (o *options) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the APC version",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(o.out, "APC Version: %s\n", Version)
			return err
		},
	}
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

func byteSize(value int64) string {
	if value >= 1<<30 {
		return fmt.Sprintf("%.1fGi", float64(value)/float64(1<<30))
	}
	return fmt.Sprintf("%.0fMi", float64(value)/float64(1<<20))
}
