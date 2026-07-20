package cli

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

func (o *options) clusterHASnapshotCommand() *cobra.Command {
	var output, outputFormat string
	command := &cobra.Command{
		Use:   "snapshot [NAME]",
		Short: "Create a protected K3s embedded-etcd snapshot package",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			result, err := newHAManager().SnapshotHA(command.Context(), o.clusterName(args), output)
			if err != nil {
				return err
			}
			switch outputFormat {
			case "", "wide":
				fmt.Fprintf(o.out, "snapshot.apc.dev created: %s (%d bytes, sha256:%s)\n", result.Path, result.Bytes, result.DataSHA256)
				if result.Warning != "" {
					fmt.Fprintf(o.errOut, "warning: %s\n", result.Warning)
				}
				return nil
			case "json", "yaml":
				return printObject(o.out, result, outputFormat)
			default:
				return fmt.Errorf("unsupported output format %q; use wide, json, or yaml", outputFormat)
			}
		},
	}
	command.Flags().StringVar(&output, "output", "", "new private destination directory for snapshot, manifest, and server token")
	command.Flags().StringVarP(&outputFormat, "format", "o", "wide", "output format: wide, json, or yaml")
	_ = command.MarkFlagRequired("output")
	return command
}

func (o *options) clusterHARestoreCommand() *cobra.Command {
	var input string
	var confirmed bool
	var timeout time.Duration
	command := &cobra.Command{
		Use:   "restore [NAME]",
		Short: "Restore all three embedded-etcd servers from a protected snapshot",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if !confirmed {
				return fmt.Errorf("refusing destructive HA restore without --yes")
			}
			name := o.clusterName(args)
			state, err := newHAManager().RestoreHA(command.Context(), name, input, timeout)
			if err != nil {
				return err
			}
			fmt.Fprintf(o.out, "cluster.apc.dev/%s restored and ready (%d/%d servers)\n", state.Name, state.ReadyMembers, haMemberCount)
			return nil
		},
	}
	command.Flags().StringVar(&input, "from", "", "protected snapshot package directory")
	command.Flags().BoolVar(&confirmed, "yes", false, "confirm replacement of current embedded-etcd state")
	command.Flags().DurationVar(&timeout, "wait", 5*time.Minute, "maximum time to restore and rejoin all three servers")
	_ = command.MarkFlagRequired("from")
	return command
}

func (o *options) clusterHAMemberCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "member",
		Short: "Perform quorum-safe lifecycle operations on one HA server",
	}
	command.AddCommand(
		o.clusterHAMemberLifecycleCommand("stop"),
		o.clusterHAMemberLifecycleCommand("start"),
		o.clusterHAMemberLifecycleCommand("restart"),
	)
	return command
}

func (o *options) clusterHAMemberLifecycleCommand(operation string) *cobra.Command {
	var confirmed bool
	var timeout time.Duration
	command := &cobra.Command{
		Use:   operation + " MEMBER [NAME]",
		Short: operation + " one HA server while preserving embedded-etcd quorum",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(command *cobra.Command, args []string) error {
			memberID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("HA member must be numeric (1, 2, or 3)")
			}
			nameArgs := args[1:]
			name := o.clusterName(nameArgs)
			if operation != "start" && !confirmed {
				return fmt.Errorf("refusing HA member %s without --yes", operation)
			}

			manager := newHAManager()
			var state cluster.HAState
			switch operation {
			case "stop":
				state, err = manager.StopHAMember(command.Context(), name, memberID, timeout)
			case "start":
				state, err = manager.StartHAMember(command.Context(), name, memberID, timeout)
			case "restart":
				state, err = manager.RestartHAMember(command.Context(), name, memberID, timeout)
			default:
				return fmt.Errorf("unsupported HA member operation %q", operation)
			}
			if err != nil {
				return err
			}
			completed := map[string]string{"stop": "stopped", "start": "started", "restart": "restarted"}[operation]
			fmt.Fprintf(o.out, "member.apc.dev/%s-%d %s (cluster ready %d/%d)\n", name, memberID, completed, state.ReadyMembers, haMemberCount)
			return nil
		},
	}
	command.Flags().DurationVar(&timeout, "wait", 3*time.Minute, "maximum time to reach the requested member state")
	if operation != "start" {
		command.Flags().BoolVar(&confirmed, "yes", false, "confirm the intentional availability change")
	}
	return command
}

func (o *options) clusterHAProxyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "proxy [NAME]",
		Short: "Serve the stable local TLS-pass-through Kubernetes API endpoint",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return newHAManager().ServeHAProxy(command.Context(), o.clusterName(args))
		},
	}
}
