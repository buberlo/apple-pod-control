package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

type helmEnvironment struct {
	getenv            func(string) string
	currentCluster    func() (string, error)
	lookPath          func(string) (string, error)
	prepareKubeconfig func(context.Context, string) (string, error)
	stat              func(string) (os.FileInfo, error)
	run               func(context.Context, string, []string, string, io.Reader, io.Writer, io.Writer) error
}

func defaultHelmEnvironment() helmEnvironment {
	manager := cluster.NewManager("container")
	return helmEnvironment{
		getenv:            os.Getenv,
		currentCluster:    cluster.CurrentCluster,
		lookPath:          exec.LookPath,
		prepareKubeconfig: manager.PrepareKubeconfig,
		stat:              os.Stat,
		run: func(ctx context.Context, binary string, arguments []string, kubeconfig string, stdin io.Reader, stdout, stderr io.Writer) error {
			process := exec.CommandContext(ctx, binary, arguments...)
			process.Stdin = stdin
			process.Stdout = stdout
			process.Stderr = stderr
			environment := make([]string, 0, len(os.Environ())+1)
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "KUBECONFIG=") {
					environment = append(environment, value)
				}
			}
			process.Env = append(environment, "KUBECONFIG="+kubeconfig)
			return process.Run()
		},
	}
}

var newHelmEnvironment = defaultHelmEnvironment

func (o *options) helmCommand() *cobra.Command {
	return &cobra.Command{
		Use:                "helm [ARG...]",
		Short:              "Run native Helm against the selected APC cluster",
		DisableFlagParsing: true,
		Args:               cobra.ArbitraryArgs,
		RunE: func(command *cobra.Command, args []string) error {
			environment := newHelmEnvironment()
			name, args, err := helmSelectedCluster(o.cluster, args)
			if err != nil {
				return err
			}
			if name == "" {
				name = environment.getenv("APC_CLUSTER")
			}
			if name == "" {
				name, err = environment.currentCluster()
				if errors.Is(err, cluster.ErrNoCurrentCluster) {
					return errNoSelectedCluster
				}
				if err != nil {
					return fmt.Errorf("resolve current APC cluster: %w", err)
				}
				if name == "" {
					return errNoSelectedCluster
				}
			}
			kubeconfig, err := environment.prepareKubeconfig(command.Context(), name)
			if err != nil {
				return fmt.Errorf("resolve kubeconfig for cluster %q: %w", name, err)
			}
			info, err := environment.stat(kubeconfig)
			if err != nil {
				return fmt.Errorf("read kubeconfig for cluster %q: %w", name, err)
			}
			if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
				return fmt.Errorf("kubeconfig for cluster %q must be a regular file with mode 0600 or stricter", name)
			}
			binary, err := environment.lookPath("helm")
			if err != nil {
				return fmt.Errorf("native Helm is required: %w", err)
			}
			if err := environment.run(command.Context(), binary, args, kubeconfig, command.InOrStdin(), o.out, o.errOut); err != nil {
				return fmt.Errorf("helm: %w", err)
			}
			return nil
		},
	}
}

// Cobra preserves persistent flags when the child disables flag parsing. APC
// owns --cluster, so remove only that selector and leave every Helm flag byte
// for byte untouched.
func helmSelectedCluster(selected string, arguments []string) (string, []string, error) {
	forwarded := make([]string, 0, len(arguments))
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			forwarded = append(forwarded, arguments[index:]...)
			break
		}
		if argument == "--cluster" {
			if index+1 >= len(arguments) || strings.HasPrefix(arguments[index+1], "-") {
				return "", nil, fmt.Errorf("--cluster requires a name")
			}
			selected = arguments[index+1]
			index++
			continue
		}
		if strings.HasPrefix(argument, "--cluster=") {
			selected = strings.TrimPrefix(argument, "--cluster=")
			if selected == "" {
				return "", nil, fmt.Errorf("--cluster requires a name")
			}
			continue
		}
		forwarded = append(forwarded, argument)
	}
	return selected, forwarded, nil
}
