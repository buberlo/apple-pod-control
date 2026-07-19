package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/buberlo/apple-pod-control/internal/cluster"
)

var apcCommands = map[string]struct{}{
	"cluster": {}, "completion": {}, "config": {}, "doctor": {}, "help": {},
	"image": {}, "kubeconfig": {}, "kubectl": {}, "node": {}, "system": {}, "version": {},
}

var leadingFlagsWithValues = map[string]struct{}{
	"--as": {}, "--as-group": {}, "--cache-dir": {}, "--certificate-authority": {},
	"--client-certificate": {}, "--client-key": {}, "--context": {}, "--kubeconfig": {},
	"--namespace": {}, "--password": {}, "--profile": {}, "--profile-output": {},
	"--request-timeout": {}, "--server": {}, "--tls-server-name": {}, "--token": {},
	"--user": {}, "--username": {}, "-n": {},
}

type passthroughEnvironment struct {
	getenv         func(string) string
	lookPath       func(string) (string, error)
	currentCluster func() (string, error)
	kubeconfigPath func(string) (string, error)
	stat           func(string) (os.FileInfo, error)
	run            func(context.Context, string, []string, string, io.Reader, io.Writer, io.Writer) error
}

// TryKubernetesPassthrough executes kubectl-compatible APC commands against the
// active APC v2 cluster. It returns handled=false for APC lifecycle commands and
// when no v2 cluster has been selected, preserving the v1 CLI path.
func TryKubernetesPassthrough(ctx context.Context, arguments []string, stdin io.Reader, stdout, stderr io.Writer) (bool, error) {
	return tryKubernetesPassthrough(ctx, arguments, stdin, stdout, stderr, defaultPassthroughEnvironment())
}

func defaultPassthroughEnvironment() passthroughEnvironment {
	return passthroughEnvironment{
		getenv:         os.Getenv,
		lookPath:       exec.LookPath,
		currentCluster: cluster.CurrentCluster,
		kubeconfigPath: cluster.ResolvedKubeconfigPath,
		stat:           os.Stat,
		run: func(ctx context.Context, binary string, arguments []string, kubeconfig string, stdin io.Reader, stdout, stderr io.Writer) error {
			command := exec.CommandContext(ctx, binary, arguments...)
			command.Stdin = stdin
			command.Stdout = stdout
			command.Stderr = stderr
			environment := make([]string, 0, len(os.Environ())+1)
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "KUBECONFIG=") {
					environment = append(environment, value)
				}
			}
			command.Env = append(environment, "KUBECONFIG="+kubeconfig)
			return command.Run()
		},
	}
}

func tryKubernetesPassthrough(ctx context.Context, arguments []string, stdin io.Reader, stdout, stderr io.Writer, env passthroughEnvironment) (bool, error) {
	forwarded, explicitCluster, kubernetesCommand, legacy, err := routeKubernetesArguments(arguments)
	if err != nil {
		return true, err
	}
	if legacy || !kubernetesCommand {
		return false, nil
	}

	clusterName := explicitCluster
	if clusterName == "" {
		clusterName = env.getenv("APC_CLUSTER")
	}
	if clusterName == "" {
		clusterName, err = env.currentCluster()
		if errors.Is(err, cluster.ErrNoCurrentCluster) {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}

	kubeconfig, err := env.kubeconfigPath(clusterName)
	if err != nil {
		return true, fmt.Errorf("resolve kubeconfig for cluster %q: %w", clusterName, err)
	}
	info, err := env.stat(kubeconfig)
	if err != nil {
		return true, fmt.Errorf("read kubeconfig for cluster %q: %w", clusterName, err)
	}
	if !info.Mode().IsRegular() {
		return true, fmt.Errorf("kubeconfig for cluster %q is not a regular file", clusterName)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return true, fmt.Errorf("kubeconfig for cluster %q must have mode 0600 or stricter", clusterName)
	}
	binary, err := env.lookPath("kubectl")
	if err != nil {
		return true, fmt.Errorf("kubectl is required for APC v2 workload commands: %w", err)
	}
	if err := env.run(ctx, binary, forwarded, kubeconfig, stdin, stdout, stderr); err != nil {
		return true, fmt.Errorf("kubectl: %w", err)
	}
	return true, nil
}

func routeKubernetesArguments(arguments []string) (forwarded []string, clusterName string, kubernetesCommand, legacy bool, err error) {
	forwarded = make([]string, 0, len(arguments))
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--legacy" {
			return arguments, "", false, true, nil
		}
		if !kubernetesCommand {
			switch {
			case argument == "--cluster":
				if index+1 >= len(arguments) || strings.HasPrefix(arguments[index+1], "-") {
					return nil, "", false, false, fmt.Errorf("--cluster requires a name")
				}
				clusterName = arguments[index+1]
				index++
				continue
			case strings.HasPrefix(argument, "--cluster="):
				clusterName = strings.TrimPrefix(argument, "--cluster=")
				if clusterName == "" {
					return nil, "", false, false, fmt.Errorf("--cluster requires a name")
				}
				continue
			}
			if _, takesValue := leadingFlagsWithValues[argument]; takesValue {
				forwarded = append(forwarded, argument)
				if index+1 < len(arguments) {
					index++
					forwarded = append(forwarded, arguments[index])
				}
				continue
			}
			if !strings.HasPrefix(argument, "-") {
				if _, internal := apcCommands[argument]; internal {
					return arguments, "", false, false, nil
				}
				kubernetesCommand = true
			}
		}
		forwarded = append(forwarded, argument)
	}
	return forwarded, clusterName, kubernetesCommand, false, nil
}
