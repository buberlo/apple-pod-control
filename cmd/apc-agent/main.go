package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/buberlo/apple-pod-control/internal/agent"
	containerruntime "github.com/buberlo/apple-pod-control/internal/runtime"
)

type labelFlags map[string]string

func (l *labelFlags) String() string {
	values := make([]string, 0, len(*l))
	for key, value := range *l {
		values = append(values, key+"="+value)
	}
	return strings.Join(values, ",")
}

func (l *labelFlags) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 || parts[0] == "" {
		return fmt.Errorf("label must have key=value form")
	}
	(*l)[parts[0]] = parts[1]
	return nil
}

func main() {
	var config agent.Config
	var runtimeBinary string
	var fakeRuntime bool
	var labels = labelFlags{"apc.dev/architecture": "arm64"}
	flag.StringVar(&config.ServerAddress, "server", envOr("APC_GRPC_SERVER", "127.0.0.1:9090"), "control plane gRPC address")
	flag.StringVar(&config.NodeID, "node-id", "", "stable node ID (defaults to hostname)")
	flag.StringVar(&config.AdvertiseAddress, "advertise-address", "", "node LAN address advertised to the control plane")
	flag.Var(&labels, "label", "node label key=value (repeatable)")
	flag.StringVar(&runtimeBinary, "container-binary", "container", "path to apple/container CLI")
	flag.BoolVar(&fakeRuntime, "fake-runtime", false, "use in-memory runtime for development")
	flag.StringVar(&config.TLSCAFile, "tls-ca", "", "control plane CA certificate")
	flag.StringVar(&config.TLSCertFile, "tls-cert", "", "agent client certificate for mTLS")
	flag.StringVar(&config.TLSKeyFile, "tls-key", "", "agent client key for mTLS")
	flag.StringVar(&config.TLSServerName, "tls-server-name", "", "expected control plane TLS server name")
	flag.Parse()
	config.Labels = labels

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var selectedRuntime containerruntime.Runtime
	if fakeRuntime {
		selectedRuntime = containerruntime.NewFake()
	} else {
		selectedRuntime = containerruntime.NewAppleContainer(runtimeBinary)
	}
	nodeAgent, err := agent.New(config, selectedRuntime, logger)
	if err != nil {
		logger.Error("configure agent", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := nodeAgent.Run(ctx); err != nil {
		logger.Error("agent stopped", "error", err)
		os.Exit(1)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
