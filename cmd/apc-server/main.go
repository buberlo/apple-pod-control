package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	apcv1 "github.com/buberlo/apple-pod-control/gen/apc/v1"
	"github.com/buberlo/apple-pod-control/internal/api"
	"github.com/buberlo/apple-pod-control/internal/control"
	"github.com/buberlo/apple-pod-control/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

func main() {
	var httpAddress, grpcAddress, databasePath, bearerToken string
	var tlsCertificate, tlsKey, clientCA string
	flag.StringVar(&httpAddress, "http-address", "0.0.0.0:8080", "REST API listen address")
	flag.StringVar(&grpcAddress, "grpc-address", "0.0.0.0:9090", "agent gRPC listen address")
	flag.StringVar(&databasePath, "database", ".apc/apc.db", "SQLite database path")
	flag.StringVar(&bearerToken, "token", os.Getenv("APC_TOKEN"), "optional REST API bearer token")
	flag.StringVar(&tlsCertificate, "tls-cert", "", "server TLS certificate")
	flag.StringVar(&tlsKey, "tls-key", "", "server TLS private key")
	flag.StringVar(&clientCA, "client-ca", "", "CA used to require agent mTLS certificates")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := os.MkdirAll(filepath.Dir(databasePath), 0o750); err != nil {
		logger.Error("create data directory", "error", err)
		os.Exit(1)
	}
	database, err := store.Open(databasePath)
	if err != nil {
		logger.Error("open state store", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	sessions := control.NewSessions()
	reconciler := control.NewReconciler(database, sessions, logger, 2*time.Second)
	agentService := control.NewAgentService(database, sessions, reconciler, logger)
	grpcOptions := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 10 * time.Second, Timeout: 3 * time.Second}),
		grpc.MaxRecvMsgSize(4 << 20), grpc.MaxSendMsgSize(4 << 20),
	}
	if tlsCertificate != "" || tlsKey != "" {
		tlsConfig, err := serverTLSConfig(tlsCertificate, tlsKey, clientCA)
		if err != nil {
			logger.Error("configure TLS", "error", err)
			os.Exit(1)
		}
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	grpcServer := grpc.NewServer(grpcOptions...)
	apcv1.RegisterAgentControlServer(grpcServer, agentService)
	reflection.Register(grpcServer)

	httpServer := &http.Server{
		Addr: httpAddress, Handler: api.NewServer(database, reconciler, logger, bearerToken).Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 90 * time.Second,
	}
	grpcListener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		logger.Error("listen for agents", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsChannel := make(chan error, 3)
	go func() { errorsChannel <- reconciler.Run(ctx) }()
	go func() {
		logger.Info("agent gRPC API listening", "address", grpcAddress, "tls", tlsCertificate != "")
		errorsChannel <- grpcServer.Serve(grpcListener)
	}()
	go func() {
		logger.Info("REST API listening", "address", httpAddress, "tls", tlsCertificate != "")
		if tlsCertificate != "" {
			errorsChannel <- httpServer.ListenAndServeTLS(tlsCertificate, tlsKey)
		} else {
			errorsChannel <- httpServer.ListenAndServe()
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errorsChannel:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server component stopped", "error", err)
		}
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
}

func serverTLSConfig(certificateFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certificateFile == "" || keyFile == "" {
		return nil, fmt.Errorf("both --tls-cert and --tls-key are required")
	}
	certificate, err := tls.LoadX509KeyPair(certificateFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	configuration := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}
	if clientCAFile != "" {
		data, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("client CA contains no certificates")
		}
		configuration.ClientCAs = roots
		configuration.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return configuration, nil
}
