package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/orca-telemetry/contract/go/v2"
	internal "github.com/orca-telemetry/core/internal"
)

func startGRPCServer(ctx context.Context, dbConnString string, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	orcaServer, err := internal.NewServer(ctx, dbConnString)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCoreServer(grpcServer, orcaServer)
	reflection.Register(grpcServer)

	go func() {
		<-ctx.Done()
		slog.Info("shutting down gRPC server")
		grpcServer.GracefulStop()
	}()

	slog.Info("starting server", "port", port)
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("server failed: %w", err)
	}
	return nil
}

func main() {
	flags := parseFlags()

	if err := validateFlags(flags); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	config := buildConfig(flags)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// configure logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	}))
	slog.SetDefault(logger)

	if err := startGRPCServer(ctx, config.ConnectionString, config.Port); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
