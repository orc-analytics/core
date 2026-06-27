package internal

import (
	"context"
	"log"
	"net"

	"os"
	"testing"

	pb "github.com/orca-telemetry/contract/go/v2"
	migrations "github.com/orca-telemetry/core/migrations"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var (
	testConnStr string
	testCtx     context.Context
	lis         *bufconn.Listener
	client      pb.CoreClient
)

// used by in memory gRPC server
const bufSize = 1024 * 1024

// confirms at a pg db can be setup and migrated
func setupPgOnce(ctx context.Context) (string, func()) {
	postgresContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)
	if err != nil {
		panic("Failed to start postgres container: " + err.Error())
	}

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic("Failed to get connection string: " + err.Error())
	}

	err = migrations.MigrateDatalayer(connStr)
	if err != nil {
		panic("Failed to migrate database: " + err.Error())
	}

	cleanup := func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			println("Failed to terminate postgres container:", err.Error())
		}
	}

	return connStr, cleanup
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestMain(m *testing.M) {
	var cleanup func()
	testCtx = context.Background()

	// set up pg
	testConnStr, cleanup = setupPgOnce(testCtx)

	// set up the mocked gRPC server
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	coreServer, err := NewServer(context.Background(), testConnStr)
	if err != nil {
		log.Fatalf("could not create new server: %w", err)

	}
	pb.RegisterCoreServer(s, coreServer)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("server exited with error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client = pb.NewCoreClient(conn)

	// runs all tests
	code := m.Run()

	cleanup()
	os.Exit(code)
}
