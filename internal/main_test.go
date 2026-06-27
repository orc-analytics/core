package internal

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"log"
	"net"

	"os"
	"testing"

	pb "github.com/orca-telemetry/contract/go/v2"
	migrations "github.com/orca-telemetry/core/migrations"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	coreServer, err := NewServer(context.Background(), testConnStr)
	if err != nil {
		log.Fatalf("could not create new server: %v", err)
	}
	s := grpc.NewServer(
		grpc.UnaryInterceptor(coreServer.AuthInterceptor),
	)
	pb.RegisterCoreServer(s, coreServer)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("server exited with error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer conn.Close()
	client = pb.NewCoreClient(conn)

	// runs all tests
	code := m.Run()

	cleanup()
	os.Exit(code)
}

// generateKeypair produces a fresh ed25519 keypair for a test worker.
func generateKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate keypair: %v", err)
	}
	return pub, priv
}

// registerWorker registers a worker and returns the worker ID string.
func registerWorker(t *testing.T, pub ed25519.PublicKey) string {
	t.Helper()
	resp, err := client.RegisterWorker(testCtx, &pb.RegisterWorkerRequest{
		PublicKey: pub,
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if resp.GetWorkerId() == "" {
		t.Fatal("RegisterWorker: empty worker ID in response")
	}
	return resp.GetWorkerId()
}

// completeNonceFlow runs GetNonce -> sign -> CheckNonce and returns the raw
// access key bytes.
func completeNonceFlow(t *testing.T, workerID string, priv ed25519.PrivateKey) []byte {
	t.Helper()

	nonceResp, err := client.GetNonce(testCtx, &pb.GetNonceRequest{
		WorkerId: workerID,
	})
	if err != nil {
		t.Fatalf("GetNonce: %v", err)
	}

	challenge := nonceResp.GetChallenge()
	if len(challenge) == 0 {
		t.Fatal("GetNonce: empty challenge")
	}

	nonceID := nonceResp.GetNonceId()
	if nonceID == "" {
		t.Fatal("GetNonce: empty nonce ID")
	}

	signed := ed25519.Sign(priv, challenge)

	checkResp, err := client.CheckNonce(testCtx, &pb.CheckNonceRequest{
		NonceId:         nonceID,
		WorkerId:        workerID,
		SignedChallenge: signed,
	})
	if err != nil {
		t.Fatalf("CheckNonce: %v", err)
	}

	accessKey := checkResp.GetAccessKey()
	if len(accessKey) == 0 {
		t.Fatal("CheckNonce: empty access key")
	}
	if checkResp.GetExpiresAt() == nil {
		t.Fatal("CheckNonce: missing expiry timestamp")
	}
	return accessKey
}

// authedCtx returns a context carrying a Bearer token built from raw access
// key bytes.
func authedCtx(accessKey []byte) context.Context {
	token := base64.StdEncoding.EncodeToString(accessKey)
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(testCtx, md)
}

// requireCode asserts that err is a gRPC status error with the expected code.
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected gRPC error with code %v, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %v, got %v: %s", want, st.Code(), st.Message())
	}
}

// ---------------------------------------------------------------------------
// Worker registration
// ---------------------------------------------------------------------------

func TestRegisterWorker(t *testing.T) {
	pub, _ := generateKeypair(t)

	resp, err := client.RegisterWorker(testCtx, &pb.RegisterWorkerRequest{PublicKey: pub})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetWorkerId() == "" {
		t.Fatal("expected a non-empty worker ID")
	}
}

func TestRegisterWorker_DuplicatePublicKey(t *testing.T) {
	pub, _ := generateKeypair(t)

	registerWorker(t, pub)

	_, err := client.RegisterWorker(testCtx, &pb.RegisterWorkerRequest{
		PublicKey: pub,
	})
	requireCode(t, err, codes.AlreadyExists)
}

func TestRegisterWorker_BadPublicKey(t *testing.T) {
	_, err := client.RegisterWorker(testCtx, &pb.RegisterWorkerRequest{
		PublicKey: []byte("too-short"),
	})
	requireCode(t, err, codes.InvalidArgument)
}

func TestRegisterWorkerSnapshot_NoAuth(t *testing.T) {
	pub, _ := generateKeypair(t)
	registerWorker(t, pub)

	_, err := client.RegisterWorkerSnapshot(testCtx, &pb.RegisterWorkerSnapshotRequest{})
	requireCode(t, err, codes.Unauthenticated)
}

// ---------------------------------------------------------------------------
// Nonce / authentication flow
// ---------------------------------------------------------------------------

func TestNonceFlow(t *testing.T) {
	pub, priv := generateKeypair(t)
	workerID := registerWorker(t, pub)

	nonceResp, err := client.GetNonce(testCtx, &pb.GetNonceRequest{
		WorkerId: workerID,
	})
	if err != nil {
		t.Fatalf("GetNonce: %v", err)
	}

	challenge := nonceResp.GetChallenge()
	if len(challenge) == 0 {
		t.Fatal("expected a non-empty challenge")
	}
	nonceID := nonceResp.GetNonceId()
	if nonceID == "" {
		t.Fatal("expected a non-empty nonce ID")
	}

	signed := ed25519.Sign(priv, challenge)
	checkResp, err := client.CheckNonce(testCtx, &pb.CheckNonceRequest{
		NonceId:         nonceID,
		WorkerId:        workerID,
		SignedChallenge: signed,
	})
	if err != nil {
		t.Fatalf("CheckNonce: %v", err)
	}
	if len(checkResp.GetAccessKey()) == 0 {
		t.Fatal("expected a non-empty access key")
	}
	if checkResp.GetExpiresAt() == nil {
		t.Fatal("expected an expiry timestamp")
	}
}

func TestNonceFlow_InvalidSignature(t *testing.T) {
	pub, _ := generateKeypair(t)
	workerID := registerWorker(t, pub)

	nonceResp, err := client.GetNonce(testCtx, &pb.GetNonceRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("GetNonce: %v", err)
	}

	// sign with a different key. should be rejected
	_, wrongPriv := generateKeypair(t)
	signed := ed25519.Sign(wrongPriv, nonceResp.GetChallenge())

	_, err = client.CheckNonce(testCtx, &pb.CheckNonceRequest{
		NonceId:         nonceResp.GetNonceId(),
		WorkerId:        workerID,
		SignedChallenge: signed,
	})
	requireCode(t, err, codes.Unauthenticated)
}

func TestNonceFlow_NonceReplayIsRejected(t *testing.T) {
	pub, priv := generateKeypair(t)
	workerID := registerWorker(t, pub)

	nonceResp, err := client.GetNonce(testCtx, &pb.GetNonceRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("GetNonce: %v", err)
	}

	signed := ed25519.Sign(priv, nonceResp.GetChallenge())
	req := &pb.CheckNonceRequest{
		NonceId:         nonceResp.GetNonceId(),
		WorkerId:        workerID,
		SignedChallenge: signed,
	}

	// first use succeeds
	if _, err := client.CheckNonce(testCtx, req); err != nil {
		t.Fatalf("first CheckNonce: %v", err)
	}

	// replay must fail. nonce is consumed
	_, err = client.CheckNonce(testCtx, req)
	requireCode(t, err, codes.NotFound)
}

func TestGetNonce_UnknownWorker(t *testing.T) {
	_, err := client.GetNonce(testCtx, &pb.GetNonceRequest{
		WorkerId: "00000000-0000-0000-0000-000000000000",
	})
	requireCode(t, err, codes.NotFound)
}
