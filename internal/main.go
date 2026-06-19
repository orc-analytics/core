package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	pb "github.com/orca-telemetry/contract/go/v2"
	db "github.com/orca-telemetry/core/internal/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type (
	CoreServer struct {
		pb.UnimplementedCoreServer
		db *db.DB
	}
)

var (
	MAX_PROCESSORS = 20
)

func AuthInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// allow unathenticated access to core to this endpoint
	if info.FullMethod == "/Core/RegisterWorker" {
		return handler(ctx, req)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	vals := md.Get("authorization")
	if len(vals) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}

	token := vals[0] // e.g. "Bearer <token>"

	// TODO: Check the authentication against the nonce's

	// Validate token, add claims to context, etc.
	ctx = context.WithValue(ctx, ctxKeyUser{}, token)

	return handler(ctx, req) // call the actual handler
}

// NewServer produces a new core gRPC server
func NewServer(
	ctx context.Context,
	connStr string,
) (*CoreServer, error) {
	DB, err := db.NewDbQueries(ctx, connStr)
	if err != nil {
		slog.Error(
			"could not initialise client",
			"error",
			err,
		)

		return nil, err
	}

	s := &CoreServer{
		db: DB,
	}
	return s, nil
}

// --------------------------- gRPC Services ---------------------------

// Register a worker, returns worker ID or an error on public key conflict
func (c *CoreServer) RegisterWorker(ctx context.Context, w *pb.RegisterWorkerRequest) (string, error) {
	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		slog.Error("could not start a transaction with the DB", "error", err)
		return "", err
	}
	defer tx.Rollback(ctx)
	qtx := c.db.WithTx(tx)

	worker_uuid, err := qtx.RegisterWorker(ctx, db.RegisterWorkerParams{
		PublicKey: w.GetPublicKey(),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == db.PgUniqueViolation {
				return "", db.ErrWorkerAlreadyExists
			}
			return "", fmt.Errorf("unknown postgres error: %w", err)
		}
		return "", fmt.Errorf("unknown error occurred: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}
	return worker_uuid.String(), nil
}

func (c *CoreServer) GetNonce(context.Context, *GetNonceRequest) (*GetNonceResponse, error) {
}
