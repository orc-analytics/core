package internal

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	pb "github.com/orca-telemetry/contract/go/v2"
	db "github.com/orca-telemetry/core/internal/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	CoreServer struct {
		pb.UnimplementedCoreServer
		db *db.DB
	}
)

var (
	// how many times should we retry generating a random key that
	// clashes with unique constraint in DB?
	RANDOM_KEY_MAX_RETRIES = 5
)

// performs a retry of a DB query when a unique violation is reached
// useful for when random keys are being generated
func retryOnUniqueViolation[T any](attempts int, fn func() (T, error)) (T, error) {
	var zero T
	for range attempts {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != db.PgUniqueViolation {
			return zero, status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
	}
	return zero, status.Error(codes.ResourceExhausted, "max retries exceeded on unique violation")
}

// TODO: Pick out the access code from within here.
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
func (c *CoreServer) RegisterWorker(ctx context.Context, w *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		slog.Error("could not start a transaction with the DB", "error", err)
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}
	defer tx.Rollback(ctx)
	qtx := c.db.WithTx(tx)

	// validate the key
	publicKey := w.GetPublicKey()
	if len(publicKey) == ed25519.PublicKeySize {
		return nil, status.Error(codes.InvalidArgument, db.ErrBadPublicKey)
	}

	workerUUID, err := qtx.RegisterWorker(ctx, db.RegisterWorkerParams{
		PublicKey: publicKey,
	})

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == db.PgUniqueViolation {
			return nil, status.Error(codes.AlreadyExists, db.ErrWorkerAlreadyExists)
		}
		slog.Error("failed to register worker", "error", err)
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}

	if err := tx.Commit(ctx); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		return nil, status.Error(codes.Internal, db.ErrServer(err))
	}

	return &pb.RegisterWorkerResponse{WorkerId: workerUUID.String()}, nil
}

// starts an authentication flow
func (c *CoreServer) GetNonce(ctx context.Context, n *pb.GetNonceRequest) (*pb.GetNonceResponse, error) {
	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		slog.Error("could not start a transaction with the DB", "error", err)
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}
	defer tx.Rollback(ctx)
	qtx := c.db.WithTx(tx)

	var workerId pgtype.UUID
	if err = workerId.Scan(n.GetWorkerId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, db.ErrBadWorkerId)
	}

	exists, err := qtx.CheckWorkerExistsById(ctx, workerId)
	if err != nil {
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}
	if !exists {
		return nil, status.Error(codes.NotFound, db.ErrWorkerNotFound)
	}

	nonceRow, err := retryOnUniqueViolation(RANDOM_KEY_MAX_RETRIES, func() (db.CreateNonceRow, error) {
		nonce := make([]byte, 32)
		if _, err = rand.Read(nonce); err != nil {
			return db.CreateNonceRow{}, status.Error(codes.Internal, db.ErrServer(err))
		}
		return qtx.CreateNonce(ctx, db.CreateNonceParams{
			WorkerID: workerId,
			Nonce:    nonce,
		})
	})

	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}

	return &pb.GetNonceResponse{Challenge: nonceRow.Nonce}, nil
}

// checks the user signed nonce and issues a access token
func (c *CoreServer) CheckNonce(ctx context.Context, n *pb.CheckNonceRequest) (*pb.CheckNonceResponse, error) {
	// validate inputs up front before touching the DB
	var nonceId pgtype.UUID
	if err := nonceId.Scan(n.GetNonceId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, db.ErrBadNonceId)
	}

	var workerId pgtype.UUID
	if err := workerId.Scan(n.GetWorkerId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, db.ErrBadWorkerId)
	}

	tx, err := c.db.BeginTx(ctx)
	if err != nil {
		slog.Error("could not start transaction", "error", err)
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}
	defer tx.Rollback(ctx)
	qtx := c.db.WithTx(tx)

	worker, err := qtx.GetWorkerByID(ctx, workerId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, db.ErrWorkerNotFound)
		}
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}

	// atomic action to get the nonce then immediately consume it is safer
	// prevents replay attacks
	consumed, err := qtx.ConsumeNonce(ctx, db.ConsumeNonceParams{
		WorkerID: workerId,
		NonceID:  nonceId,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, db.ErrNonceNotFound)
		}
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}

	if !ed25519.Verify(worker.PublicKey, consumed.Nonce, n.GetSignedChallenge()) {
		return nil, status.Error(codes.Unauthenticated, db.ErrBadSignature)
	}

	// issue access key
	sessionRow, err := retryOnUniqueViolation(RANDOM_KEY_MAX_RETRIES, func() (db.CreateSessionRow, error) {
		accessKey := make([]byte, 32)
		if _, err = rand.Read(accessKey); err != nil {
			return db.CreateSessionRow{}, status.Error(codes.Internal, db.ErrServer(err))
		}
		return qtx.CreateSession(ctx, db.CreateSessionParams{
			WorkerID:  workerId,
			AccessKey: accessKey,
		})
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
	}

	return &pb.CheckNonceResponse{AccessKey: sessionRow.AccessKey, ExpiresAt: timestamppb.New(
		sessionRow.ExpiresAt.Time,
	)}, nil
}
