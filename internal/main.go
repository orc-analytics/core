package internal

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
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

	// key used to access session details from context
	ctxKey struct{}
)

const (
	// how many times should we retry generating a random key that
	// clashes with unique constraint in DB?
	RANDOM_KEY_MAX_RETRIES = 5

	// number of times we should retry a transaction
	TRANSACTION_MAX_RETRIES = 5
)

// captures postgres transaction errors (due to contention) where a retry is
// worth it
func isTxRetryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		(pgErr.Code == db.PgSerializationFailure || pgErr.Code == db.PgDeadlock)
}

// performs a retry of a DB query when a unique violation is reached
// useful for when random keys are being generated
func retryOnUniqueViolation[T any](attempts int, fn func() (T, error)) (T, error) {
	var zero T
	for range attempts {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		if isTxRetryable(err) {
			// return anyway - we need a transaction level retry
			return zero, err
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != db.PgUniqueViolation {
			return zero, status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
	}
	return zero, status.Error(codes.ResourceExhausted, "max retries exceeded on unique violation")
}

// c.db.WithTx but with retries in the event of a contentions query or commit.
func (c *CoreServer) txWithRetry(ctx context.Context, fn func(qtx *db.Queries) error) error {
	for range TRANSACTION_MAX_RETRIES {
		tx, err := c.db.BeginTx(ctx)
		if err != nil {
			return err // don't retry. begin itself failed
		}

		if err = fn(c.db.WithTx(tx)); err != nil {
			tx.Rollback(ctx)
			if isTxRetryable(err) {
				continue
			}
			return err
		}

		if err = tx.Commit(ctx); err != nil {
			tx.Rollback(ctx)
			if isTxRetryable(err) {
				// serialisation failure  can surface at commit
				// so we check here also for a retry
				continue
			}
			return err
		}
		return nil
	}
	return status.Error(codes.Unavailable, "transaction failed after max retries")
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

type transistivePair struct {
	from string
	to   string
}

// --------------------------- Interceptors ---------------------------
type ctxSession struct {
	workerId  pgtype.UUID
	expiresAt time.Time
}

// checks for an access key in the Bearer token
func (c *CoreServer) AuthInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// allow unathenticated access to these endpoints
	// required for setting up the auth flow
	var publicMethods = map[string]struct{}{
		"/Core/RegisterWorker": {},
		"/Core/GetNonce":       {},
		"/Core/CheckNonce":     {},
	}

	if _, ok := publicMethods[info.FullMethod]; ok {
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

	// Bearer <token>
	authHeader := vals[0]
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, status.Error(codes.Unauthenticated, "authorization header must be Bearer token")
	}
	accessKey, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader, "Bearer "))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "access key needs to be base64 encoded")
	}

	// validate token
	sessionRow, err := c.db.Query().GetSession(ctx, accessKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.Unauthenticated, "access key not found")
		} else {
			return nil, status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
	}
	if sessionRow.ExpiresAt.Time.Before(time.Now()) {
		return nil, status.Error(codes.Unauthenticated, "access key expired")
	}

	// get the worker ID
	var workerId pgtype.UUID
	if err := workerId.Scan(sessionRow.WorkerID.String()); err != nil {
		return nil, status.Error(codes.Internal, db.ErrBadWorkerId)
	}

	// add claims
	session := ctxSession{
		workerId:  workerId,
		expiresAt: sessionRow.ExpiresAt.Time}
	ctx = context.WithValue(ctx, ctxKey{}, session)

	return handler(ctx, req)
}
func SessionFromCtx(ctx context.Context) (ctxSession, bool) {
	v, ok := ctx.Value(ctxKey{}).(ctxSession)
	return v, ok
}

// --------------------------- gRPC Services ---------------------------
// Register a worker, returns worker ID or an error on public key conflict
// public endpoint
func (c *CoreServer) RegisterWorker(ctx context.Context, w *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	// validate the key
	publicKey := w.GetPublicKey()
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, status.Error(codes.InvalidArgument, db.ErrBadPublicKey)
	}
	var workerUUID pgtype.UUID
	err := c.txWithRetry(ctx, func(qtx *db.Queries) error {
		var err error
		workerUUID, err = qtx.RegisterWorker(ctx, db.RegisterWorkerParams{
			PublicKey: publicKey,
		})

		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == db.PgUniqueViolation {
				return status.Error(codes.AlreadyExists, db.ErrWorkerAlreadyExists)
			}
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterWorkerResponse{WorkerId: workerUUID.String()}, nil
}

// starts an authentication flow
// public endpoint
func (c *CoreServer) GetNonce(ctx context.Context, n *pb.GetNonceRequest) (*pb.GetNonceResponse, error) {
	var workerId pgtype.UUID
	if err := workerId.Scan(n.GetWorkerId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, db.ErrBadWorkerId)
	}
	var nonceRow db.CreateNonceRow
	err := c.txWithRetry(ctx, func(qtx *db.Queries) error {
		exists, err := qtx.CheckWorkerExistsById(ctx, workerId)
		if err != nil {
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
		if !exists {
			return status.Error(codes.NotFound, db.ErrWorkerNotFound)
		}

		nonceRow, err = retryOnUniqueViolation(RANDOM_KEY_MAX_RETRIES, func() (db.CreateNonceRow, error) {
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
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &pb.GetNonceResponse{Challenge: nonceRow.Nonce, NonceId: nonceRow.ID.String()}, nil
}

// checks the user signed nonce and issues a access token
// public endpoint
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
	var sessionRow db.CreateSessionRow
	err := c.txWithRetry(ctx, func(qtx *db.Queries) error {
		worker, err := qtx.GetWorkerByID(ctx, workerId)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return status.Error(codes.NotFound, db.ErrWorkerNotFound)
			}
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}

		// atomic action to get the nonce then immediately consume it is safer
		// prevents replay attacks
		consumed, err := qtx.ConsumeNonce(ctx, db.ConsumeNonceParams{
			WorkerID: workerId,
			NonceID:  nonceId,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return status.Error(codes.NotFound, db.ErrNonceNotFound)
			}
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}

		if !ed25519.Verify(worker.PublicKey, consumed.Nonce, n.GetSignedChallenge()) {
			return status.Error(codes.Unauthenticated, db.ErrBadSignature)
		}

		// issue access key
		sessionRow, err = retryOnUniqueViolation(RANDOM_KEY_MAX_RETRIES, func() (db.CreateSessionRow, error) {
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
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &pb.CheckNonceResponse{
		AccessKey: sessionRow.AccessKey,
		ExpiresAt: timestamppb.New(
			sessionRow.ExpiresAt.Time,
		)}, nil
}

func (c *CoreServer) RegisterWorkerSnapshot(ctx context.Context, r *pb.RegisterWorkerSnapshotRequest) (*pb.RegisterWorkerSnapshotResponse, error) {
	// get session
	session, ok := SessionFromCtx(ctx)
	if !ok { // should never reach
		return nil, status.Error(codes.Unauthenticated, "no active session")
	}

	err := c.txWithRetry(ctx, func(qtx *db.Queries) error {
		// insert data functions
		for _, df := range r.DataFunctions {
			// validate that we can marshal the input and output models
			var inputModel *jsonschema.Schema
			err := json.Unmarshal(df.GetInputModel(), inputModel)
			if err != nil {
				return status.Error(
					codes.InvalidArgument,
					db.ErrBadArgument(fmt.Sprintf("input model to data function %s not a valid json-schema", df.GetName())),
				)
			}
			var outputModel *jsonschema.Schema
			err = json.Unmarshal(df.GetOutputModel(), outputModel)
			if err != nil {
				return status.Error(
					codes.InvalidArgument,
					db.ErrBadArgument(fmt.Sprintf("output model to data function %s not a valid json-schema", df.GetName())),
				)
			}

			dfSettings := df.GetSettings()

			err = qtx.CreateDataFunction(ctx, db.CreateDataFunctionParams{
				Name:                    df.GetName(),
				GitCommitHash:           df.GetGitCommitHash(),
				WorkerID:                session.workerId,
				OutputModel:             df.GetOutputModel(),
				InputModel:              df.GetOutputModel(),
				Status:                  db.AssetStatusPending,
				ExecutionTimeoutSeconds: pgtype.Int4{Int32: dfSettings.GetTimeout(), Valid: true},
				TtlSeconds:              pgtype.Int4{Int32: dfSettings.GetTimeout(), Valid: true},
			})
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == db.PgUniqueViolation {
				switch pgErr.ConstraintName {
				case "data_function_pKey":
					// if there is a primary key violation - do nothing. this exact data
					// function already exists

				case "one_active_owner_per_data_function":
					// raise an error - we are trying to add a data function with the same name
					// as another active function
					return status.Error(codes.AlreadyExists, db.ErrAlreadyExists(fmt.Sprintf("a data function with name (%s) already exists and is either pending or active", df.GetName())))
				}
			}
			if err != nil {
				return status.Error(codes.Unavailable, db.ErrDatabase(err))
			}
		}
		// insert tasks
		for _, task := range r.GetTasks() {
			// validate the input and output models
			var inputModel *jsonschema.Schema
			err := json.Unmarshal(task.GetInputModel(), inputModel)
			if err != nil {
				return status.Error(
					codes.InvalidArgument,
					db.ErrBadArgument(fmt.Sprintf("input model to task %s not a valid json-schema", task.GetName())),
				)
			}
			var outputModel *jsonschema.Schema
			err = json.Unmarshal(task.GetOutputModel(), outputModel)
			if err != nil {
				return status.Error(
					codes.InvalidArgument,
					db.ErrBadArgument(fmt.Sprintf("output model to task %s not a valid json-schema", task.GetName())),
				)
			}

			taskSettings := task.GetExecutionSettings()

			// backoff strategy
			var backoffStrategy db.BackoffStrategy
			switch taskSettings.GetBackoffStrategy() {
			case pb.BackoffStrategy_BACKOFF_STRATEGY_LINEAR:
				backoffStrategy = db.BackoffStrategyBACKOFFSTRATEGYLINEAR
			case pb.BackoffStrategy_BACKOFF_STRATEGY_EXPONENTIAL:
				backoffStrategy = db.BackoffStrategyBACKOFFSTRATEGYEXPONENTIAL
			default:
				backoffStrategy = db.BackoffStrategyBACKOFFSTRATEGYLINEAR
			}

			err = qtx.CreateTask(ctx, db.CreateTaskParams{
				Name:        task.GetName(),
				WorkerID:    session.workerId,
				Description: task.GetDescription(),
				ExecutionTimeout: pgtype.Int4{
					Int32: taskSettings.GetExecutionTimeout(),
					Valid: true,
				},
				Deadline: pgtype.Int4{
					Int32: taskSettings.GetDeadline(),
					Valid: true,
				},
				RetryCount: pgtype.Int4{
					Int32: taskSettings.GetRetryCount(),
					Valid: true,
				},
				BackoffStrategy: db.NullBackoffStrategy{
					BackoffStrategy: backoffStrategy,
					Valid:           true,
				},
				InputModel:    task.InputModel,
				OutputModel:   task.OutputModel,
				GitCommitHash: task.GitCommitHash,
				Status:        db.AssetStatusPending,
			})
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == db.PgUniqueViolation {
				switch pgErr.ConstraintName {
				case "task_pKey":
					// if there is a primary key violation - do nothing. this exact task
					// already exists

				case "one_active_owner_per_task":
					// raise an error - we are trying to add a task with the same name
					// as another in an active or pending state
					return status.Error(codes.AlreadyExists, db.ErrAlreadyExists(fmt.Sprintf("a task with name (%s) already exists and is either pending or active", task.GetName())))
				}
			}
			if err != nil {
				return status.Error(codes.Unavailable, db.ErrDatabase(err))
			}

			// add in the data functions the task requires
			for _, df := range task.GetRequiredDataFunctions() {
				// get the worker ID
				var dfWorkerId pgtype.UUID
				if err := dfWorkerId.Scan(df.GetDfWorkerId()); err != nil {
					return status.Error(codes.InvalidArgument, db.ErrBadWorkerId)
				}
				err := qtx.RequireDatafunctionForTask(ctx, db.RequireDatafunctionForTaskParams{
					TaskName:          task.GetName(),
					TaskGitCommitHash: task.GetGitCommitHash(),
					TaskWorkerID:      session.workerId,
					DfName:            df.GetDfName(),
					DfGitCommitHash:   df.GetDfGitCommitHash(),
					DfWorkerID:        dfWorkerId,
				})

				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) && pgErr.Code != db.PgUniqueViolation && pgErr.ConstraintName != "task_required_data_function_pKey" {
					// return if the error is not the accepted kind (this row already exists)
					return status.Error(codes.Unavailable, db.ErrDatabase(err))
				}
			}
		}
		// register all workflows
		for _, workflow := range r.GetWorkflows() {
			// validate the input model
			var inputModel *jsonschema.Schema
			err := json.Unmarshal(workflow.GetInputModel(), inputModel)
			if err != nil {
				return status.Error(
					codes.InvalidArgument,
					db.ErrBadArgument(fmt.Sprintf("input model to workflow %s not a valid json-schema", workflow.GetWorkflowName())),
				)
			}

			var workflowSource db.WorkflowSource
			switch workflow.GetWorkflowSource() {
			case pb.WorkflowSource_UNDEFINED:
				workflowSource = db.WorkflowSourceWORKFLOWSOURCEUNDEFINED
			case pb.WorkflowSource_WORKER:
				workflowSource = db.WorkflowSourceWORKFLOWSOURCEWORKER
			default:
				workflowSource = db.WorkflowSourceWORKFLOWSOURCEUNDEFINED
			}

			executionSettings := workflow.GetExecutionSettings()

			workflowId, err := qtx.CreateWorkflow(ctx, db.CreateWorkflowParams{
				Name:     workflow.GetWorkflowName(),
				Hash:     workflow.GetWorkflowHash(),
				WorkerID: session.workerId,
				Source:   workflowSource,
				Description: pgtype.Text{
					Valid:  true,
					String: workflow.GetDescription(),
				},
				InputModel: workflow.GetInputModel(),
				TaskConcurrencyLimit: pgtype.Int4{
					Int32: executionSettings.GetConcurrencyLimit(),
					Valid: true,
				},
				HaltOnFailure: pgtype.Bool{
					Bool:  executionSettings.GetHaltOnFailure(),
					Valid: true,
				},
				GitCommitHash: workflow.GetGitCommitHash(),
				Status:        db.AssetStatusPending,
			})

			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == db.PgUniqueViolation {
				switch pgErr.ConstraintName {
				case "workflow_pKey":
				// this workflow already exists - do nothing
				case "one_active_owner_per_workflow":
					// this workflow name is taken
					return status.Error(codes.AlreadyExists, db.ErrAlreadyExists(fmt.Sprintf("a workflow with name (%s) already exists and is either pending or active", workflow.GetWorkflowName())))
				}
			}
			if err != nil {
				return status.Error(codes.Unavailable, db.ErrDatabase(err))
			}

			// list of all edges, ready to calculate transitive pairs
			allEdges := make([][2]transitiveEdge, 0)

			// add workflow edges
			for _, edge := range workflow.GetEdges() {
				var fromTaskWorkerUUID pgtype.UUID
				var toTaskWorkerUUID pgtype.UUID

				if err := fromTaskWorkerUUID.Scan(edge.GetFromTaskWorkerId()); err != nil {
					return status.Error(codes.Internal, db.ErrBadWorkerId)
				}
				if err := toTaskWorkerUUID.Scan(edge.GetToTaskWorkerId()); err != nil {
					return status.Error(codes.Internal, db.ErrBadWorkerId)
				}
				// add the edge to the transistive pairs
				fromUnique := fmt.Sprintf("%s_%s_%s", edge.GetFromTaskName(), edge.GetFromTaskGitCommitHash(), edge.GetFromTaskWorkerId())
				toUnique := fmt.Sprintf("%s_%s_%s", edge.GetToTaskName(), edge.GetToTaskGitCommitHash(), edge.GetToTaskWorkerId())

				allEdges = append(allEdges, [2]transitiveEdge{transitiveEdge{
					uniqueName: fromUnique,
					name:       edge.GetFromTaskName(),
					hash:       edge.GetFromTaskGitCommitHash(),
					workerId:   fromTaskWorkerUUID,
				}, transitiveEdge{
					uniqueName: toUnique,
					name:       edge.GetToTaskName(),
					hash:       edge.GetToTaskGitCommitHash(),
					workerId:   toTaskWorkerUUID,
				}})

				err := qtx.CreateWorkflowEdge(ctx, db.CreateWorkflowEdgeParams{
					WorkflowID:            workflowId,
					FromTaskName:          edge.GetFromTaskName(),
					ToTaskName:            edge.GetToTaskName(),
					FromTaskWorkerID:      fromTaskWorkerUUID,
					ToTaskWorkerID:        toTaskWorkerUUID,
					FromTaskGitCommitHash: edge.GetFromTaskGitCommitHash(),
					ToTaskGitCommitHash:   edge.GetToTaskGitCommitHash(),
				})
				if err != nil {
					return status.Error(codes.Unavailable, db.ErrDatabase(err))
				}
			}
			// go through all the edges and pairs a
			pairs := transitivePairs(allEdges, func(e transitiveEdge) string { return e.uniqueName })

			for _, pair := range pairs {
				// add workflow transistive pairs - let the DB enforce cycle detection
				err := qtx.CreateWorkflowTransitivepair(ctx, db.CreateWorkflowTransitivepairParams{
					WorkflowID:            workflowId,
					FromTaskName:          pair[0].name,
					ToTaskName:            pair[1].name,
					FromTaskGitCommitHash: pair[0].hash,
					ToTaskGitCommitHash:   pair[1].hash,
					FromTaskWorkerID:      pair[0].workerId,
					ToTaskWorkerID:        pair[1].workerId,
				})

				// check whether the error is a cycle
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == db.PgCycleDetected {
					return status.Error(codes.InvalidArgument, fmt.Sprintf("cycle detected between task %v and %v", pair[0].name, pair[1].name))
				}
				if errors.As(err, &pgErr) && pgErr.ConstraintName == "workflow_transitive_pair_pKey" {
					// do nothing - this pair already exists
					continue
				}
				if err != nil {
					return status.Error(codes.Unavailable, db.ErrDatabase(err))
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterWorkerSnapshotResponse{}, nil
}

func (c *CoreServer) RegisterServing(ctx context.Context, r *pb.RegisterServingRequest) (*pb.RegisterServingResponse, error) {
	// modify all data functions, tasks and workflows that are in pending state for this commit hash and worker
	// to active.
	//
	// deactivate other versions
	session, ok := SessionFromCtx(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no active session")
	}
	err := c.txWithRetry(ctx, func(qtx *db.Queries) error {
		err := qtx.SetWorkerServing(ctx, db.SetWorkerServingParams{
			IsServing: true,
			ID:        session.workerId,
		})
		if err != nil {
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}

		err = qtx.SetDataFunctionsToActive(ctx, db.SetDataFunctionsToActiveParams{
			CommitHash: r.GetCommitHash(),
			WorkerID:   session.workerId,
		})
		if err != nil {
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
		err = qtx.SetTasksToActive(ctx, db.SetTasksToActiveParams{
			CommitHash: r.GetCommitHash(),
			WorkerID:   session.workerId,
		})
		if err != nil {
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
		err = qtx.SetWorkflowToActive(ctx, db.SetWorkflowToActiveParams{
			CommitHash: r.GetCommitHash(),
			WorkerID:   session.workerId,
		})
		if err != nil {
			return status.Error(codes.Unavailable, db.ErrDatabase(err))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &pb.RegisterServingResponse{}, nil
}
