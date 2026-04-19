package postgresql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/orca-telemetry/contract/go"
	types "github.com/orca-telemetry/core/internal/types"
)

type Datalayer struct {
	queries *Queries
	conn    *pgxpool.Pool
	closeFn func()
}

type PgTx struct {
	tx pgx.Tx
}

func (t *PgTx) Rollback(ctx context.Context) {
	t.tx.Rollback(ctx)
}

func (t *PgTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// generate a new client for the postgres datalayer
func NewClient(ctx context.Context, connStr string) (*Datalayer, error) {
	if connStr == "" {
		return nil, errors.New("connection string empty")
	}

	connPool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		slog.Error("Issue connecting to postgres", "error", err)
		return nil, err
	}

	return &Datalayer{
		queries: New(connPool),
		conn:    connPool,
		closeFn: connPool.Close,
	}, nil
}

func (d *Datalayer) WithTx(ctx context.Context) (types.Tx, error) {
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		slog.Error("could not start transaction", "error", err)
		return nil, err
	}
	return &PgTx{tx: tx}, nil
}

func (d *Datalayer) createProcessor(
	ctx context.Context,
	tx types.Tx,
	proc *pb.ProcessorRegistration,
) error {
	pgTx := tx.(*PgTx)

	qtx := d.queries.WithTx(pgTx.tx)

	err := qtx.CreateProcessor(ctx, CreateProcessorParams{
		Name:             proc.GetName(),
		Runtime:          proc.GetRuntime(),
		ConnectionString: proc.GetConnectionStr(),
		ProjectName:      pgtype.Text{String: proc.GetProjectName(), Valid: true},
	})
	if err != nil {
		slog.Error("could not create processor", "error", err)
		return err
	}
	return nil
}

func (d *Datalayer) createMetadataField(
	ctx context.Context,
	tx types.Tx,
	metadataField *pb.MetadataField,
) (int64, error) {
	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)
	metadataFieldId, err := qtx.CreateMetadataField(ctx, CreateMetadataFieldParams{
		Name:        metadataField.GetName(),
		Description: metadataField.GetDescription(),
	})
	if err != nil {
		slog.Error("could not create metadata field", "error", err)
		return 0, err
	}
	return metadataFieldId, nil
}

func (d *Datalayer) readMetadataFieldsByWindowType(
	ctx context.Context,
	tx types.Tx,
	windowType *pb.WindowType,
) ([]*pb.MetadataField, error) {
	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)
	metadataFields, err := qtx.ReadMetadataFieldsByWindowType(ctx, ReadMetadataFieldsByWindowTypeParams{
		WindowTypeName:    windowType.GetName(),
		WindowTypeVersion: windowType.GetVersion(),
	})
	if err != nil {
		return []*pb.MetadataField{}, fmt.Errorf("could not read metadata fields: %v", err)

	}
	metadataFieldsPb := make([]*pb.MetadataField, len(metadataFields))
	for ii, field := range metadataFields {
		metadataFieldsPb[ii] = &pb.MetadataField{
			Name:        field.MetadataFieldName,
			Description: field.MetadataFieldDescription,
		}
	}
	return metadataFieldsPb, nil
}

func (d *Datalayer) createWindowType(
	ctx context.Context,
	tx types.Tx,
	windowType *pb.WindowType,
) (int64, error) {
	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)
	windowTypeId, err := qtx.CreateWindowType(ctx, CreateWindowTypeParams{
		Name:        windowType.GetName(),
		Version:     windowType.GetVersion(),
		Description: windowType.GetDescription(),
	})
	if err != nil {
		slog.Error("could not create window type", "error", err)
		return 0, err
	}
	return windowTypeId, nil
}

func (d *Datalayer) createMetadataFieldBridge(
	ctx context.Context,
	tx types.Tx,
	windowTypeId int64,
	metadataFieldId int64,
) error {
	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)
	err := qtx.CreateWindowTypeMetadataFieldBridge(ctx, CreateWindowTypeMetadataFieldBridgeParams{
		WindowTypeID:     windowTypeId,
		MetadataFieldsID: metadataFieldId,
	})
	if err != nil {
		slog.Error("could not create metadata field bridge", "error", err)
		return err
	}
	return nil
}

func (d *Datalayer) addAlgorithm(
	ctx context.Context,
	tx types.Tx,
	algo *pb.Algorithm,
	proc *pb.ProcessorRegistration,
) error {
	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)

	// create algos
	var resultType ResultType
	switch algo.GetResultType() {
	case pb.ResultType_ARRAY:
		resultType = ResultTypeArray
	case pb.ResultType_STRUCT:
		resultType = ResultTypeStruct
	case pb.ResultType_VALUE:
		resultType = ResultTypeValue
	case pb.ResultType_NONE:
		resultType = ResultTypeNone
	default:
		return fmt.Errorf("result type %v not supported", algo.GetResultType())
	}

	params := CreateAlgorithmParams{
		Name:              algo.GetName(),
		Version:           algo.GetVersion(),
		Description:       algo.GetDescription(),
		ProcessorName:     proc.GetName(),
		ProcessorRuntime:  proc.GetRuntime(),
		WindowTypeName:    algo.GetWindowType().GetName(),
		WindowTypeVersion: algo.GetWindowType().GetVersion(),
		ResultType:        resultType,
	}

	err := qtx.CreateAlgorithm(ctx, params)
	if err != nil {
		slog.Error("error creating algorithm", "error", err)
		return err
	}
	return nil
}

func (d *Datalayer) addOverwriteAlgorithmDependency(
	ctx context.Context,
	tx types.Tx,
	algo *pb.Algorithm,
	proc *pb.ProcessorRegistration,
) error {
	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)
	// get algorithm id
	algoId, err := qtx.ReadAlgorithmId(ctx, ReadAlgorithmIdParams{
		AlgorithmName:    algo.GetName(),
		AlgorithmVersion: algo.GetVersion(),
		ProcessorName:    proc.GetName(),
		ProcessorRuntime: proc.GetRuntime(),
	})
	if err != nil {
		slog.Error("could not get algorithm ID", "algorithm", algo)
		return err
	}
	dependencies := algo.GetDependencies()
	for _, algoDependentOn := range dependencies {
		// get algorithm id
		algoDependentOnId, err := qtx.ReadAlgorithmId(ctx, ReadAlgorithmIdParams{
			AlgorithmName:    algoDependentOn.GetName(),
			AlgorithmVersion: algoDependentOn.GetVersion(),
			ProcessorName:    algoDependentOn.GetProcessorName(),
			ProcessorRuntime: algoDependentOn.GetProcessorRuntime(),
		})
		if err != nil {
			return fmt.Errorf("issue getting algorithm ID of dependant: %v", err)
		}

		// get the algo execution path
		execPaths, err := qtx.ReadAlgorithmExecutionPathsForAlgo(ctx, algoDependentOnId)
		if err != nil {
			slog.Error("could not obtain execution paths", "algorithm_id", algoDependentOnId)
			return err
		}
		for _, algoPath := range execPaths {
			algoIds := strings.Split(algoPath.AlgoIDPath, ".")
			if slices.Contains(algoIds, strconv.Itoa(int(algoId))) {
				slog.Error(
					"found circular dependency",
					"from_algo",
					algoDependentOn,
					"to_algo",
					algo,
				)
				return &types.CircularDependencyError{
					FromAlgoName:      algoDependentOn.GetName(),
					FromAlgoVersion:   algoDependentOn.GetVersion(),
					FromAlgoProcessor: algoDependentOn.GetProcessorName(),
					ToAlgoName:        algo.GetName(),
					ToAlgoVersion:     algo.GetVersion(),
					ToAlgoProcessor:   proc.GetName(),
				}
			} else {
				err = qtx.CreateAlgorithmDependency(ctx, CreateAlgorithmDependencyParams{
					FromAlgorithmName:    algoDependentOn.GetName(),
					FromAlgorithmVersion: algoDependentOn.GetVersion(),
					FromProcessorName:    algoDependentOn.GetProcessorName(),
					FromProcessorRuntime: algoDependentOn.GetProcessorRuntime(),
					ToAlgorithmName:      algo.GetName(),
					ToAlgorithmVersion:   algo.GetVersion(),
					ToProcessorName:      proc.GetName(),
					ToProcessorRuntime:   proc.GetRuntime(),
					LookbackCount:        int64(algoDependentOn.GetLookbackNum()),
					LookbackTimedelta:    int64(algoDependentOn.GetLookbackTimeDelta()),
				})
				if err != nil {
					return fmt.Errorf("issue constructing algorithm dependency: %v", err)
				}

			}

		}
	}
	return nil
}
