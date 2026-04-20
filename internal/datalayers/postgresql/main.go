package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	pb "github.com/orca-telemetry/contract/go"
	"github.com/orca-telemetry/core/internal/dag"
)

// RegisterProcessor with Orca Core
func (d *Datalayer) RegisterProcessor(
	ctx context.Context,
	proc *pb.ProcessorRegistration,
) error {
	slog.Debug("registering processor", "processor", proc)

	tx, err := d.WithTx(ctx)

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err != nil {
		slog.Error("could not start a transaction", "error", err)
		return err
	}

	// register the processor
	err = d.createProcessor(ctx, tx, proc)

	if err != nil {
		slog.Error("could not create processor", "error", err)
		return err
	}

	// add all algorithms first
	for _, algo := range proc.GetSupportedAlgorithms() {
		// add window types
		windowType := algo.GetWindowType()

		// create / update the window type
		windowTypeId, err := d.createWindowType(ctx, tx, windowType)
		if err != nil {
			return err
		}

		// read any existing metadata fields for the window
		metadataFieldsAsStored, err := d.readMetadataFieldsByWindowType(ctx, tx, windowType)
		if err != nil {
			return err
		}

		// if there are existing fields, check they are the same as the provided window
		// just check on metadatafield name
		if len(metadataFieldsAsStored) > 0 {
			if len(windowType.MetadataFields) != len(metadataFieldsAsStored) {
				return fmt.Errorf(
					`Metadata fields of incoming window type %v, do not match the
					number of fields stored in the database for this window.
					Expected: %v, got %v. Considering bumping the version of the
					window type.`, windowType, metadataFieldsAsStored, windowType.MetadataFields,
				)
			}
			metadataFieldNamesAsStored := make([]string, len(metadataFieldsAsStored))
			for ii, field := range metadataFieldsAsStored {
				metadataFieldNamesAsStored[ii] = field.GetName()
			}
			for _, metadataField := range windowType.MetadataFields {
				if !slices.Contains(metadataFieldNamesAsStored, metadataField.GetName()) {
					return fmt.Errorf(
						`Recieved a metadata field %v of window type %v that is not registered
						in the database. If you want to keep this field, bump the version
						of the window type.`, metadataField.GetName(), windowType,
					)
				}
			}
		} else {
			var metadataFieldIds []int64
			for _, metadataField := range windowType.MetadataFields {
				metadataFieldId, err := d.createMetadataField(ctx, tx, metadataField)
				if err != nil {
					return fmt.Errorf("sql issue creating the metadata field: %v", err)
				}

				err = d.createMetadataFieldBridge(ctx, tx, windowTypeId, metadataFieldId)
				if err != nil {
					return fmt.Errorf("sql issue in creating the metadata field bridge: %v", err)
				}
				metadataFieldIds = append(metadataFieldIds, metadataFieldId)
			}
		}

		// create algos
		err = d.addAlgorithm(ctx, tx, algo, proc)
		if err != nil {
			slog.Error("error creating algorithm", "error", err)
			return err
		}
	}

	// then add the dependencies and associate the processor with all the algos
	for _, algo := range proc.GetSupportedAlgorithms() {
		err := d.addOverwriteAlgorithmDependency(
			ctx,
			tx,
			algo,
			proc,
		)
		if err != nil {
			// error wrapping is important here because we return some custom errors
			return fmt.Errorf("issue adding algorithm dependency: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// EmitWindow with Orca core
func (d *Datalayer) EmitWindow(
	ctx context.Context,
	window *pb.Window,
) (pb.WindowEmitStatus, error) {
	slog.Debug("recieved emitted window", "window", window)

	tx, err := d.WithTx(ctx)

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err != nil {
		slog.Error("could not start a transaction", "error", err)
		return pb.WindowEmitStatus{}, err
	}

	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)

	// marshal metadata
	metadata := window.GetMetadata()
	metadataBytes, err := metadata.MarshalJSON()
	if err != nil {
		return pb.WindowEmitStatus{}, fmt.Errorf("could not marshal metadata: %v", err)
	}

	// check whether metadata is needed
	metadataFields, err := qtx.ReadMetadataFieldsByWindowType(ctx, ReadMetadataFieldsByWindowTypeParams{
		WindowTypeName:    window.GetWindowTypeName(),
		WindowTypeVersion: window.GetWindowTypeVersion(),
	})
	if err != nil {
		return pb.WindowEmitStatus{}, fmt.Errorf("could not read metadata for window: %v", err)
	}

	// confident that any required metadata is being supplied to the processor
	if len(metadataFields) > 0 {
		var metadataMap map[string]any
		if err := json.Unmarshal(metadataBytes, &metadataMap); err != nil {
			return pb.WindowEmitStatus{}, fmt.Errorf("could not unmarshal metadata for validation: %v", err)
		}

		for _, mDataField := range metadataFields {
			fieldName := mDataField.MetadataFieldName
			if _, exists := metadataMap[fieldName]; !exists {
				return pb.WindowEmitStatus{}, fmt.Errorf("required metadata field '%s' is missing", fieldName)
			}
		}
	}

	insertedWindow, err := qtx.RegisterWindow(ctx, RegisterWindowParams{
		WindowTypeName:    window.GetWindowTypeName(),
		WindowTypeVersion: window.GetWindowTypeVersion(),
		TimeFrom: pgtype.Timestamp{
			Time:  window.GetTimeFrom().AsTime().UTC(),
			Valid: true,
		},
		TimeTo: pgtype.Timestamp{
			Time:  window.GetTimeTo().AsTime().UTC(),
			Valid: true,
		},
		Origin:   window.GetOrigin(),
		Metadata: metadataBytes,
	})
	if err != nil {
		slog.Error("could not insert window", "error", err)
		if strings.Contains(err.Error(), "(SQLSTATE 23503)") {
			return pb.WindowEmitStatus{}, fmt.Errorf(
				"window type does not exist - insert via window type registration: %v",
				err.Error(),
			)
		}
	}
	slog.Debug("window record inserted into the datalayer", "window", insertedWindow)
	execPaths, err := qtx.ReadAlgorithmExecutionPaths(
		ctx,
		strconv.Itoa(int(insertedWindow.WindowTypeID)),
	)
	if err != nil {
		slog.Error(
			"could not read execution paths for window id",
			"window_id",
			insertedWindow,
			"error",
			err,
		)
		return pb.WindowEmitStatus{Status: pb.WindowEmitStatus_TRIGGERING_FAILED}, err
	}

	// create the algo path args
	var (
		algoIDPaths       []string
		windowTypeIDPaths []string
		procIDPaths       []string
		// lookbacks
		lookbackCounts         []string
		lookbackTimedeltas     []string
		selfLookbackCounts     []string
		selfLookbackTimedeltas []string
		// lookback gaps
		lookbackGapCounts         []string
		lookbackGapTimedeltas     []string
		selfLookbackGapCounts     []string
		selfLookbackGapTimedeltas []string
	)
	for _, path := range execPaths {
		algoIDPaths = append(algoIDPaths, path.AlgoIDPath)
		windowTypeIDPaths = append(windowTypeIDPaths, path.WindowTypeIDPath)
		procIDPaths = append(procIDPaths, path.ProcIDPath)

		lookbackCounts = append(lookbackCounts, path.LookbackCountPath)
		lookbackTimedeltas = append(lookbackTimedeltas, path.LookbackTimedeltaPath)
		selfLookbackCounts = append(selfLookbackCounts, path.SelfLookbackCountPath)
		selfLookbackTimedeltas = append(selfLookbackTimedeltas, path.SelfLookbackTimedeltaPath)

		lookbackGapCounts = append(lookbackGapCounts, path.LookbackCountPath)
		lookbackGapTimedeltas = append(lookbackGapTimedeltas, path.LookbackTimedeltaPath)
		selfLookbackGapCounts = append(selfLookbackGapCounts, path.SelfLookbackCountPath)
		selfLookbackGapTimedeltas = append(selfLookbackGapTimedeltas, path.SelfLookbackTimedeltaPath)
	}

	// fire off processings
	executionPlan, err := dag.BuildPlan(
		algoIDPaths,
		windowTypeIDPaths,
		procIDPaths,
		lookbackCounts,
		lookbackTimedeltas,
		lookbackGapCounts,
		lookbackGapTimedeltas,
		selfLookbackCounts,
		selfLookbackTimedeltas,
		selfLookbackGapCounts,
		selfLookbackGapTimedeltas,
		int64(insertedWindow.WindowTypeID),
	)
	if err != nil {
		slog.Error(
			"failed to construct execution paths for window",
			"window",
			insertedWindow,
			"error",
			err,
		)
		return pb.WindowEmitStatus{Status: pb.WindowEmitStatus_TRIGGERING_FAILED}, err
	}

	if len(executionPlan.Stages) > 0 {
		go processTasks(d, executionPlan, window, insertedWindow)

		return pb.WindowEmitStatus{
			Status: pb.WindowEmitStatus_PROCESSING_TRIGGERED,
		}, tx.Commit(ctx)
	}
	return pb.WindowEmitStatus{
		Status: pb.WindowEmitStatus_NO_TRIGGERED_ALGORITHMS,
	}, nil
}

func (d *Datalayer) Expose(
	ctx context.Context,
	settings *pb.ExposeSettings,
) (*pb.InternalState, error) {
	// settings not handled for now

	tx, err := d.WithTx(ctx)

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err != nil {
		slog.Error("could not start a transaction", "error", err)
		return nil, err
	}

	pgTx := tx.(*PgTx)

	qtx := d.queries.WithTx(pgTx.tx)
	var processors []Processor
	if len(settings.ExcludeProject) > 0 {
		processors, err = qtx.ReadProcessorExcludeProject(ctx, pgtype.Text{
			String: settings.ExcludeProject,
			Valid:  true,
		})
	} else {
		// read all the processors
		processors, err = qtx.ReadProcessors(ctx)
	}
	if err != nil {
		slog.Error("could not read algorithms", "error", err)
		return nil, fmt.Errorf("could not read algorithms: %w", err)
	}

	// read all the algorithms
	algorithms, err := qtx.ReadAlgorithms(ctx)
	if err != nil {
		slog.Error("could not read algorithms", "error", err)
		return nil, fmt.Errorf("could not read algorithms: %w", err)
	}

	algosMap := make(map[int]Algorithm, len(algorithms))
	for ii, algo := range algorithms {
		// pack 'em into the map
		algosMap[ii] = algo
	}

	// read all the metadata fields
	mdf, err := qtx.ReadMetadataFields(ctx)
	if err != nil {
		slog.Error("could not read metadata fields", "error", err)
		return nil, fmt.Errorf("could not read metadata fields: %w", err)
	}
	mdfsMap := make(map[int64]MetadataField, len(mdf))
	for _, mdf := range mdf {
		mdfsMap[mdf.ID] = mdf
	}

	// read all the metadata fields for window types from bridge table
	wtmdf, err := qtx.ReadWindowTypeMetadataFields(ctx)
	if err != nil {
		slog.Error("could not read window type metadata fields", "error", err)
		return nil, fmt.Errorf("could not read window type metadata fields: %w", err)
	}
	wtToMdf := make(map[string][]*pb.MetadataField)
	for _, wtmd := range wtmdf {
		_key := fmt.Sprintf("%v_%v", wtmd.WindowTypeName, wtmd.WindowTypeVersion)

		wtToMdf[_key] = append(wtToMdf[_key], &pb.MetadataField{
			Name:        wtmd.MetadataFieldName,
			Description: wtmd.MetadataFieldDescription,
		})
	}

	// read all the window types
	wts, err := qtx.ReadWindowTypes(ctx)
	if err != nil {
		slog.Error("could not read window types", "error", err)
		return nil, fmt.Errorf("could not read window types: %w", err)
	}
	wtsMap := make(map[int64]*pb.WindowType, len(wts))
	for _, wt := range wts {
		metadataFields, ok := wtToMdf[fmt.Sprintf("%v_%v", wt.Name, wt.Version)]
		if !ok {
			slog.Info("no metadata fields found for window type", "windowType", wt)
		}
		wtsMap[wt.ID] = &pb.WindowType{
			Name:           wt.Name,
			Version:        wt.Version,
			Description:    wt.Description,
			MetadataFields: metadataFields,
		}
	}

	algosForProcessor := make(map[int64][]*pb.Algorithm)
	for _, algo := range algorithms {
		// get the window type for this algorithm
		wt, ok := wtsMap[algo.WindowTypeID]
		if !ok {
			slog.Error("could not find the window type id, which algorithm depends on", "window_type_id", algo.WindowTypeID, "algorithm_id", algo.ID)
			return nil, fmt.Errorf("could not find the window type that algorithm %v, depends on", algo.Name)
		}
		// parse out the result type
		var resultType pb.ResultType
		switch algo.ResultType {
		case ResultTypeStruct:
			resultType = pb.ResultType_STRUCT
		case ResultTypeValue:
			resultType = pb.ResultType_VALUE
		case ResultTypeNone:
			resultType = pb.ResultType_NONE
		case ResultTypeArray:
			resultType = pb.ResultType_ARRAY
		default:
			resultType = pb.ResultType_NOT_SPECIFIED
		}

		algosForProcessor[algo.ProcessorID] = append(algosForProcessor[algo.ProcessorID], &pb.Algorithm{
			Name:        algo.Name,
			Version:     algo.Version,
			WindowType:  wt,
			ResultType:  resultType,
			Description: algo.Description,
		},
		)
	}

	processorsPb := make([]*pb.ProcessorRegistration, len(processors))

	for ll, p := range processors {
		algos, ok := algosForProcessor[p.ID]
		if !ok {
			slog.Error("could not find algorithms for processor", "processorId", p.ID)
			return nil, fmt.Errorf("could not find algorithms for processor ID: %v", p.ID)

		}
		processorsPb[ll] = &pb.ProcessorRegistration{
			Name:                p.Name,
			Runtime:             p.Runtime,
			SupportedAlgorithms: algos,
		}
	}

	slog.Debug("exposed state", "processors", processorsPb)
	return &pb.InternalState{
		Processors: processorsPb,
	}, nil

}
