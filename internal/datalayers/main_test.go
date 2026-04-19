package datalayers

import (
	"context"
	"os"
	"testing"
	"time"

	pb "github.com/orca-telemetry/contract/go"
	types "github.com/orca-telemetry/core/internal/types"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	testConnStr string
	testCtx     context.Context
)

func TestMain(m *testing.M) {
	var cleanup func()
	testCtx = context.Background()
	testConnStr, cleanup = setupPgOnce(testCtx)

	// runs all tests
	code := m.Run()

	cleanup()
	os.Exit(code)
}

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

	err = MigrateDatalayer("postgresql", connStr)
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

// TestAddProcessor tests that several processors can be added
func TestAddProcessor(t *testing.T) {

	// start the mock OrcaProcessor gRPC server
	mockProcessor_1, mockListener_1, err := StartMockOrcaProcessor(0) // set port to 0 to get random available port
	mockProcessor_2, mockListener_2, err := StartMockOrcaProcessor(0) // set port to 0 to get random available port
	assert.NoError(t, err)

	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond) // some time for processing to complete
		mockProcessor_1.GracefulStop()
		mockListener_1.Close()
		mockProcessor_2.GracefulStop()
		mockListener_2.Close()
	})

	// get the actual address the mock server is listening on
	processorConnStr_1 := mockListener_1.Addr().String()
	processorConnStr_2 := mockListener_2.Addr().String()

	// TODO: paramaterise if we have more datalayers (e.g. MySQL, SQLite) - high level function should be the same between them
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	asset_id := pb.MetadataField{Name: "asset_id", Description: "Unique ID of the asset"}
	fleet_id := pb.MetadataField{Name: "fleet_id", Description: "Unique ID of the fleet"}

	windowType := pb.WindowType{
		Name:    "TestWindow",
		Version: "1.0.0",
		MetadataFields: []*pb.MetadataField{
			&asset_id,
			&fleet_id,
		},
	}

	algo_1 := pb.Algorithm{
		Name:       "TestAlgorithm1",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	algo_2 := pb.Algorithm{
		Name:       "TestAlgorithm2",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	proc_1 := pb.ProcessorRegistration{
		Name:                "TestProcessor1",
		Runtime:             "Test",
		ProjectName:         "Test",
		ConnectionStr:       processorConnStr_1,
		SupportedAlgorithms: []*pb.Algorithm{&algo_1},
	}

	proc_2 := pb.ProcessorRegistration{
		Name:                "TestProcessor2",
		Runtime:             "Test",
		ConnectionStr:       processorConnStr_2,
		ProjectName:         "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo_2},
	}

	// 1. register a processor
	err = dlyr.RegisterProcessor(testCtx, &proc_1)
	assert.NoError(t, err)

	// 1. register another processor
	err = dlyr.RegisterProcessor(testCtx, &proc_2)
	assert.NoError(t, err)

	// 2. Emit a window
	window := pb.Window{TimeFrom: &timestamppb.Timestamp{
		Seconds: 0,
		Nanos:   0,
	}, TimeTo: &timestamppb.Timestamp{
		Seconds: 1,
		Nanos:   0,
	},
		WindowTypeName:    windowType.GetName(),
		WindowTypeVersion: windowType.GetVersion(),
		Origin:            "Test",
		Metadata: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"asset_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
				"fleet_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
			},
		},
	}
	emitStatus, err := dlyr.EmitWindow(testCtx, &window)
	assert.Equal(t, emitStatus.GetStatus(), pb.WindowEmitStatus_PROCESSING_TRIGGERED)
}

// TestAddProcessor tests that several processors can be added
func TestLookbackDependenciesBetweenAlgorithms(t *testing.T) {

	// start the mock OrcaProcessor gRPC server
	mockProcessor_1, mockListener_1, err := StartMockOrcaProcessor(0) // set port to 0 to get random available port
	mockProcessor_2, mockListener_2, err := StartMockOrcaProcessor(0) // set port to 0 to get random available port
	assert.NoError(t, err)

	// FIXME: Should not have to do this. The `processTask` function should be wait grouped.
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond) // some time for processing to complete
		mockProcessor_1.GracefulStop()
		mockListener_1.Close()
		mockProcessor_2.GracefulStop()
		mockListener_2.Close()
	})

	// get the actual address the mock server is listening on
	processorConnStr_1 := mockListener_1.Addr().String()
	processorConnStr_2 := mockListener_2.Addr().String()

	// TODO: paramaterise if we have more datalayers (e.g. MySQL, SQLite) - high level function should be the same between them
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	asset_id := pb.MetadataField{Name: "asset_id", Description: "Unique ID of the asset"}
	fleet_id := pb.MetadataField{Name: "fleet_id", Description: "Unique ID of the fleet"}

	windowType := pb.WindowType{
		Name:    "TestWindow",
		Version: "1.0.0",
		MetadataFields: []*pb.MetadataField{
			&asset_id,
			&fleet_id,
		},
	}

	algo_1 := pb.Algorithm{
		Name:       "TestAlgorithm1",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	algo_2 := pb.Algorithm{
		Name:       "TestAlgorithm2",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	proc_1 := pb.ProcessorRegistration{
		Name:                "TestProcessor1",
		Runtime:             "Test",
		ProjectName:         "Test",
		ConnectionStr:       processorConnStr_1,
		SupportedAlgorithms: []*pb.Algorithm{&algo_1},
	}

	algo_2.Dependencies = []*pb.AlgorithmDependency{{
		Name:             algo_1.Name,
		Version:          algo_1.Version,
		ProcessorName:    proc_1.Name,
		ProcessorRuntime: proc_1.Runtime,
		Lookback: &pb.AlgorithmDependency_LookbackNum{
			LookbackNum: 5,
		},
	}}

	proc_2 := pb.ProcessorRegistration{
		Name:                "TestProcessor2",
		Runtime:             "Test",
		ConnectionStr:       processorConnStr_2,
		ProjectName:         "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo_2},
	}

	// 1. register a processor
	err = dlyr.RegisterProcessor(testCtx, &proc_1)
	assert.NoError(t, err)

	// 1. register another processor
	err = dlyr.RegisterProcessor(testCtx, &proc_2)
	assert.NoError(t, err)

	// 2. Emit enough windows to exhaust the lookback
	for ii := range 10 {
		window := pb.Window{TimeFrom: &timestamppb.Timestamp{
			Seconds: int64(ii),
			Nanos:   0,
		}, TimeTo: &timestamppb.Timestamp{
			Seconds: int64(ii + 1),
			Nanos:   0,
		},
			WindowTypeName:    windowType.GetName(),
			WindowTypeVersion: windowType.GetVersion(),
			Origin:            "Test",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"asset_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
					"fleet_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
				},
			},
		}
		emitStatus, err := dlyr.EmitWindow(testCtx, &window)
		assert.NoError(t, err)
		assert.Equal(t, emitStatus.GetStatus(), pb.WindowEmitStatus_PROCESSING_TRIGGERED)
	}
}

// TestMetadataFieldsChangeable tests that metadata fields on a window type can be changed
func TestMetadataFieldsChangeable(t *testing.T) {

	// start the mock OrcaProcessor gRPC server
	mockProcessor, mockListener, err := StartMockOrcaProcessor(0) // set port to 0 to get random available port
	assert.NoError(t, err)

	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond) // some time for processing to complete
		mockProcessor.GracefulStop()
		mockListener.Close()
	})

	// get the actual address the mock server is listening on
	processorConnStr := mockListener.Addr().String()

	// TODO: paramaterise if we have more datalayers (e.g. MySQL, SQLite) - high level function should be the same between them
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	asset_id := pb.MetadataField{Name: "asset_id", Description: "Unique ID of the asset"}
	fleet_id := pb.MetadataField{Name: "fleet_id", Description: "Unique ID of the fleet"}

	// two windows - that look the same, but have different metadata ids.
	// when registering with the first, we should see no issue. but registering
	// the second should return an issue.
	windowType := pb.WindowType{
		Name:    "TestWindowForMetadataFields",
		Version: "1.0.0",
		MetadataFields: []*pb.MetadataField{
			&asset_id,
			&fleet_id,
		},
	}

	windowTypeModified := pb.WindowType{
		Name:    "TestWindowForMetadataFields`",
		Version: "1.0.0",
		MetadataFields: []*pb.MetadataField{
			&asset_id, // just contains asset_id this time
		},
	}
	windowTypeNew := pb.WindowType{
		Name:    "TestWindowForMetadataFields`",
		Version: "1.1.0", // Minor version bump
		MetadataFields: []*pb.MetadataField{
			&asset_id,
		},
	}

	algo_1 := pb.Algorithm{
		Name:       "TestAlgorithm1",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	algo_2 := pb.Algorithm{
		Name:       "TestAlgorithm2",
		Version:    "1.0.0",
		WindowType: &windowTypeModified,
		ResultType: pb.ResultType_VALUE,
	}

	algoNew := pb.Algorithm{
		Name:       "TestAlgorithm2",
		Version:    "1.0.0", // no need to bump the version
		WindowType: &windowTypeNew,
		ResultType: pb.ResultType_VALUE,
	}

	proc := pb.ProcessorRegistration{
		Name:                "TestProcessor",
		Runtime:             "Test",
		ConnectionStr:       processorConnStr,
		SupportedAlgorithms: []*pb.Algorithm{&algo_1},
	}

	procModified := pb.ProcessorRegistration{
		Name:                "TestProcessor",
		Runtime:             "Test",
		ConnectionStr:       processorConnStr,
		SupportedAlgorithms: []*pb.Algorithm{&algo_2},
	}

	procNew := pb.ProcessorRegistration{
		Name:                "TestProcessor",
		Runtime:             "Test",
		ConnectionStr:       processorConnStr,
		SupportedAlgorithms: []*pb.Algorithm{&algoNew},
	}

	// 1. register a processor
	err = dlyr.RegisterProcessor(testCtx, &proc)
	assert.NoError(t, err)

	// 2. Emit a window
	window := pb.Window{TimeFrom: &timestamppb.Timestamp{
		Seconds: 0,
		Nanos:   0,
	}, TimeTo: &timestamppb.Timestamp{
		Seconds: 1,
		Nanos:   0,
	},
		WindowTypeName:    windowType.GetName(),
		WindowTypeVersion: windowType.GetVersion(),
		Origin:            "Test",
		Metadata: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"asset_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
				"fleet_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
			},
		},
	}
	emitStatus, err := dlyr.EmitWindow(testCtx, &window)
	assert.NoError(t, err)
	assert.Equal(t, pb.WindowEmitStatus_PROCESSING_TRIGGERED, emitStatus.GetStatus())

	// 3. Re-register with the modified window type
	err = dlyr.RegisterProcessor(testCtx, &procModified)
	assert.NoError(t, err)

	// 4. Emit a window with the new metadata field structure
	window = pb.Window{TimeFrom: &timestamppb.Timestamp{
		Seconds: 0,
		Nanos:   0,
	}, TimeTo: &timestamppb.Timestamp{
		Seconds: 1,
		Nanos:   0,
	},
		WindowTypeName:    windowType.GetName(),
		WindowTypeVersion: windowType.GetVersion(),
		Origin:            "Test",
		Metadata: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"asset_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
			},
		},
	}
	// 5. Confirm that the window could not be emitted becuase it is missing the field ID
	emitStatus, err = dlyr.EmitWindow(testCtx, &window)
	assert.Error(t, err)
	assert.Equal(t, pb.WindowEmitStatus_TRIGGERING_FAILED, emitStatus.GetStatus())

	// 6. Confirm that if we bump the window version, then this is treated as a new window.
	// To do this, we need to re-register the processor with the new window
	err = dlyr.RegisterProcessor(testCtx, &procNew)
	assert.NoError(t, err)

	window = pb.Window{TimeFrom: &timestamppb.Timestamp{
		Seconds: 0,
		Nanos:   0,
	}, TimeTo: &timestamppb.Timestamp{
		Seconds: 1,
		Nanos:   0,
	},
		WindowTypeName:    windowTypeNew.GetName(),
		WindowTypeVersion: windowTypeNew.GetVersion(),
		Origin:            "Test",
		Metadata: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"asset_id": {Kind: &structpb.Value_NumberValue{NumberValue: 0}},
			},
		},
	}

	// 7. Confirm that this window is totally different and so not bound by the old metadata fields
	emitStatus, err = dlyr.EmitWindow(testCtx, &window)
	assert.NoError(t, err)
	assert.Equal(t, pb.WindowEmitStatus_PROCESSING_TRIGGERED, emitStatus.GetStatus())
}

// TestWindowTypeDefinition tests for errors raised when building and registering window types that already exist
func TestWindowTypeDefintion(t *testing.T) {

	// start the mock OrcaProcessor gRPC server
	mockProcessor, mockListener, err := StartMockOrcaProcessor(0) // set port to 0 to get random available port
	assert.NoError(t, err)

	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond) // some time for processing to complete
		mockProcessor.GracefulStop()
		mockListener.Close()
	})

	// get the actual address the mock server is listening on
	processorConnStr := mockListener.Addr().String()

	// TODO: paramaterise if we have more datalayers (e.g. MySQL, SQLite) - high level function should be the same between them
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	asset_id := pb.MetadataField{Name: "asset_id", Description: "Unique ID of the asset"}
	fleet_id := pb.MetadataField{Name: "fleet_id", Description: "Unique ID of the fleet"}

	windowtype1 := pb.WindowType{
		Name:    "TestWindow",
		Version: "1.0.0",
		MetadataFields: []*pb.MetadataField{
			&asset_id,
			&fleet_id,
		},
	}

	// second window has the same name and version, but one less field
	windowtype2 := pb.WindowType{
		Name:    "TestWindow",
		Version: "1.0.0",
		MetadataFields: []*pb.MetadataField{
			&asset_id,
		},
	}

	algo := pb.Algorithm{
		Name:       "StubAlgo",
		Version:    "1.0.0",
		WindowType: &windowtype1,
		ResultType: pb.ResultType_VALUE,
	}

	proc := pb.ProcessorRegistration{
		Name:                "StubProcessor",
		Runtime:             "Stub",
		SupportedAlgorithms: []*pb.Algorithm{&algo},
		ConnectionStr:       processorConnStr,
	}

	err = dlyr.RegisterProcessor(testCtx, &proc)
	assert.NoError(t, err)

	algo.WindowType = &windowtype2
	err = dlyr.RegisterProcessor(testCtx, &proc)
	assert.Error(t, err)

}

func TestCircularDependency(t *testing.T) {
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	windowType := pb.WindowType{
		Name:    "TestWindow",
		Version: "2.0.0",
	}

	algo1 := pb.Algorithm{
		Name:       "TestAlgorithm1",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_NONE,
	}

	algo2 := pb.Algorithm{
		Name:       "TestAlgorithm2",
		Version:    "1.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_NONE,
	}

	proc := pb.ProcessorRegistration{
		Name:                "TestProcessor",
		Runtime:             "Test",
		ConnectionStr:       "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo1, &algo2},
	}

	// 1. register a processor
	err = dlyr.RegisterProcessor(testCtx, &proc)
	assert.NoError(t, err)

	// 5. add a dependency between algorithm 1 and algorithm 2
	algo1.Dependencies = []*pb.AlgorithmDependency{
		{
			Name:             "TestAlgorithm2",
			Version:          "1.0.0",
			ProcessorName:    "TestProcessor",
			ProcessorRuntime: "Test",
		},
	}

	err = dlyr.RegisterProcessor(testCtx, &proc)
	assert.NoError(t, err)

	// 6. now add a dependency between 2 and 1. This should raise a circular error
	algo2.Dependencies = []*pb.AlgorithmDependency{
		{
			Name:             "TestAlgorithm1",
			Version:          "1.0.0",
			ProcessorName:    "TestProcessor",
			ProcessorRuntime: "Test",
		},
	}

	err = dlyr.RegisterProcessor(testCtx, &proc)

	var circularError *types.CircularDependencyError
	assert.ErrorAs(t, err, &circularError)

	assert.Equal(t, algo1.GetName(), circularError.FromAlgoName)
	assert.Equal(t, algo2.GetName(), circularError.ToAlgoName)
	assert.Equal(t, algo1.GetVersion(), circularError.FromAlgoVersion)
	assert.Equal(t, algo2.GetVersion(), circularError.ToAlgoVersion)
	assert.Equal(t, proc.GetName(), circularError.FromAlgoProcessor)
	assert.Equal(t, proc.GetName(), circularError.ToAlgoProcessor)
}

func TestValidDependenciesBetweenProcessors(t *testing.T) {
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	windowType := pb.WindowType{
		Name:    "TestWindow",
		Version: "3.0.0",
	}

	algo1 := pb.Algorithm{
		Name:       "TestAlgorithm1",
		Version:    "2.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}
	algo2 := pb.Algorithm{
		Name:       "TestAlgorithm2",
		Version:    "2.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	algo3 := pb.Algorithm{
		Name:       "TestAlgorithm3",
		Version:    "2.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	algo4 := pb.Algorithm{
		Name:       "TestAlgorithm4",
		Version:    "2.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_VALUE,
	}

	proc_1 := pb.ProcessorRegistration{
		Name:                "TestProcessor1",
		Runtime:             "Test",
		ConnectionStr:       "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo1, &algo2},
	}

	algo3.Dependencies = []*pb.AlgorithmDependency{
		{
			Name:             algo1.Name,
			Version:          algo1.Version,
			ProcessorName:    proc_1.GetName(),
			ProcessorRuntime: proc_1.GetRuntime(),
		},
		{
			Name:             algo2.Name,
			Version:          algo2.Version,
			ProcessorName:    proc_1.GetName(),
			ProcessorRuntime: proc_1.GetRuntime(),
		},
	}

	algo4.Dependencies = []*pb.AlgorithmDependency{
		{
			Name:             algo3.Name,
			Version:          algo3.Version,
			ProcessorName:    "TestProcessor2",
			ProcessorRuntime: "Test",
		},
	}
	proc_2 := pb.ProcessorRegistration{
		Name:                "TestProcessor2",
		Runtime:             "Test",
		ConnectionStr:       "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo3, &algo4},
	}

	err = dlyr.RegisterProcessor(testCtx, &proc_1)
	assert.NoError(t, err)

	err = dlyr.RegisterProcessor(testCtx, &proc_2)
	assert.NoError(t, err)

	// emit for good measure. tests some useful code pathways like the
	// dag resolution
	window := &pb.Window{TimeFrom: &timestamppb.Timestamp{
		Seconds: 0,
		Nanos:   0,
	}, TimeTo: &timestamppb.Timestamp{
		Seconds: 1,
		Nanos:   0,
	},
		WindowTypeName:    windowType.GetName(),
		WindowTypeVersion: windowType.GetVersion(),
		Origin:            "Test",
	}
	emitStatus, err := dlyr.EmitWindow(testCtx, window)
	assert.NoError(t, err)
	assert.Equal(t, emitStatus.GetStatus(), pb.WindowEmitStatus_PROCESSING_TRIGGERED)
}

func TestAlgosSameNamesDifferentProcessors(t *testing.T) {
	dlyr, err := NewDatalayerClient(testCtx, "postgresql", testConnStr)
	assert.NoError(t, err)

	windowType := pb.WindowType{
		Name:    "TestWindow",
		Version: "4.0.0",
	}

	algo1 := pb.Algorithm{
		Name:       "TestAlgorithm",
		Version:    "4.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_NONE,
	}

	algo2 := pb.Algorithm{
		Name:       "TestAlgorithm",
		Version:    "4.0.0",
		WindowType: &windowType,
		ResultType: pb.ResultType_NONE,
	}

	proc1 := pb.ProcessorRegistration{
		Name:                "TestProcessor1",
		Runtime:             "Test",
		ConnectionStr:       "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo1},
	}

	proc2 := pb.ProcessorRegistration{
		Name:                "TestProcessor2",
		Runtime:             "Test",
		ConnectionStr:       "Test",
		SupportedAlgorithms: []*pb.Algorithm{&algo2},
	}

	// 1. register the processors
	err = dlyr.RegisterProcessor(testCtx, &proc1)
	assert.NoError(t, err)
	err = dlyr.RegisterProcessor(testCtx, &proc2)
	assert.NoError(t, err)
}
