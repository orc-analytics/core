package internal

import (
	"context"
	"log/slog"

	"buf.build/go/protovalidate"
	pb "github.com/orca-telemetry/contract/go"
	dlyr "github.com/orca-telemetry/core/internal/datalayers"
	types "github.com/orca-telemetry/core/internal/types"
	"google.golang.org/protobuf/proto"
)

type (
	OrcaCoreServer struct {
		pb.UnimplementedOrcaCoreServer
		client types.Datalayer
	}
)

var (
	MAX_PROCESSORS = 20
)

// NewServer produces a new ORCA gRPC server
func NewServer(
	ctx context.Context,
	platform dlyr.Platform,
	connStr string,
) (*OrcaCoreServer, error) {
	client, err := dlyr.NewDatalayerClient(ctx, platform, connStr)
	if err != nil {
		slog.Error(
			"Could not initialise new platform client whilst initialising server",
			"platform",
			platform,
			"error",
			err,
		)

		return nil, err
	}

	s := &OrcaCoreServer{
		client: client,
	}
	return s, nil
}

// validate a protobuf via protovalidate
func validate[T proto.Message](msg T) error {
	v, err := protovalidate.New()
	if err != nil {
		return err
	}

	if err := v.Validate(msg); err != nil {
		return err
	}

	return nil
}

// --------------------------- gRPC Services ---------------------------
// -------------------------- Core Operations --------------------------
// Register a processor with orca-core. Called when a processor startsup.
func (o *OrcaCoreServer) RegisterProcessor(
	ctx context.Context,
	proc *pb.ProcessorRegistration,
) (*pb.Status, error) {
	err := validate(proc)
	if err != nil {
		return nil, err
	}
	slog.Info("registering processor")
	err = o.client.RegisterProcessor(ctx, proc)
	if err != nil {
		return nil, err
	}
	slog.Debug("registered processor", "processor", proc)
	return &pb.Status{
		Received: true,
		Message:  "Successfully registered processor",
	}, nil
}

func (o *OrcaCoreServer) EmitWindow(
	ctx context.Context,
	window *pb.Window,
) (*pb.WindowEmitStatus, error) {
	slog.Debug("Recieved Window", "window", window)
	err := validate(window)
	if err != nil {
		return nil, err
	}
	slog.Info("emitting window", "window", window)
	windowEmitStatus, err := o.client.EmitWindow(ctx, window)
	return &windowEmitStatus, err
}

func (o *OrcaCoreServer) Expose(
	ctx context.Context,
	settings *pb.ExposeSettings,
) (*pb.InternalState, error) {
	slog.Debug("recieved request to expose internal state", "settings", settings)
	err := validate(settings)
	if err != nil {
		return nil, err
	}
	internalState, err := o.client.Expose(ctx, settings)
	return internalState, err
}
