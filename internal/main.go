package internal

import (
	"context"
	"log/slog"

	"buf.build/go/protovalidate"
	pb "github.com/orca-telemetry/contract/go/v2"
	"github.com/orca-telemetry/core/internal/db"
	"google.golang.org/protobuf/proto"
)

type (
	CoreServer struct {
		pb.UnimplementedCoreServer
		client *db.Datalayer
	}
)

var (
	MAX_PROCESSORS = 20
)

// NewServer produces a new ORCA gRPC server
func NewServer(
	ctx context.Context,
	connStr string,
) (*CoreServer, error) {
	client, err := db.NewClient(ctx, connStr)
	if err != nil {
		slog.Error(
			"could not initialise client",
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
	config := GetConfig()
	windowEmitStatus, err := o.client.EmitWindow(ctx, window, config.IsProduction)
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
