package datalayers

import (
	"context"
	"fmt"

	pb "github.com/orca-telemetry/contract/go"
)

// the interface that all datalayers must implement to be compatible with Orca
type (
	Tx interface {
		Rollback(ctx context.Context)
		Commit(ctx context.Context) error
	}
	Datalayer interface {
		WithTx(ctx context.Context) (Tx, error)

		// Core level operations
		RegisterProcessor(ctx context.Context, proc *pb.ProcessorRegistration) error
		EmitWindow(ctx context.Context, window *pb.Window) (pb.WindowEmitStatus, error)
		Expose(ctx context.Context, settings *pb.ExposeSettings) (*pb.InternalState, error)
	}
)

// custom errors
var (
	AlgorithmExistsUnderDifferentProcessor = fmt.Errorf(
		"algorithm exists under a different processor",
	)
)

type CircularDependencyError struct {
	FromAlgoName      string
	ToAlgoName        string
	FromAlgoVersion   string
	ToAlgoVersion     string
	FromAlgoProcessor string
	ToAlgoProcessor   string
}

func (c *CircularDependencyError) Error() string {
	return fmt.Sprintf(
		"Circular dependency introduced between algorithm %s to %s, with versions %s and %s, of processor(s) %s and %s respectively.",
		c.FromAlgoName,
		c.ToAlgoName,
		c.FromAlgoVersion,
		c.ToAlgoVersion,
		c.FromAlgoProcessor,
		c.ToAlgoProcessor,
	)
}
