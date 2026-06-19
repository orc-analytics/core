package db

import (
	"errors"
	"fmt"
)

// custom errors
var (
	AlgorithmExistsUnderDifferentProcessor = fmt.Errorf(
		"algorithm exists under a different processor",
	)
)

var (
	ErrWorkerAlreadyExists = errors.New("worker already exists")
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
