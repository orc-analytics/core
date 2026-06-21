package db

import (
	"fmt"
)

// custom errors
var (
	AlgorithmExistsUnderDifferentProcessor = fmt.Errorf(
		"algorithm exists under a different processor",
	)
)

var (
	ErrWorkerAlreadyExists       = "worker already exists"
	ErrWorkerNotFound            = "worker is not found"
	ErrBadWorkerId               = "worker Id is malformed"
	ErrBadNonceId                = "nonce Id is malformed"
	ErrCouldNotGenerateUniqueKey = "unable to generate a unique key - too many clashes"
	ErrNonceNotFound             = "nonce not found"
	ErrBadSignature              = "signature provided is bad"
	ErrBadPublicKey              = "public key is malformed. must be ed25519"
	ErrDatabase                  = func(err error) string { return fmt.Sprintf("database error: %v", err.Error()) }
	ErrServer                    = func(err error) string { return fmt.Sprintf("server error: %v", err.Error()) }
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
