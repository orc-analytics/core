package db

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// partial index and PK clashes
	PgUniqueViolation = "23505"
	// inserting with a non-existent foreign key
	PgForeignKeyViolation = "23503"
	// missing required fields
	PgNotNullViolation = "23502"
	// contention on the database - transaction rejected
	PgSerializationFailure = "40001"
	// concurrent writes to the same rows
	PgDeadlock = "40P01"
	// Cycle detected - custom error
	PgCycleDetected = "UE001"
)

// a helper struct that exposes the database via the predefiend queries
type DB struct {
	queries *Queries
	conn    *pgxpool.Pool
	closeFn func()
}

// generate a new client for the postgres datalayer
func NewDbQueries(ctx context.Context, connStr string) (*DB, error) {
	if connStr == "" {
		return nil, errors.New("connection string empty")
	}

	connPool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		slog.Error("issue connecting to the db", "error", err)
		return nil, err
	}

	return &DB{
		queries: New(connPool),
		conn:    connPool,
		closeFn: connPool.Close,
	}, nil
}

func (d *DB) BeginTx(ctx context.Context) (pgx.Tx, error) {

	tx, err := d.conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable, // ensures that concurrent actions happen in an order.
	})
	if err != nil {
		slog.Error("could not start a transaction with the DB", "error", err)
		return nil, err
	}
	return tx, nil
}

func (d *DB) WithTx(tx pgx.Tx) *Queries {
	return d.queries.WithTx(tx)
}

func (d *DB) Query() *Queries {
	return d.queries
}
