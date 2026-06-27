package migrations

import (
	"embed"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed *.sql
var PostgresqlMigrations embed.FS

func MigrateDatalayer(connStr string) error {
	d, err := iofs.New(PostgresqlMigrations, ".")
	if err != nil {
		return fmt.Errorf("failed to load embedded migrations: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", d, connStr)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	if err := m.Up(); err == migrate.ErrNoChange {
		slog.Info("no migrations needed")
	} else if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}
