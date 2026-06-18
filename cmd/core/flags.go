package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"

	internal "github.com/orca-telemetry/core/internal"
	migrations "github.com/orca-telemetry/core/migrations"
)

type cliFlags struct {
	migrate  bool
	showHelp bool
}

var logLevels = []string{
	"DEBUG",
	"INFO",
	"WARN",
	"ERROR",
}

const postgresExampleConnStr = "postgresql://<user>:<pass>@<localhost>:<port>/<db>?<setting=value>"

func ValidateConnStr(s string) error {
	if s == "" {
		return errors.New("connection string cannot be empty")
	}
	_, err := ParsePostgresURL(s, postgresExampleConnStr)
	return err
}

func ValidatePort(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid port number %d (must be between 1-65535)", port)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("port %d is already in use", port)
	}
	listener.Close()

	return nil
}

func ValidateLogLevel(s string) error {
	if s == "" {
		return errors.New("you must select a log level")
	}

	s = strings.ToUpper(s)
	if slices.Contains(logLevels, s) {
		return nil
	}
	return fmt.Errorf("invalid log level: %s. Must be one of: %s", s, strings.Join(logLevels, ", "))
}

func parseFlags() cliFlags {
	flags := cliFlags{}

	flag.BoolVar(&flags.showHelp, "help", false, "Show help")
	flag.BoolVar(
		&flags.migrate,
		"migrate",
		false,
		"Migrate the orca db prior to launching orca. Will need to be run at least once to provision the store before use",
	)
	flag.Parse()

	return flags
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func validateFlags(flags cliFlags) error {
	return nil
}

func validateConfig(config *internal.Config) error {
	if config.ConnectionString == "" {
		return fmt.Errorf("ORCA_CONNECTION_STRING environment variable is required")
	}
	if err := ValidateConnStr(config.ConnectionString); err != nil {
		return fmt.Errorf("invalid connection string: %w", err)
	}

	if err := ValidatePort(config.Port); err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}

	if err := ValidateLogLevel(config.LogLevel); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}

	return nil
}

func buildConfig(flags cliFlags) *internal.Config {
	if flags.showHelp {
		flag.Usage()
		fmt.Println("\nEnvironment Variables:")
		fmt.Println("  ORCA_CONNECTION_STRING  PostgreSQL connection string (required)")
		fmt.Println("  ORCA_PORT              Server port (default: 4040)")
		fmt.Println("  ORCA_LOG_LEVEL         Log level (default: INFO)")
		fmt.Println("  ORCA_ENV               Environment (production/prod for production mode - if in production mode TLS will be used throughout for all gRPC connections)")
		return nil
	}

	config := internal.GetConfig()

	if err := validateConfig(config); err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(config.LogLevel),
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	slog.Info("starting orca",
		"port", config.Port,
		"production", config.IsProduction,
		"logLevel", config.LogLevel)

	slog.Info("premigration")
	if flags.migrate {
		slog.Info("migrating datalayer")
		err := migrations.MigrateDatalayer(config.ConnectionString)
		if err != nil {
			slog.Error("could not migrate the datalayer, exiting", "error", err)
			os.Exit(1)
		}
	}
	return config
}
