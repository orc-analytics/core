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

	"github.com/orca-telemetry/core/internal"
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

// valid datalayers - as they are displayed
var datalayerSuggestions = []string{
	"postgresql",
}

// templates for filling out connection string
type (
	ConnectionStrParser func(connectionStr string, example string) (map[string]string, error)
	connStringTemplate  struct {
		validationFunc ConnectionStrParser
		exampleConnStr string
	}
)

var connectionTemplates = map[string]connStringTemplate{
	"postgresql": {
		validationFunc: ParsePostgresURL,
		exampleConnStr: "postgresql://<user>:<pass>@<localhost>:<port>/<db>?<setting=value>",
	},
}

// validation functions
func ValidateDatalayer(s string) error {
	if s == "" {
		return fmt.Errorf("platform cannot be determined from connection string")
	}
	if slices.Contains(datalayerSuggestions, s) {
		return nil
	}
	return fmt.Errorf("unsupported datalayer: %s", s)
}

func ValidateConnStr(s, platform string) error {
	if s == "" {
		return errors.New("connection string cannot be empty")
	}
	template, ok := connectionTemplates[platform]
	if !ok {
		// If we don't have a specific template, do basic validation
		if len(s) < 5 {
			return fmt.Errorf("connection string appears invalid")
		}
		return nil
	}
	_, err := template.validationFunc(s, template.exampleConnStr)
	return err
}

func ValidatePort(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid port number %d (must be between 1-65535)", port)
	}

	// check if port is already in use
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
	if flags.showHelp {
		return nil
	}
	return nil
}

func validateConfig(config *internal.Config) error {
	if config.Platform == "" {
		return fmt.Errorf("platform cannot be determined from connection string")
	}
	if err := ValidateDatalayer(config.Platform); err != nil {
		return fmt.Errorf("invalid platform: %w", err)
	}

	if config.ConnectionString == "" {
		return fmt.Errorf("ORCA_CONNECTION_STRING environment variable is required")
	}
	if err := ValidateConnStr(config.ConnectionString, config.Platform); err != nil {
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

func runCLI(flags cliFlags) {
	if flags.showHelp {
		flag.Usage()
		fmt.Println("\nEnvironment Variables:")
		fmt.Println("  ORCA_CONNECTION_STRING  Database connection string (required)")
		fmt.Println("  ORCA_PORT              Server port (default: 4040)")
		fmt.Println("  ORCA_LOG_LEVEL         Log level (default: INFO)")
		fmt.Println("  ORCA_ENV               Environment (production/prod for production mode - if in production mode TLS will be used throughout for all gRPC connections)")
		return
	}

	// get singleton configuration
	config := internal.GetConfig()

	// validate configuration
	if err := validateConfig(config); err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	// stdout logger
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(config.LogLevel),
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	slog.Info("starting orca",
		"platform", config.Platform,
		"port", config.Port,
		"production", config.IsProduction,
		"logLevel", config.LogLevel)

	// perform migrations if requested
	slog.Info("premigration")
	if flags.migrate {
		slog.Info("migrating datalayer", "platform", config.Platform)
		err := MigrateDatalayer(config.Platform, config.ConnectionString)
		if err != nil {
			slog.Error("could not migrate the datalayer, exiting", "error", err)
			os.Exit(1)
		}
	}
	startGRPCServer(config.Platform, config.ConnectionString, config.Port, config.LogLevel)

	// keep main thread alive
	select {}
}
