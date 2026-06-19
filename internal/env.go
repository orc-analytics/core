package internal

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	IsProduction     bool
	ConnectionString string
	Port             int
	Platform         string
	LogLevel         slog.Leveler
}

var (
	configInstance *Config
	configOnce     sync.Once
)

// GetConfig returns the singleton configuration instance
func GetConfig() *Config {
	configOnce.Do(func() {
		configInstance = loadConfig()
	})
	return configInstance
}

// loadConfig loads configuration from environment variables
func loadConfig() *Config {
	config := &Config{}

	orcaEnv := os.Getenv("ORCA_ENV")
	config.IsProduction = orcaEnv == "production" || orcaEnv == "prod"

	config.ConnectionString = os.Getenv("ORCA_CONNECTION_STRING")

	config.Port = 4040
	if portStr := os.Getenv("ORCA_PORT"); portStr != "" {
		if parsedPort, err := strconv.Atoi(portStr); err == nil {
			config.Port = parsedPort
		}
	}

	if logLevel := os.Getenv("ORCA_LOG_LEVEL"); logLevel != "" {
		switch strings.ToLower(logLevel) {
		case "debug":
			config.LogLevel = slog.LevelDebug
		case "info":
			config.LogLevel = slog.LevelInfo
		case "warn":
			config.LogLevel = slog.LevelWarn
		case "error":
			config.LogLevel = slog.LevelError
		default:
			slog.Error(fmt.Sprintf("log level %v not valid. valid options are debug, info, warn, and error", logLevel))
			os.Exit(1)
		}
	}

	config.Platform = inferPlatformFromConnectionString(config.ConnectionString)

	return config
}

// ReloadConfig forces a reload of the configuration
func ReloadConfig() *Config {
	configOnce = sync.Once{}
	return GetConfig()
}

// inferPlatformFromConnectionString determines the database platform from the connection string
func inferPlatformFromConnectionString(connStr string) string {
	if connStr == "" {
		return ""
	}

	connStr = strings.ToLower(connStr)

	// Check for PostgreSQL patterns
	if strings.HasPrefix(connStr, "postgresql://") ||
		strings.HasPrefix(connStr, "postgres://") ||
		strings.Contains(connStr, "postgres") {
		return "postgresql"
	}

	// Add more database types as needed:
	// MySQL
	if strings.HasPrefix(connStr, "mysql://") ||
		strings.Contains(connStr, "mysql") {
		return "mysql"
	}

	// SQLite
	if strings.HasSuffix(connStr, ".db") ||
		strings.HasSuffix(connStr, ".sqlite") ||
		strings.HasSuffix(connStr, ".sqlite3") ||
		strings.HasPrefix(connStr, "sqlite://") {
		return "sqlite"
	}

	// Default to postgresql if we can't determine
	return "postgresql"
}
