package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// style for the placeholder text
var (
	// errorStyle = lipgloss.NewStyle().
	// 		Foreground(lipgloss.Color("9")). // Red text
	// 		Bold(true).
	// 		MarginTop(1)
	errorHeaderStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("9")). // Red text
				Bold(true).
				Underline(true)
	errorDetailStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("11")). // Yellow text
				Italic(true).
				MarginLeft(2)
)

// ParsePostgresURL parses a PostgreSQL connection string and returns a map of named capture groups
func ParsePostgresURL(s string, example string) (map[string]string, error) {
	// define the regex pattern with named capture groups
	pattern := `(?P<protocol>postgresql|postgres):\/\/(?:(?P<user>[^:@\s]*)(?::(?P<password>[^@\s]*))?@)?(?P<host>[^\/:@\s]+)(?::(?P<port>\d+))?(?:\/(?P<database>[^?\s]*))?(?:\?(?P<settings>[^\s]*))?`

	// copile the regex
	re, err := regexp.Compile(pattern)
	if err != nil {
		// should never occur
		return nil, fmt.Errorf("failed to compile regex: %w", err)
	}

	// find matches
	match := re.FindStringSubmatch(s)
	if match == nil {
		if !strings.HasPrefix(s, "postgresql://") && !strings.HasPrefix(s, "postgres://") {
			return nil, fmt.Errorf(
				"Connection string must start with 'postgresql://' or 'postgres://'",
			)
		}
		return nil, fmt.Errorf("Failed to recognise this as a postgres connection string")
	}

	// get the names of the capture groups
	groupNames := re.SubexpNames()

	// create a map to store the results
	result := make(map[string]string)

	// populate the map with capture group values
	for i, name := range groupNames {
		if i != 0 && name != "" && i < len(match) { // skip the first element as it's the full match
			result[name] = match[i]
		}
	}

	// iterate through the map and collect errors if there are any
	var errorMsgs []string
	if result["user"] == "" {
		errorMsgs = append(
			errorMsgs,
			"No username found. Credentials must be in format 'user:password'",
		)
	}
	if result["host"] == "" {
		errorMsgs = append(errorMsgs, "Missing host/domain (e.g., 'localhost' or 'db.example.com')")
	}
	if result["port"] == "" {
		errorMsgs = append(errorMsgs, "Missing port number (e.g., ':5432')")
	}
	if result["database"] == "" {
		errorMsgs = append(errorMsgs, "Missing database name (e.g., '/mydb')")
	}

	if len(errorMsgs) > 0 {
		// Create a formatted error message
		var sb strings.Builder
		sb.WriteString(errorHeaderStyle.Render("Invalid Connection String Format"))
		sb.WriteString("\n")
		sb.WriteString(errorDetailStyle.Render("Expected format: " + example))
		sb.WriteString("\n")
		sb.WriteString(errorDetailStyle.Render("Issues found:"))
		for _, msg := range errorMsgs {
			sb.WriteString("\n" + errorDetailStyle.Render("• "+msg))
		}
		return result, errors.New(sb.String())
	}

	return result, nil
}
