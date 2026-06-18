package main

import (
	"testing"
)

func TestParsePostgresURL(t *testing.T) {
	testCases := []struct {
		name     string
		url      string
		expected map[string]string
		wantErr  bool
	}{
		{
			name: "Full URL with all components",
			url:  "postgresql://user:password@localhost:5432/mydb?sslmode=disable",
			expected: map[string]string{
				"protocol": "postgresql",
				"user":     "user",
				"password": "password",
				"host":     "localhost",
				"port":     "5432",
				"database": "mydb",
				"settings": "sslmode=disable",
			},
			wantErr: false,
		},
		{
			name: "Minimal URL with host and database only",
			url:  "postgres://localhost/db",
			expected: map[string]string{
				"protocol": "postgres",
				"host":     "localhost",
				"database": "db",
			},
			wantErr: true,
		},
		{
			name: "URL with user, port, database and settings",
			url:  "postgres://user@localhost:5433/analytics?connect_timeout=10",
			expected: map[string]string{
				"protocol": "postgres",
				"user":     "user",
				"host":     "localhost",
				"port":     "5433",
				"database": "analytics",
				"settings": "connect_timeout=10",
			},
			wantErr: false,
		},
		{
			name:     "Invalid URL",
			url:      "invalid://connection-string",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePostgresURL(tc.url, "")

			// Check error status
			if (err != nil) != tc.wantErr {
				t.Errorf("ParsePostgresURL() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			// If we expect an error, no need to check the result
			if tc.wantErr {
				return
			}

			// Check if all expected keys exist and have the correct values
			for key, expectedValue := range tc.expected {
				if gotValue, exists := got[key]; !exists || gotValue != expectedValue {
					t.Errorf(
						"ParsePostgresURL() for key %q = %q, want %q",
						key,
						gotValue,
						expectedValue,
					)
				}
			}

			// Check if there are any unexpected keys
			for key := range got {
				if _, exists := tc.expected[key]; !exists && got[key] != "" {
					t.Errorf("ParsePostgresURL() unexpected non-empty key %q = %q", key, got[key])
				}
			}
		})
	}
}
