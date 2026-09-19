package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	defaultListenAddr    = "localhost:3000"
	defaultAllowedOrigin = "http://localhost:3001"
)

type APIConfig struct {
	DatabaseURL    string
	ClickHouseURL  string
	ListenAddr     string
	AllowedOrigins []string
	CookieSecure   bool
}

// LoadAPI loads API config from environment variables.
// Example: POSTGRES_URL=... CLICKHOUSE_URL=... -> APIConfig{...}.
func LoadAPI() (APIConfig, error) {
	return ParseAPI(os.Getenv)
}

// ParseAPI builds API config using getenv and validates required values.
// Example: LISTEN_ADDR="" -> ListenAddr="localhost:3000".
func ParseAPI(getenv func(string) string) (APIConfig, error) {
	databaseURL := getenv("POSTGRES_URL")
	if databaseURL == "" {
		return APIConfig{}, errors.New("POSTGRES_URL is not set")
	}

	clickhouseURL := strings.TrimSpace(getenv("CLICKHOUSE_URL"))
	if clickhouseURL == "" {
		return APIConfig{}, errors.New("CLICKHOUSE_URL is not set")
	}

	listenAddr, err := parseListenAddr(getenv("LISTEN_ADDR"))
	if err != nil {
		return APIConfig{}, err
	}

	return APIConfig{
		DatabaseURL:    databaseURL,
		ClickHouseURL:  clickhouseURL,
		ListenAddr:     listenAddr,
		AllowedOrigins: parseAllowedOrigins(getenv("CORS_ALLOWED_ORIGINS")),
		CookieSecure:   getenv("COOKIE_SECURE") == "true",
	}, nil
}

type JobsConfig struct {
	DatabaseURL   string
	ClickHouseURL string
	DashboardURL  string
}

// LoadJobs loads jobs config from environment variables.
// Example: POSTGRES_URL=... CLICKHOUSE_URL=... DASHBOARD_URL=http://localhost:3001 -> JobsConfig{...}.
func LoadJobs() (JobsConfig, error) {
	return ParseJobs(os.Getenv)
}

// ParseJobs builds jobs config using getenv and validates required values.
// Example: DASHBOARD_URL="http://localhost:3001/" -> DashboardURL="http://localhost:3001".
func ParseJobs(getenv func(string) string) (JobsConfig, error) {
	databaseURL := getenv("POSTGRES_URL")
	if databaseURL == "" {
		return JobsConfig{}, errors.New("POSTGRES_URL is not set")
	}

	clickhouseURL := strings.TrimSpace(getenv("CLICKHOUSE_URL"))
	if clickhouseURL == "" {
		return JobsConfig{}, errors.New("CLICKHOUSE_URL is not set")
	}

	return JobsConfig{
		DatabaseURL:   databaseURL,
		ClickHouseURL: clickhouseURL,
		DashboardURL:  strings.TrimSuffix(strings.TrimSpace(getenv("DASHBOARD_URL")), "/"),
	}, nil
}

func parseListenAddr(raw string) (string, error) {
	if raw == "" {
		raw = defaultListenAddr
	}

	if _, _, err := net.SplitHostPort(raw); err != nil {
		return "", fmt.Errorf("LISTEN_ADDR must be a host:port address (e.g. 0.0.0.0:3000): %w", err)
	}

	return raw, nil
}

func parseAllowedOrigins(raw string) []string {
	origins := splitAndTrim(raw)
	if len(origins) == 0 {
		return []string{defaultAllowedOrigin}
	}

	return origins
}

func splitAndTrim(raw string) []string {
	var values []string
	for part := range strings.SplitSeq(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}

	return values
}
