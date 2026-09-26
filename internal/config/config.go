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

type Config struct {
	DatabaseURL    string
	ClickHouseURL  string
	ListenAddr     string
	AllowedOrigins []string
	CookieSecure   bool
	DashboardURL   string
}

// Load reads config from environment variables.
// Example: POSTGRES_URL=... CLICKHOUSE_URL=... -> Config{...}.
func Load() (Config, error) {
	return Parse(os.Getenv)
}

// Parse builds config using getenv, applying defaults and validating required values.
// Example: LISTEN_ADDR="" -> ListenAddr="localhost:3000".
func Parse(getenv func(string) string) (Config, error) {
	databaseURL := strings.TrimSpace(getenv("POSTGRES_URL"))
	if databaseURL == "" {
		return Config{}, errors.New("POSTGRES_URL is not set")
	}

	clickhouseURL := strings.TrimSpace(getenv("CLICKHOUSE_URL"))
	if clickhouseURL == "" {
		return Config{}, errors.New("CLICKHOUSE_URL is not set")
	}

	listenAddr, err := parseListenAddr(getenv("LISTEN_ADDR"))
	if err != nil {
		return Config{}, err
	}

	return Config{
		DatabaseURL:    databaseURL,
		ClickHouseURL:  clickhouseURL,
		ListenAddr:     listenAddr,
		AllowedOrigins: parseAllowedOrigins(getenv("CORS_ALLOWED_ORIGINS")),
		CookieSecure:   getenv("COOKIE_SECURE") == "true",
		DashboardURL:   strings.TrimSuffix(strings.TrimSpace(getenv("DASHBOARD_URL")), "/"),
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
	var origins []string
	for part := range strings.SplitSeq(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			origins = append(origins, trimmed)
		}
	}

	if len(origins) == 0 {
		return []string{defaultAllowedOrigin}
	}

	return origins
}
