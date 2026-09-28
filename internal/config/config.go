package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	defaultListenAddr    = "localhost:3000"
	defaultAllowedOrigin = "http://localhost:3001"
	defaultRetentionDays = 30
	maxRetentionDays     = 365
)

type Config struct {
	DatabaseURL    string
	ClickHouseURL  string
	ListenAddr     string
	AllowedOrigins []string
	CookieSecure   bool
	DashboardURL   string
	DevAutoLogin   bool
	RetentionDays  int
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

	retentionDays, err := parseRetentionDays(getenv("RETENTION_DAYS"))
	if err != nil {
		return Config{}, err
	}

	devAutoLogin := getenv("DEV_AUTO_LOGIN") == "true"
	host, _, _ := net.SplitHostPort(listenAddr)
	if devAutoLogin && host != "localhost" && !net.ParseIP(host).IsLoopback() {
		return Config{}, fmt.Errorf("DEV_AUTO_LOGIN needs a loopback LISTEN_ADDR, got %q", listenAddr)
	}

	return Config{
		DatabaseURL:    databaseURL,
		ClickHouseURL:  clickhouseURL,
		ListenAddr:     listenAddr,
		AllowedOrigins: parseAllowedOrigins(getenv("CORS_ALLOWED_ORIGINS")),
		CookieSecure:   getenv("COOKIE_SECURE") == "true",
		DashboardURL:   strings.TrimSuffix(strings.TrimSpace(getenv("DASHBOARD_URL")), "/"),
		DevAutoLogin:   devAutoLogin,
		RetentionDays:  retentionDays,
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

func parseRetentionDays(raw string) (int, error) {
	if raw == "" {
		return defaultRetentionDays, nil
	}

	days, err := strconv.Atoi(raw)
	if err != nil || days < 1 || days > maxRetentionDays {
		return 0, fmt.Errorf(
			"RETENTION_DAYS must be a whole number of days from 1 to %d, got %q",
			maxRetentionDays,
			raw,
		)
	}

	return days, nil
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
