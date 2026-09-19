package config_test

import (
	"slices"
	"testing"

	"github.com/querysheriff/backend/internal/config"
)

func env(pairs map[string]string) func(string) string {
	return func(key string) string { return pairs[key] }
}

func requiredAPIEnv() map[string]string {
	return map[string]string{
		"POSTGRES_URL":   "postgres://localhost/qs",
		"CLICKHOUSE_URL": "clickhouse://localhost:9000",
	}
}

func TestParseAPIRequiresBothStores(t *testing.T) {
	t.Parallel()

	for _, missing := range []string{"POSTGRES_URL", "CLICKHOUSE_URL"} {
		pairs := requiredAPIEnv()
		delete(pairs, missing)

		if _, err := config.ParseAPI(env(pairs)); err == nil {
			t.Errorf("ParseAPI without %s = nil error, want a failure", missing)
		}
	}

	if _, err := config.ParseAPI(env(requiredAPIEnv())); err != nil {
		t.Errorf("ParseAPI with both stores = %v, want nil", err)
	}
}

func TestParseAPIListenAddr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{"defaults when unset", "", "localhost:3000", false},
		{"host and port", "0.0.0.0:3000", "0.0.0.0:3000", false},
		{"port only", ":8080", ":8080", false},
		{"missing port", "0.0.0.0", "", true},
		{"not an address", "http://localhost:3000", "", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			pairs := requiredAPIEnv()
			pairs["LISTEN_ADDR"] = c.raw

			cfg, err := config.ParseAPI(env(pairs))
			if c.wantErr {
				if err == nil {
					t.Errorf("ParseAPI(LISTEN_ADDR=%q) = nil error, want a failure", c.raw)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseAPI(LISTEN_ADDR=%q): %v", c.raw, err)
			}

			if cfg.ListenAddr != c.want {
				t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, c.want)
			}
		})
	}
}

func TestParseAPIAllowedOrigins(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"defaults when unset", "", []string{"http://localhost:3001"}},
		{"single origin", "https://qs.example", []string{"https://qs.example"}},
		{
			"trims and splits",
			" https://a.example , https://b.example ",
			[]string{"https://a.example", "https://b.example"},
		},
		{"drops empty entries", "https://a.example,,  ,", []string{"https://a.example"}},
		{"blank falls back to the default", "  ,  ", []string{"http://localhost:3001"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			pairs := requiredAPIEnv()
			pairs["CORS_ALLOWED_ORIGINS"] = c.raw

			cfg, err := config.ParseAPI(env(pairs))
			if err != nil {
				t.Fatalf("ParseAPI(CORS_ALLOWED_ORIGINS=%q): %v", c.raw, err)
			}

			if !slices.Equal(cfg.AllowedOrigins, c.want) {
				t.Errorf("AllowedOrigins = %v, want %v", cfg.AllowedOrigins, c.want)
			}
		})
	}
}

func TestParseAPICookieSecureIsOptIn(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]bool{"": false, "false": false, "1": false, "TRUE": false, "true": true} {
		pairs := requiredAPIEnv()
		pairs["COOKIE_SECURE"] = raw

		cfg, err := config.ParseAPI(env(pairs))
		if err != nil {
			t.Fatalf("ParseAPI(COOKIE_SECURE=%q): %v", raw, err)
		}

		if cfg.CookieSecure != want {
			t.Errorf("COOKIE_SECURE=%q gave CookieSecure = %v, want %v", raw, cfg.CookieSecure, want)
		}
	}
}

func TestParseJobs(t *testing.T) {
	t.Parallel()

	for _, missing := range []string{"POSTGRES_URL", "CLICKHOUSE_URL"} {
		pairs := requiredAPIEnv()
		delete(pairs, missing)

		if _, err := config.ParseJobs(env(pairs)); err == nil {
			t.Errorf("ParseJobs without %s = nil error, want a failure", missing)
		}
	}

	pairs := requiredAPIEnv()
	pairs["DASHBOARD_URL"] = "  https://qs.example/  "

	cfg, err := config.ParseJobs(env(pairs))
	if err != nil {
		t.Fatalf("ParseJobs: %v", err)
	}

	if want := "https://qs.example"; cfg.DashboardURL != want {
		t.Errorf("DashboardURL = %q, want %q (trimmed, no trailing slash)", cfg.DashboardURL, want)
	}
}
