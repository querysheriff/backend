package main

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/rs/cors"

	"github.com/querysheriff/backend/gen/querysheriff/v1/querysheriffv1connect"
	"github.com/querysheriff/backend/internal/alerts"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/config"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/postgres"
	"github.com/querysheriff/backend/internal/server"
	schema "github.com/querysheriff/backend/proto"
)

const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 1 * time.Minute
	idleTimeout       = 2 * time.Minute
	shutdownTimeout   = 10 * time.Second
	readyzTimeout     = 2 * time.Second
	apiPrefix         = "/api"
	maxRequestBytes   = 256 << 20 // 256 MiB
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	queries := db.New(pool)
	handlerOptions := connect.WithOptions(
		connect.WithInterceptors(server.NewAuthInterceptor(queries, cfg.DevAutoLogin)),
		connect.WithReadMaxBytes(maxRequestBytes),
	)

	conn, err := chstats.Connect(ctx, cfg.ClickHouseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	stats := chstats.New(conn)
	notifier := alerts.NewNotifier(queries, logger)
	defer notifier.Wait()

	apiMux := registerServices(pool, queries, stats, notifier, cfg, handlerOptions)

	mux := http.NewServeMux()

	registerHealthEndpoints(mux, pool, conn)

	if err = registerSchemaEndpoint(mux); err != nil {
		return err
	}

	mux.Handle(apiPrefix+"/", http.StripPrefix(apiPrefix, apiMux))

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           withCORS(mux, cfg.AllowedOrigins),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
		Protocols:         &protocols,
	}

	logger.InfoContext(ctx, "querysheriff api listening", "addr", cfg.ListenAddr)

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.ListenAndServe() }()

	select {
	case serveError := <-serveErr:
		if errors.Is(serveError, http.ErrServerClosed) {
			return nil
		}

		return serveError
	case <-ctx.Done():
		logger.InfoContext(ctx, "shutdown signal received, draining connections")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		return httpServer.Shutdown(shutdownCtx)
	}
}

func registerHealthEndpoints(mux *http.ServeMux, pool *pgxpool.Pool, conn driver.Conn) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
		defer cancel()

		if pool.Ping(pingCtx) != nil || conn.Ping(pingCtx) != nil {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

// registerSchemaEndpoint serves the proto sources so clients run `buf generate http://localhost:3000/schema.zip`.
func registerSchemaEndpoint(mux *http.ServeMux) error {
	var archive bytes.Buffer

	zipped := zip.NewWriter(&archive)
	if err := zipped.AddFS(schema.FS); err != nil {
		return err
	}

	if err := zipped.Close(); err != nil {
		return err
	}

	body := archive.Bytes()

	mux.HandleFunc("/schema.zip", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(body)
	})

	return nil
}

func withCORS(handler http.Handler, allowedOrigins []string) http.Handler {
	middleware := cors.New(cors.Options{
		AllowedOrigins:   allowedOrigins,
		AllowedMethods:   connectcors.AllowedMethods(),
		AllowedHeaders:   connectcors.AllowedHeaders(),
		ExposedHeaders:   connectcors.ExposedHeaders(),
		AllowCredentials: true,
	})

	return middleware.Handler(handler)
}

func registerServices(
	pool *pgxpool.Pool,
	queries *db.Queries,
	stats *chstats.Client,
	notifier *alerts.Notifier,
	cfg config.Config,
	handlerOptions connect.Option,
) *http.ServeMux {
	apiMux := http.NewServeMux()

	activityPath, activityHandler := querysheriffv1connect.NewActivityServiceHandler(
		server.NewActivityServer(stats, notifier),
		handlerOptions,
	)
	apiMux.Handle(activityPath, activityHandler)

	statementPath, statementHandler := querysheriffv1connect.NewStatementServiceHandler(
		server.NewStatementServer(stats),
		handlerOptions,
	)
	apiMux.Handle(statementPath, statementHandler)

	logPath, logHandler := querysheriffv1connect.NewLogServiceHandler(
		server.NewLogServer(stats, notifier),
		handlerOptions,
	)
	apiMux.Handle(logPath, logHandler)

	healthPath, healthHandler := querysheriffv1connect.NewHealthServiceHandler(
		server.NewHealthServer(queries),
		handlerOptions,
	)
	apiMux.Handle(healthPath, healthHandler)

	authPath, authHandler := querysheriffv1connect.NewAuthServiceHandler(
		server.NewAuthServer(queries, cfg.CookieSecure),
		handlerOptions,
	)
	apiMux.Handle(authPath, authHandler)

	adminPath, adminHandler := querysheriffv1connect.NewAdminServiceHandler(server.NewAdminServer(pool), handlerOptions)
	apiMux.Handle(adminPath, adminHandler)

	alertPath, alertHandler := querysheriffv1connect.NewAlertServiceHandler(
		server.NewAlertServer(queries),
		handlerOptions,
	)
	apiMux.Handle(alertPath, alertHandler)

	return apiMux
}
