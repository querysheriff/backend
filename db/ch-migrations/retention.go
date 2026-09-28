package chmigrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SetRetention updates TTL on all expiring tables, skipping ones already correct.
func SetRetention(ctx context.Context, db *sql.DB, days int) error {
	// Match ClickHouse's normalized TTL text when checking the current schema.
	ttl := fmt.Sprintf("collected_at + toIntervalDay(%d)", days)

	for _, table := range []string{"statement_deltas", "statement_samples", "log_events", "transaction_activity"} {
		var engine string
		if err := db.QueryRowContext(ctx,
			"SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = ?", table,
		).Scan(&engine); err != nil {
			return fmt.Errorf("read %s ttl: %w", table, err)
		}

		// Skip the ALTER when the TTL is already correct.
		if strings.Contains(engine, "TTL "+ttl) {
			continue
		}

		if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", table, ttl)); err != nil {
			return fmt.Errorf("set %s ttl: %w", table, err)
		}
	}

	return nil
}
