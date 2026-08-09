package server

import (
	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// Ninety-eight classifications are too many to browse or to stack in a chart, so the LOGS
// filter and both heatmaps work in categories first and drill into classifications second.
//
// The nine categories and their membership are pganalyze's Log Insights taxonomy rather than
// one of our own, so their per-code documentation is the reference for what a classification
// here means. Each group is annotated with the pganalyze code range it mirrors, in their order,
// so a diff against https://pganalyze.com/docs/log-insights is mechanical.
//
// Two deliberate departures from one-to-one:
//
//   - C21 "Connection authorized" is one message there; Postgres 14 added a separate
//     "connection authenticated" line, which the collector classifies on its own. Both sit
//     under Connection.
//   - W46 "Invalid WAL timeline" is a WAL & Checkpoint code for pganalyze even though our
//     classification is named STANDBY_INVALID_TIMELINE. It follows the code, not the name.
type logCategoryGroup struct {
	category        querysheriffv1.LogEvent_LogCategory
	classifications []querysheriffv1.LogEvent_LogClassification
}

// logCategories is the resolved two-way mapping between a classification and its category,
// built once per server rather than as a package variable.
type logCategories struct {
	byClassification map[querysheriffv1.LogEvent_LogClassification]querysheriffv1.LogEvent_LogCategory
	byCategory       map[querysheriffv1.LogEvent_LogCategory][]querysheriffv1.LogEvent_LogClassification
	order            []querysheriffv1.LogEvent_LogCategory
}

func newLogCategories() *logCategories {
	groups := append(serverLogCategoryGroups(), statementLogCategoryGroups()...)

	resolved := &logCategories{
		byClassification: make(map[querysheriffv1.LogEvent_LogClassification]querysheriffv1.LogEvent_LogCategory),
		byCategory:       make(map[querysheriffv1.LogEvent_LogCategory][]querysheriffv1.LogEvent_LogClassification),
	}

	for _, group := range groups {
		resolved.byCategory[group.category] = group.classifications
		resolved.order = append(resolved.order, group.category)

		for _, classification := range group.classifications {
			resolved.byClassification[classification] = group.category
		}
	}

	return resolved
}

// all returns every category in declaration order, so the picker and the heatmap can list one
// that produced nothing rather than silently omitting it.
func (c *logCategories) all() []querysheriffv1.LogEvent_LogCategory {
	return c.order
}

// categoryOf returns UNSPECIFIED for a classification with no category, which is also how
// an unrecognised log message arrives from the collector.
func (c *logCategories) categoryOf(
	classification querysheriffv1.LogEvent_LogClassification,
) querysheriffv1.LogEvent_LogCategory {
	return c.byClassification[classification]
}

// expand turns a category selection into the classifications it covers, so the category filter
// needs no column of its own.
func (c *logCategories) expand(categories []querysheriffv1.LogEvent_LogCategory) []int32 {
	if len(categories) == 0 {
		return nil
	}

	var out []int32

	for _, category := range categories {
		// Uncategorized is real to filter on but is not one of the declared groups, so
		// byCategory has no entry for it. Without this arm it expands to nothing, `selected`
		// returns nil, and nil means "every classification" — so the filter would silently
		// turn itself off.
		if category == querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED {
			out = append(out, int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED))

			continue
		}

		for _, classification := range c.byCategory[category] {
			out = append(out, int32(classification))
		}
	}

	return out
}

// selected unions the explicitly picked classifications with everything the picked categories
// cover: picking Lock and "Syntax error" asks for both, not their (empty) intersection. Nil
// means every classification.
func (c *logCategories) selected(
	explicit []int32,
	categories []querysheriffv1.LogEvent_LogCategory,
) []int32 {
	expanded := c.expand(categories)
	if len(explicit) == 0 && len(expanded) == 0 {
		return nil
	}

	seen := make(map[int32]struct{}, len(explicit)+len(expanded))
	out := make([]int32, 0, len(explicit)+len(expanded))

	for _, classification := range append(explicit, expanded...) {
		if _, ok := seen[classification]; ok {
			continue
		}

		seen[classification] = struct{}{}
		out = append(out, classification)
	}

	return out
}

// orderingArray is the mapping as an array indexed by classification, so ordering by category
// is `category_of[classification + 1]` in SQL and the taxonomy stays in Go. The value is the
// category's position in `order`, not its enum number, so sorting groups them as the UI lists
// them.
func (c *logCategories) orderingArray() []int32 {
	rank := make(map[querysheriffv1.LogEvent_LogCategory]int32, len(c.order))

	var next int32
	for _, category := range c.order {
		rank[category] = next
		next++
	}

	var highest int32
	for classification := range c.byClassification {
		if int32(classification) > highest {
			highest = int32(classification)
		}
	}

	// Anything without a category — index 0 is the UNSPECIFIED classification — sorts last.
	out := make([]int32, highest+1)
	for i := range out {
		out[i] = next
	}

	for classification, category := range c.byClassification {
		out[classification] = rank[category]
	}

	return out
}

// serverLogCategoryGroups covers what the server does on its own behalf: pganalyze's
// Server, Connections, WAL & Checkpoints, Autovacuum and Standby Servers.
func serverLogCategoryGroups() []logCategoryGroup {
	return []logCategoryGroup{
		// S1 to S11.
		{querysheriffv1.LogEvent_LOG_CATEGORY_SERVER, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_CRASHED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_START,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_START_RECOVERING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_SHUTDOWN,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_OUT_OF_MEMORY,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_INVALID_CHECKSUM,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_TEMP_FILE_CREATED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_MISC,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_RELOAD,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_PROCESS_EXITED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_STATS_COLLECTOR_TIMEOUT,
		}},
		// C20 to C33, plus the Postgres 14 "connection authenticated" line.
		{querysheriffv1.LogEvent_LOG_CATEGORY_CONNECTION, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_RECEIVED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_AUTHORIZED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_AUTHENTICATED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_REJECTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_DISCONNECTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_CLIENT_FAILED_TO_CONNECT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_LOST,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_LOST_OPEN_TX,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CONNECTION_TERMINATED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_OUT_OF_CONNECTIONS,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_TOO_MANY_CONNECTIONS_ROLE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COULD_NOT_ACCEPT_SSL_CONNECTION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_PROTOCOL_ERROR_UNSUPPORTED_VERSION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_PROTOCOL_ERROR_INCOMPLETE_MESSAGE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_TOO_MANY_CONNECTIONS_DATABASE,
		}},
		// W40 to W46 and W50 to W53. W46 is the invalid *timeline*, a WAL code for pganalyze
		// despite our classification carrying a STANDBY_ name.
		{querysheriffv1.LogEvent_LOG_CATEGORY_WAL_CHECKPOINT, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CHECKPOINT_STARTING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CHECKPOINT_COMPLETE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CHECKPOINT_TOO_FREQUENT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_RESTARTPOINT_STARTING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_RESTARTPOINT_COMPLETE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_RESTARTPOINT_AT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_INVALID_TIMELINE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_WAL_INVALID_RECORD_LENGTH,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_WAL_REDO,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_WAL_ARCHIVE_COMMAND_FAILED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_WAL_BASE_BACKUP_COMPLETE,
		}},
		// A60 to A68.
		{querysheriffv1.LogEvent_LOG_CATEGORY_AUTOVACUUM, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_AUTOVACUUM_CANCEL,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_TXID_WRAPAROUND_WARNING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_TXID_WRAPAROUND_ERROR,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_AUTOVACUUM_LAUNCHER_STARTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_AUTOVACUUM_LAUNCHER_SHUTTING_DOWN,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_AUTOVACUUM_COMPLETED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_AUTOANALYZE_COMPLETED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SKIPPING_VACUUM_LOCK_NOT_AVAILABLE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SKIPPING_ANALYZE_LOCK_NOT_AVAILABLE,
		}},
		// B90 to B95.
		{querysheriffv1.LogEvent_LOG_CATEGORY_STANDBY, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_RESTORED_WAL_FROM_ARCHIVE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STARTED_STREAMING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STREAMING_INTERRUPTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STOPPED_STREAMING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_CONSISTENT_RECOVERY_STATE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STATEMENT_CANCELED,
		}},
	}
}

// statementLogCategoryGroups covers what a client statement caused: pganalyze's Locks,
// Statements, Constraint Violations and Application Errors.
func statementLogCategoryGroups() []logCategoryGroup {
	return []logCategoryGroup{
		// L70 to L74.
		{querysheriffv1.LogEvent_LOG_CATEGORY_LOCK, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_ACQUIRED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_WAITING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_TIMEOUT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_DETECTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_AVOIDED,
		}},
		// T80 to T84.
		{querysheriffv1.LogEvent_LOG_CATEGORY_STATEMENT, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_DURATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_CANCELED_TIMEOUT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_CANCELED_USER,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_LOG,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_AUTO_EXPLAIN,
		}},
		// V100 to V104.
		{querysheriffv1.LogEvent_LOG_CATEGORY_CONSTRAINT_VIOLATION, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNIQUE_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_FOREIGN_KEY_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_NOT_NULL_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CHECK_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_EXCLUSION_CONSTRAINT_VIOLATION,
		}},
		// U110 to U140.
		{querysheriffv1.LogEvent_LOG_CATEGORY_APPLICATION_ERROR, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SYNTAX_ERROR,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INVALID_INPUT_SYNTAX,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_VALUE_TOO_LONG_FOR_TYPE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INVALID_VALUE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_MALFORMED_ARRAY_LITERAL,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_SUBQUERY_MISSING_ALIAS,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INSERT_TARGET_COLUMN_MISMATCH,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_ANY_ALL_REQUIRES_ARRAY,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COLUMN_MISSING_FROM_GROUP_BY,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_RELATION_DOES_NOT_EXIST,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COLUMN_DOES_NOT_EXIST,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_OPERATOR_DOES_NOT_EXIST,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COLUMN_REFERENCE_AMBIGUOUS,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_PERMISSION_DENIED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_TRANSACTION_IS_ABORTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_ON_CONFLICT_NO_CONSTRAINT_MATCH,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_ON_CONFLICT_ROW_AFFECTED_TWICE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COLUMN_CANNOT_BE_CAST,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_DIVISION_BY_ZERO,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CANNOT_DROP,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INTEGER_OUT_OF_RANGE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INVALID_REGEXP,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_PARAM_MISSING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_FUNCTION_DOES_NOT_EXIST,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_NO_SUCH_SAVEPOINT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNTERMINATED_QUOTED_STRING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNTERMINATED_QUOTED_IDENTIFIER,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INVALID_BYTE_SEQUENCE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COULD_NOT_SERIALIZE_REPEATABLE_READ,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_COULD_NOT_SERIALIZE_SERIALIZABLE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_INCONSISTENT_RANGE_BOUNDS,
		}},
	}
}
