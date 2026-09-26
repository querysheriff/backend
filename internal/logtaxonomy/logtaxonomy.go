package logtaxonomy

import (
	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

type group struct {
	category        querysheriffv1.LogCategory
	classifications []querysheriffv1.LogEvent_LogClassification
}

type Categories struct {
	byClassification map[querysheriffv1.LogEvent_LogClassification]querysheriffv1.LogCategory
	byCategory       map[querysheriffv1.LogCategory][]querysheriffv1.LogEvent_LogClassification
	order            []querysheriffv1.LogCategory
}

func New() *Categories {
	groups := append(serverGroups(), statementGroups()...)

	resolved := &Categories{
		byClassification: make(map[querysheriffv1.LogEvent_LogClassification]querysheriffv1.LogCategory),
		byCategory:       make(map[querysheriffv1.LogCategory][]querysheriffv1.LogEvent_LogClassification),
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

// All returns categories in display order.
// Example: [SERVER, CONNECTION, WAL_CHECKPOINT, ...].
func (c *Categories) All() []querysheriffv1.LogCategory {
	return c.order
}

// Of returns the category containing a classification.
// Example: Of(LOCK_TIMEOUT) -> LOCK.
func (c *Categories) Of(
	classification querysheriffv1.LogEvent_LogClassification,
) querysheriffv1.LogCategory {
	return c.byClassification[classification]
}

func (c *Categories) expand(categories []querysheriffv1.LogCategory) []int32 {
	if len(categories) == 0 {
		return nil
	}

	var out []int32

	for _, category := range categories {
		if category == querysheriffv1.LogCategory_LOG_CATEGORY_UNSPECIFIED {
			out = append(out, int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED))

			continue
		}

		for _, classification := range c.byCategory[category] {
			out = append(out, int32(classification))
		}
	}

	return out
}

// Selected combines explicit classifications with all classifications from selected categories, removing duplicates.
// Example: explicit=[LOCK_TIMEOUT], categories=[STATEMENT] -> [LOCK_TIMEOUT, STATEMENT_DURATION, ...].
func (c *Categories) Selected(
	explicit []int32,
	categories []querysheriffv1.LogCategory,
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

func serverGroups() []group {
	return []group{
		{querysheriffv1.LogCategory_LOG_CATEGORY_SERVER, []querysheriffv1.LogEvent_LogClassification{
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
		{querysheriffv1.LogCategory_LOG_CATEGORY_CONNECTION, []querysheriffv1.LogEvent_LogClassification{
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
		{querysheriffv1.LogCategory_LOG_CATEGORY_WAL_CHECKPOINT, []querysheriffv1.LogEvent_LogClassification{
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
		{querysheriffv1.LogCategory_LOG_CATEGORY_AUTOVACUUM, []querysheriffv1.LogEvent_LogClassification{
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
		{querysheriffv1.LogCategory_LOG_CATEGORY_STANDBY, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_RESTORED_WAL_FROM_ARCHIVE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STARTED_STREAMING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STREAMING_INTERRUPTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STOPPED_STREAMING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_CONSISTENT_RECOVERY_STATE,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STANDBY_STATEMENT_CANCELED,
		}},
	}
}

func statementGroups() []group {
	return []group{
		{querysheriffv1.LogCategory_LOG_CATEGORY_LOCK, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_ACQUIRED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_WAITING,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_TIMEOUT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_DETECTED,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_AVOIDED,
		}},
		{querysheriffv1.LogCategory_LOG_CATEGORY_STATEMENT, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_DURATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_CANCELED_TIMEOUT,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_CANCELED_USER,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_LOG,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_AUTO_EXPLAIN,
		}},
		{querysheriffv1.LogCategory_LOG_CATEGORY_CONSTRAINT_VIOLATION, []querysheriffv1.LogEvent_LogClassification{
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNIQUE_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_FOREIGN_KEY_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_NOT_NULL_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_CHECK_CONSTRAINT_VIOLATION,
			querysheriffv1.LogEvent_LOG_CLASSIFICATION_EXCLUSION_CONSTRAINT_VIOLATION,
		}},
		{querysheriffv1.LogCategory_LOG_CATEGORY_APPLICATION_ERROR, []querysheriffv1.LogEvent_LogClassification{
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
