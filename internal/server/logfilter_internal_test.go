package server

import (
	"testing"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// The enum's own order follows log_min_messages, where LOG outranks ERROR. Mirrors LEVEL_ROWS.
func TestLogSeverityRanksOrderBySeriousness(t *testing.T) {
	t.Parallel()

	ranks := logSeverityRanks()

	if len(ranks) != len(querysheriffv1.LogEvent_LogLevel_name) {
		t.Fatalf("ranked %d levels, want %d", len(ranks), len(querysheriffv1.LogEvent_LogLevel_name))
	}

	worstFirst := []querysheriffv1.LogEvent_LogLevel{
		querysheriffv1.LogEvent_LOG_LEVEL_PANIC,
		querysheriffv1.LogEvent_LOG_LEVEL_FATAL,
		querysheriffv1.LogEvent_LOG_LEVEL_ERROR,
		querysheriffv1.LogEvent_LOG_LEVEL_WARNING,
		querysheriffv1.LogEvent_LOG_LEVEL_NOTICE,
		querysheriffv1.LogEvent_LOG_LEVEL_LOG,
		querysheriffv1.LogEvent_LOG_LEVEL_INFO,
		querysheriffv1.LogEvent_LOG_LEVEL_DEBUG,
		querysheriffv1.LogEvent_LOG_LEVEL_UNSPECIFIED,
	}

	if len(worstFirst) != len(ranks) {
		t.Fatalf("test covers %d levels, want %d", len(worstFirst), len(ranks))
	}

	for i := 1; i < len(worstFirst); i++ {
		above, below := worstFirst[i-1], worstFirst[i]
		if ranks[above] <= ranks[below] {
			t.Errorf("%s ranks %d, not above %s at %d", above, ranks[above], below, ranks[below])
		}
	}
}
