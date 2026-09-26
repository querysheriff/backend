package alerts

import (
	"fmt"
	"time"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/humanize"
	"github.com/querysheriff/backend/internal/sqltext"
)

const (
	activeState        = "active"
	longQueryThreshold = time.Minute
	blockingThreshold  = 10 * time.Second
	openTxnThreshold   = 10 * time.Minute
)

// CheckActivity fires the live alerts one pg_stat_activity sample warrants.
// Example: a query waiting 30s on a lock -> the blocked-query alert fires.
func (n *Notifier) CheckActivity(
	serverName string,
	collectedAt time.Time,
	snapshots []*querysheriffv1.ActivitySnapshot,
) {
	blocked, blockedFor := longestOver(snapshots, blockingThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) (time.Duration, bool) {
			if snap.GetBlockedByPid() == 0 || snap.GetLockWaitStart() == nil {
				return 0, false
			}

			return collectedAt.Sub(snap.GetLockWaitStart().AsTime()), true
		})

	running, runningFor := longestOver(snapshots, longQueryThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) (time.Duration, bool) {
			if snap.GetState() != activeState || snap.GetQueryStart() == nil {
				return 0, false
			}

			return collectedAt.Sub(snap.GetQueryStart().AsTime()), true
		})

	open, openFor := longestOver(snapshots, openTxnThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) (time.Duration, bool) {
			return collectedAt.Sub(snap.GetXactStart().AsTime()), true
		})

	if blocked != nil {
		n.Fire(serverName, KeyBlockedQuery, blockedQueryText(blocked, blockedFor, snapshots))
	}
	if running != nil {
		n.Fire(serverName, KeyLongQuery, sessionText("query", "running", running, runningFor))
	}
	if open != nil {
		n.Fire(serverName, KeyLongTransaction, sessionText("transaction", "open", open, openFor))
	}
}

func longestOver(
	snapshots []*querysheriffv1.ActivitySnapshot,
	threshold time.Duration,
	measure func(*querysheriffv1.ActivitySnapshot) (time.Duration, bool),
) (*querysheriffv1.ActivitySnapshot, time.Duration) {
	var worst *querysheriffv1.ActivitySnapshot
	var longest time.Duration

	for _, snap := range snapshots {
		measured, ok := measure(snap)
		if ok && measured >= threshold && measured > longest {
			worst, longest = snap, measured
		}
	}

	return worst, longest
}

func inDatabase(name string) string {
	if name == "" {
		return ""
	}

	return " in " + name
}

func sessionText(
	subject, verb string,
	snap *querysheriffv1.ActivitySnapshot,
	elapsed time.Duration,
) string {
	return fmt.Sprintf(
		"A %s%s has been %s %s.\n\nquery: %s\npid: %d",
		subject,
		inDatabase(snap.GetDatabaseName()),
		verb,
		humanize.Duration(elapsed),
		sqltext.AlertPreview(snap.GetQuery()),
		snap.GetPid(),
	)
}

func blockedQueryText(
	blocked *querysheriffv1.ActivitySnapshot,
	waited time.Duration,
	snapshots []*querysheriffv1.ActivitySnapshot,
) string {
	blockerQuery := "not captured"
	for _, snap := range snapshots {
		if snap.GetPid() == blocked.GetBlockedByPid() {
			blockerQuery = sqltext.AlertPreview(snap.GetQuery())

			break
		}
	}

	return fmt.Sprintf(
		"A query%s has been waiting %s for a lock.\n\nwaiting: %s\nblocking: %s\npids: %d blocked by %d",
		inDatabase(blocked.GetDatabaseName()),
		humanize.Duration(waited),
		sqltext.AlertPreview(blocked.GetQuery()),
		blockerQuery,
		blocked.GetPid(),
		blocked.GetBlockedByPid(),
	)
}

// CheckLogs fires the crash alert when a log batch shows the server crashing.
// Example: "server process was terminated by signal 11" -> the crash alert fires.
func (n *Notifier) CheckLogs(serverName string, events []*querysheriffv1.LogEvent) {
	for _, event := range events {
		if event.GetLogLevel() == querysheriffv1.LogEvent_LOG_LEVEL_PANIC ||
			event.GetClassification() == querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_CRASHED {
			n.Fire(serverName, KeyPanic, event.GetMessage())

			return
		}
	}
}
