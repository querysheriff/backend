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
	blocked := longestOver(snapshots, blockingThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) time.Duration {
			if snap.GetBlockedByPid() == 0 || snap.GetLockWaitStart() == nil {
				return 0
			}

			return collectedAt.Sub(snap.GetLockWaitStart().AsTime())
		})

	running := longestOver(snapshots, longQueryThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) time.Duration {
			if snap.GetState() != activeState || snap.GetQueryStart() == nil {
				return 0
			}

			return collectedAt.Sub(snap.GetQueryStart().AsTime())
		})

	open := longestOver(snapshots, openTxnThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) time.Duration {
			return collectedAt.Sub(snap.GetXactStart().AsTime())
		})

	if blocked != nil {
		n.Fire(serverName, blocked.GetDatabaseName(), KeyBlockedQuery, blockedQueryText(blocked, snapshots))
	}

	if running != nil {
		n.Fire(serverName, running.GetDatabaseName(), KeyLongQuery,
			fmt.Sprintf("A query has been running more than %s.\n", humanize.Duration(longQueryThreshold))+
				queryBlock("Query", running)+killLine("Kill", running.GetPid()))
	}

	if open != nil {
		n.Fire(serverName, open.GetDatabaseName(), KeyLongTransaction,
			fmt.Sprintf("A transaction has been open more than %s.\n", humanize.Duration(openTxnThreshold))+
				queryBlock("Current query", open)+killLine("Kill", open.GetPid()))
	}
}

// longestOver returns the snapshot with the largest measured duration at or above threshold.
// measure should return 0 for snapshots where the measurement does not apply.
func longestOver(
	snapshots []*querysheriffv1.ActivitySnapshot,
	threshold time.Duration,
	measure func(*querysheriffv1.ActivitySnapshot) time.Duration,
) *querysheriffv1.ActivitySnapshot {
	var worst *querysheriffv1.ActivitySnapshot
	var longest time.Duration

	for _, snap := range snapshots {
		if measured := measure(snap); measured >= threshold && measured > longest {
			worst, longest = snap, measured
		}
	}

	return worst
}

func blockedQueryText(blocked *querysheriffv1.ActivitySnapshot, snapshots []*querysheriffv1.ActivitySnapshot) string {
	blocking := "\n*Blocking:* not captured"

	for _, snap := range snapshots {
		if snap.GetPid() == blocked.GetBlockedByPid() {
			blocking = queryBlock("Blocking", snap)

			break
		}
	}

	return fmt.Sprintf("A query has been waiting more than %s for a lock.\n", humanize.Duration(blockingThreshold)) +
		queryBlock("Waiting", blocked) + blocking + killLine("Kill blocking", blocked.GetBlockedByPid())
}

func queryBlock(label string, snap *querysheriffv1.ActivitySnapshot) string {
	return fmt.Sprintf("\n*%s:*%s\n```%s```", label, tagPills(snap.GetQueryTags()),
		slackEscape(sqltext.AlertQuery(snap.GetQuery())))
}

func killLine(label string, pid int32) string {
	return fmt.Sprintf("\n*%s:* `SELECT pg_terminate_backend(%d);`", label, pid)
}

// CheckLogs fires the crash alert when a log batch shows the server crashing.
// Example: "server process was terminated by signal 11" -> the crash alert fires.
func (n *Notifier) CheckLogs(serverName string, events []*querysheriffv1.LogEvent) {
	for _, event := range events {
		if event.GetLogLevel() == querysheriffv1.LogEvent_LOG_LEVEL_PANIC ||
			event.GetClassification() == querysheriffv1.LogEvent_LOG_CLASSIFICATION_SERVER_CRASHED {
			n.Fire(serverName, "", KeyPanic, slackEscape(event.GetMessage()))

			return
		}
	}
}
