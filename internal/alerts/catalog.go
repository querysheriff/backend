package alerts

import (
	"time"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

const (
	info     = querysheriffv1.AlertLevel_ALERT_LEVEL_INFO
	warning  = querysheriffv1.AlertLevel_ALERT_LEVEL_WARNING
	critical = querysheriffv1.AlertLevel_ALERT_LEVEL_CRITICAL
)

const (
	KeyMonitoringStopped = "monitoring_stopped"
	KeyPanic             = "panic"
	KeyBlockedQuery      = "blocked_query"
	KeyLongQuery         = "long_query"
	KeyLongTransaction   = "long_transaction"
	KeyWeeklyReport      = "weekly_report"
)

const (
	liveProblemCooldown = time.Minute
	openTxnCooldown     = 10 * time.Minute
	crashCooldown       = 15 * time.Minute
	monitoringCooldown  = 30 * time.Minute
	weeklyCadence       = 6 * 24 * time.Hour
)

const FireHistoryWindow = 7 * 24 * time.Hour

type Def struct {
	Key      string
	Title    string
	Level    querysheriffv1.AlertLevel
	Cooldown time.Duration
}

func Catalog() []Def {
	return []Def{
		{KeyMonitoringStopped, "Monitoring stopped", critical, monitoringCooldown},
		{KeyPanic, "Database crashed", critical, crashCooldown},
		{KeyBlockedQuery, "Query blocked by a lock", warning, liveProblemCooldown},
		{KeyLongQuery, "Query running too long", warning, liveProblemCooldown},
		{KeyLongTransaction, "Transaction open too long", warning, openTxnCooldown},
		{KeyWeeklyReport, "Weekly report", info, weeklyCadence},
	}
}

func IsKnownKey(key string) bool {
	_, ok := defByKey(key)

	return ok
}

func defByKey(key string) (Def, bool) {
	for _, def := range Catalog() {
		if def.Key == key {
			return def, true
		}
	}

	return Def{}, false
}
