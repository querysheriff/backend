package alerts

import "time"

type Level int

const (
	LevelInfo Level = iota
	LevelWarning
	LevelCritical
)

const (
	KeyMonitoringStopped = "monitoring_stopped"
	KeyPanic             = "panic"
	KeyBlockedQuery      = "blocked_query"
	KeyLongQuery         = "long_query"
	KeyLongTransaction   = "long_transaction"
	KeySlowQueryReport   = "slow_query_report"
	KeyWeeklyReport      = "weekly_report"
)

const (
	liveProblemCooldown = time.Minute
	openTxnCooldown     = 10 * time.Minute
	crashCooldown       = 15 * time.Minute
	monitoringCooldown  = 30 * time.Minute
	dailyCadence        = 23 * time.Hour
	weeklyCadence       = 6 * 24 * time.Hour
)

const FireHistoryWindow = 7 * 24 * time.Hour

type Def struct {
	Key      string
	Title    string
	Level    Level
	Cooldown time.Duration
}

func defs() []Def {
	return []Def{
		{KeyMonitoringStopped, "Monitoring stopped", LevelCritical, monitoringCooldown},
		{KeyPanic, "Database crashed", LevelCritical, crashCooldown},
		{KeyBlockedQuery, "Query blocked by a lock", LevelWarning, liveProblemCooldown},
		{KeyLongQuery, "Query running too long", LevelWarning, liveProblemCooldown},
		{KeyLongTransaction, "Transaction open too long", LevelWarning, openTxnCooldown},
		{KeySlowQueryReport, "Daily slow query report", LevelInfo, dailyCadence},
		{KeyWeeklyReport, "Weekly report", LevelInfo, weeklyCadence},
	}
}

func Catalog() []Def {
	return defs()
}

func IsKnownKey(key string) bool {
	_, ok := defByKey(key)

	return ok
}

func defByKey(key string) (Def, bool) {
	for _, def := range defs() {
		if def.Key == key {
			return def, true
		}
	}

	return Def{}, false
}
