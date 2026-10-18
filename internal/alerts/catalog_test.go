package alerts_test

import (
	"testing"

	"github.com/querysheriff/backend/internal/alerts"
)

func TestCooldownsFitInTheFireHistory(t *testing.T) {
	t.Parallel()

	for _, def := range alerts.Catalog() {
		if def.Cooldown > alerts.FireHistoryWindow {
			t.Errorf("%s cooldown %s outlives the %s fire history", def.Key, def.Cooldown, alerts.FireHistoryWindow)
		}
	}
}
