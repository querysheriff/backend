package logtaxonomy_test

import (
	"slices"
	"testing"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/logtaxonomy"
)

func TestEveryClassificationHasACategory(t *testing.T) {
	t.Parallel()

	categories := logtaxonomy.New()

	for number, name := range querysheriffv1.LogEvent_LogClassification_name {
		classification := querysheriffv1.LogEvent_LogClassification(number)
		if classification == querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED {
			continue
		}

		if categories.Of(classification) == querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED {
			t.Errorf("%s has no category", name)
		}
	}
}

func TestUnspecifiedClassificationHasNoCategory(t *testing.T) {
	t.Parallel()

	got := logtaxonomy.New().Of(querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED)
	if got != querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED {
		t.Errorf("unspecified classification = %s, want the unspecified category", got)
	}
}

func TestEveryCategoryIsListedOnceAndSelectsSomething(t *testing.T) {
	t.Parallel()

	categories := logtaxonomy.New()

	all := categories.All()
	for i, category := range all {
		if slices.Index(all, category) != i {
			t.Errorf("%s is listed twice by All()", category)
		}

		selected := categories.Selected(nil, []querysheriffv1.LogEvent_LogCategory{category})
		if len(selected) == 0 {
			t.Errorf("%s selects no classification", category)
		}

		for _, classification := range selected {
			got := categories.Of(querysheriffv1.LogEvent_LogClassification(classification))
			if got != category {
				t.Errorf("%s selected classification %d, which belongs to %s", category, classification, got)
			}
		}
	}

	for number, name := range querysheriffv1.LogEvent_LogCategory_name {
		category := querysheriffv1.LogEvent_LogCategory(number)
		if category == querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED {
			continue
		}

		if !slices.Contains(all, category) {
			t.Errorf("%s is missing from All()", name)
		}
	}
}

func TestSelectedUnionsCategoriesWithExplicitClassifications(t *testing.T) {
	t.Parallel()

	categories := logtaxonomy.New()
	explicit := int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_CHECKPOINT_COMPLETE)

	selected := categories.Selected(
		[]int32{explicit, explicit},
		[]querysheriffv1.LogEvent_LogCategory{
			querysheriffv1.LogEvent_LOG_CATEGORY_LOCK,
			querysheriffv1.LogEvent_LOG_CATEGORY_LOCK,
		},
	)

	if !slices.Contains(selected, explicit) {
		t.Errorf("Selected() = %v, want it to keep the explicit classification %d", selected, explicit)
	}

	if !slices.Contains(selected, int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_WAITING)) {
		t.Errorf("Selected() = %v, want it to expand the lock category", selected)
	}

	for i, classification := range selected {
		if slices.Index(selected, classification) != i {
			t.Errorf("Selected() = %v, want no duplicates", selected)

			break
		}
	}
}

func TestSelectedIsEmptyWhenNothingIsAskedFor(t *testing.T) {
	t.Parallel()

	if got := logtaxonomy.New().Selected(nil, nil); got != nil {
		t.Errorf("Selected(nil, nil) = %v, want nil so the query stays unfiltered", got)
	}
}

func TestUnspecifiedCategorySelectsOnlyTheUnclassified(t *testing.T) {
	t.Parallel()

	got := logtaxonomy.New().Selected(nil, []querysheriffv1.LogEvent_LogCategory{
		querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED,
	})

	want := []int32{int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED)}
	if !slices.Equal(got, want) {
		t.Errorf("Selected(unspecified) = %v, want %v", got, want)
	}
}

func TestRanksGroupClassificationsByCategoryOrder(t *testing.T) {
	t.Parallel()

	categories := logtaxonomy.New()
	ranks := categories.Ranks()
	all := categories.All()

	for number := range querysheriffv1.LogEvent_LogClassification_name {
		classification := querysheriffv1.LogEvent_LogClassification(number)
		if int(classification) >= len(ranks) {
			t.Fatalf("Ranks() is %d long, too short for classification %d", len(ranks), classification)
		}

		category := categories.Of(classification)

		want := len(all)
		if index := slices.Index(all, category); index >= 0 {
			want = index
		}

		if got := int(ranks[classification]); got != want {
			t.Errorf("classification %d ranks %d, want %d (%s)", classification, got, want, category)
		}
	}
}

func TestSeverityRanksOrderBySeriousness(t *testing.T) {
	t.Parallel()

	ranks := logtaxonomy.SeverityRanks()

	order := []querysheriffv1.LogEvent_LogLevel{
		querysheriffv1.LogEvent_LOG_LEVEL_UNSPECIFIED,
		querysheriffv1.LogEvent_LOG_LEVEL_DEBUG,
		querysheriffv1.LogEvent_LOG_LEVEL_INFO,
		querysheriffv1.LogEvent_LOG_LEVEL_LOG,
		querysheriffv1.LogEvent_LOG_LEVEL_NOTICE,
		querysheriffv1.LogEvent_LOG_LEVEL_WARNING,
		querysheriffv1.LogEvent_LOG_LEVEL_ERROR,
		querysheriffv1.LogEvent_LOG_LEVEL_FATAL,
		querysheriffv1.LogEvent_LOG_LEVEL_PANIC,
	}

	if len(ranks) != len(querysheriffv1.LogEvent_LogLevel_name) {
		t.Fatalf("SeverityRanks() is %d long, want one entry per level (%d)",
			len(ranks), len(querysheriffv1.LogEvent_LogLevel_name))
	}

	if len(order) != len(ranks) {
		t.Fatalf("the test lists %d levels but the proto has %d", len(order), len(ranks))
	}

	for i := 1; i < len(order); i++ {
		if ranks[order[i]] <= ranks[order[i-1]] {
			t.Errorf("%s ranks %d, not above %s at %d",
				order[i], ranks[order[i]], order[i-1], ranks[order[i-1]])
		}
	}
}
