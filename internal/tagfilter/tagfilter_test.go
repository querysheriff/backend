package tagfilter_test

import (
	"strings"
	"testing"

	"github.com/querysheriff/backend/internal/tagfilter"
)

func TestValidateAcceptsWellFormedFilters(t *testing.T) {
	t.Parallel()

	filters := []tagfilter.Filter{
		{Key: "service", Op: tagfilter.OpEqual, Values: []string{"checkout"}},
		{Key: "route_2", Op: tagfilter.OpNotEqual, Values: []string{"/health", "/metrics"}},
		{Key: "team", Op: tagfilter.OpExists},
	}

	if err := tagfilter.Validate(filters); err != nil {
		t.Errorf("Validate(%+v) = %v, want nil", filters, err)
	}
}

func TestValidateRejectsMalformedFilters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter tagfilter.Filter
	}{
		{"missing op", tagfilter.Filter{Key: "service", Values: []string{"a"}}},
		{"exists with values", tagfilter.Filter{Key: "service", Op: tagfilter.OpExists, Values: []string{"a"}}},
		{"equal without values", tagfilter.Filter{Key: "service", Op: tagfilter.OpEqual}},
		{"not equal without values", tagfilter.Filter{Key: "service", Op: tagfilter.OpNotEqual}},
		{"empty value", tagfilter.Filter{Key: "service", Op: tagfilter.OpEqual, Values: []string{""}}},
		{"uppercase key", tagfilter.Filter{Key: "Service", Op: tagfilter.OpExists}},
		{"empty key", tagfilter.Filter{Key: "", Op: tagfilter.OpExists}},
		{"leading digit", tagfilter.Filter{Key: "2service", Op: tagfilter.OpExists}},
		{"dash in key", tagfilter.Filter{Key: "ser-vice", Op: tagfilter.OpExists}},
		{
			"oversized value",
			tagfilter.Filter{
				Key: "service", Op: tagfilter.OpEqual,
				Values: []string{strings.Repeat("x", tagfilter.MaxValueLen+1)},
			},
		},
		{
			"too many values",
			tagfilter.Filter{
				Key: "service", Op: tagfilter.OpEqual,
				Values: make([]string, tagfilter.MaxValues+1),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if err := tagfilter.Validate([]tagfilter.Filter{c.filter}); err == nil {
				t.Errorf("Validate(%+v) = nil, want an error", c.filter)
			}
		})
	}
}

func TestValidateCapsTheNumberOfFilters(t *testing.T) {
	t.Parallel()

	filters := make([]tagfilter.Filter, tagfilter.MaxFilters+1)
	for i := range filters {
		filters[i] = tagfilter.Filter{Key: "service", Op: tagfilter.OpExists}
	}

	if err := tagfilter.Validate(filters); err == nil {
		t.Errorf("Validate(%d filters) = nil, want an error", len(filters))
	}

	if err := tagfilter.Validate(filters[:tagfilter.MaxFilters]); err != nil {
		t.Errorf("Validate(%d filters) = %v, want nil", tagfilter.MaxFilters, err)
	}
}

func TestValidKey(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"a", "service", "service_2", "a0_"} {
		if !tagfilter.ValidKey(key) {
			t.Errorf("ValidKey(%q) = false, want true", key)
		}
	}

	for _, key := range []string{"", "A", "0a", "_a", "a-b", "a.b", "a b", "ä"} {
		if tagfilter.ValidKey(key) {
			t.Errorf("ValidKey(%q) = true, want false", key)
		}
	}
}
