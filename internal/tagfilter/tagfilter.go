package tagfilter

import (
	"errors"
	"fmt"
)

const (
	MaxFilters  = 20
	MaxValues   = 50
	MaxValueLen = 256
)

type Op uint8

const (
	OpExists Op = iota + 1
	OpEqual
	OpNotEqual
)

type Filter struct {
	Key    string
	Op     Op
	Values []string
}

// Validate checks tag filters for valid keys, operators, values, and limits.
// Example: [{Key:"env", Op:OpEqual, Values:["prod"]}] -> nil.
func Validate(filters []Filter) error {
	if len(filters) > MaxFilters {
		return fmt.Errorf("at most %d tag filters are allowed", MaxFilters)
	}

	for _, filter := range filters {
		if !ValidKey(filter.Key) {
			return fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", filter.Key)
		}

		if err := validateValues(filter); err != nil {
			return err
		}
	}

	return nil
}

func validateValues(filter Filter) error {
	switch filter.Op {
	case OpExists:
		if len(filter.Values) != 0 {
			return errors.New("an exists tag filter must not carry values")
		}

		return nil
	case OpEqual, OpNotEqual:
	default:
		return errors.New("unknown tag filter op")
	}

	if len(filter.Values) == 0 {
		return errors.New("tag filter requires at least one value")
	}

	if len(filter.Values) > MaxValues {
		return fmt.Errorf("a tag filter accepts at most %d values", MaxValues)
	}

	for _, v := range filter.Values {
		if v == "" {
			return errors.New("tag filter values must not be empty")
		}

		if len(v) > MaxValueLen {
			return fmt.Errorf("tag filter values must be at most %d bytes", MaxValueLen)
		}
	}

	return nil
}

// ValidKey reports whether a tag key matches ^[a-z][a-z0-9_]*$.
// Example: "server_name" -> true, "Server-Name" -> false.
func ValidKey(s string) bool {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return false
	}

	for i := 1; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}

	return true
}
