package clickhouse

import "strings"

type conditions struct {
	where []string
	args  []any
}

func (c *conditions) add(clause string, args ...any) {
	c.where = append(c.where, clause)
	c.args = append(c.args, args...)
}

func (c *conditions) sql() string {
	return strings.Join(c.where, "\n  AND ")
}

func (c *conditions) with(extra ...any) []any {
	args := make([]any, 0, len(c.args)+len(extra))
	args = append(args, c.args...)

	return append(args, extra...)
}
