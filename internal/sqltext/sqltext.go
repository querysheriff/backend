package sqltext

import (
	"slices"
	"strconv"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

const (
	previewLimit       = 100
	samplePreviewLimit = 120
	alertPreviewLimit  = 80
)

type Result struct {
	Clean   string
	Preview string
	Kind    querysheriffv1.QueryKind
}

// Process normalizes SQL, creates a preview, and classifies it.
// Example: "SELECT  * FROM users" -> {Clean:"SELECT * FROM users", Preview:"SELECT * FROM users", Kind:READS}.
func Process(sql string) Result {
	clean := cleanText(sql)
	summary, err := pg.Summary(sql, previewLimit)
	return Result{
		Clean:   clean,
		Preview: previewText(clean, summary, err),
		Kind:    classify(sql, summary, err),
	}
}

// CleanSample removes SQL comments and normalizes spacing.
// Example: "SELECT * /* x */ FROM users" -> "SELECT * FROM users".
func CleanSample(sql string) string {
	result, err := pg.Scan(sql)
	if err != nil {
		return collapse(sql)
	}

	var b strings.Builder
	b.Grow(len(sql))

	prevEnd := 0
	wrote := false
	for _, tok := range result.GetTokens() {
		if tok.GetToken() == pg.Token_SQL_COMMENT || tok.GetToken() == pg.Token_C_COMMENT {
			continue
		}

		start, end := int(tok.GetStart()), int(tok.GetEnd())
		if start < 0 || end > len(sql) || start >= end {
			continue
		}

		if wrote && start > prevEnd {
			b.WriteByte(' ')
		}
		b.WriteString(sql[start:end])
		prevEnd = end
		wrote = true
	}

	return b.String()
}

// Concretize replaces $n placeholders with matching parameter values.
// Example: Concretize("WHERE id=$1 AND name=$2", ["42", "'Bob'"]) -> "WHERE id=42 AND name='Bob'".
func Concretize(query string, params []string) string {
	if len(params) == 0 {
		return query
	}

	result, err := pg.Scan(query)
	if err != nil {
		return concretizeNaive(query, params)
	}

	var b strings.Builder
	b.Grow(len(query))

	prev := 0
	for _, tok := range result.GetTokens() {
		if tok.GetToken() != pg.Token_PARAM {
			continue
		}

		start, end := int(tok.GetStart()), int(tok.GetEnd())
		if start < prev || end > len(query) {
			continue
		}

		n, convErr := strconv.Atoi(query[start+1 : end])
		if convErr != nil || n < 1 || n > len(params) {
			continue
		}

		b.WriteString(query[prev:start])
		b.WriteString(params[n-1])
		prev = end
	}
	b.WriteString(query[prev:])

	return b.String()
}

func concretizeNaive(query string, params []string) string {
	var b strings.Builder
	for i := 0; i < len(query); {
		if query[i] != '$' || i+1 >= len(query) || query[i+1] < '0' || query[i+1] > '9' {
			b.WriteByte(query[i])
			i++

			continue
		}

		j := i + 1
		for j < len(query) && query[j] >= '0' && query[j] <= '9' {
			j++
		}

		if n, convErr := strconv.Atoi(query[i+1 : j]); convErr == nil && n >= 1 && n <= len(params) {
			b.WriteString(params[n-1])
		} else {
			b.WriteString(query[i:j])
		}
		i = j
	}

	return b.String()
}

// AlertPreview collapses whitespace and truncates a query for an alert message.
// Example: "SELECT  *\nFROM x" -> "SELECT * FROM x".
func AlertPreview(query string) string {
	return capLen(collapse(query), alertPreviewLimit)
}

// SamplePreview cleans comments, inserts parameters, and truncates to 120 characters.
// Example: "SELECT * FROM users WHERE id=$1 -- x", ["42"] -> "SELECT * FROM users WHERE id=42".
func SamplePreview(query string, params []string) string {
	return capLen(Concretize(CleanSample(query), params), samplePreviewLimit)
}

func cleanText(sql string) string {
	tree, err := pg.Parse(sql)
	if err != nil {
		return collapse(sql)
	}
	out, err := pg.Deparse(tree)
	if err != nil {
		return collapse(sql)
	}
	return out
}

func previewText(clean string, summary *pg.SummaryResult, err error) string {
	if err == nil {
		if t := summary.GetTruncatedQuery(); t != "" {
			return t
		}
	}
	return capLen(clean, previewLimit)
}

func classify(sql string, summary *pg.SummaryResult, err error) querysheriffv1.QueryKind {
	if err != nil || isUtility(sql) || isConfigCall(summary) {
		return querysheriffv1.QueryKind_QUERY_KIND_OTHERS
	}

	types := summary.GetStatementTypes()
	for _, t := range types {
		switch t {
		case "InsertStmt", "UpdateStmt", "DeleteStmt", "MergeStmt":
			return querysheriffv1.QueryKind_QUERY_KIND_WRITES
		}
	}
	if slices.Contains(types, "SelectStmt") {
		return querysheriffv1.QueryKind_QUERY_KIND_READS
	}
	return querysheriffv1.QueryKind_QUERY_KIND_OTHERS
}

func isConfigCall(summary *pg.SummaryResult) bool {
	return slices.ContainsFunc(summary.GetFunctions(), func(f *pg.SummaryResult_Function) bool {
		return f.GetFunctionName() == "set_config"
	})
}

func isUtility(sql string) bool {
	flags, err := pg.IsUtilityStmt(sql)
	if err != nil {
		return false
	}
	for _, f := range flags {
		if f {
			return true
		}
	}
	return false
}

func collapse(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

func capLen(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "..."
}
