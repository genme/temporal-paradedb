package paradedb

import (
	"strings"
	"testing"

	"github.com/temporalio/sqlparser"
)

func TestExpressionConverter(t *testing.T) {
	c := &ExpressionConverter{}

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{
			name:  "Simple AND",
			query: "col = 'foo' AND col = 'bar'",
			// Previously: BuildMust(...)
			want: "paradedb.boolean(must => ARRAY[paradedb.term('col','foo'), paradedb.term('col','bar')])",
		},
		{
			name:  "Simple OR",
			query: "col = 'foo' OR col = 'bar'",
			// Previously: BuildShould(...)
			want: "paradedb.boolean(should => ARRAY[paradedb.term('col','foo'), paradedb.term('col','bar')])",
		},
		{
			name:  "NOT equality",
			query: "NOT col = 'foo'",
			// BuildMustNot(...)
			want: "paradedb.boolean(must_not => ARRAY[paradedb.term('col','foo')])",
		},
		{
			name:  "IS NULL",
			query: "col IS NULL",
			// BuildMustNot with exists
			want: "paradedb.boolean(must_not => ARRAY[paradedb.exists('col')])",
		},
		{
			name:  "IS NOT NULL",
			query: "col IS NOT NULL",
			// paradedb.exists('col')
			want: "paradedb.exists('col')",
		},
		{
			name:  "Equality",
			query: "col = 'value'",
			// BuildTermExpr(...)
			want: "paradedb.term('col','value')",
		},
		{
			name:  "Inequality",
			query: "col != 10",
			// BuildMustNot on paradedb.term(...)
			want: "paradedb.boolean(must_not => ARRAY[paradedb.term('col','10')])",
		},
		{
			name:  "IN list",
			query: "col IN ('a','b','c')",
			// BuildShould(...)
			want: "paradedb.boolean(should => ARRAY[paradedb.term('col','a'), paradedb.term('col','b'), paradedb.term('col','c')])",
		},
		{
			name:  "NOT IN list",
			query: "col NOT IN ('a','b')",
			// BuildMustNot(BuildShould(...))
			want: "paradedb.boolean(must_not => ARRAY[paradedb.boolean(should => ARRAY[paradedb.term('col','a'), paradedb.term('col','b')])])",
		},
		{
			name:  "LIKE",
			query: "col LIKE 'foo%'",
			// BuildTermExpr(...)
			want: "paradedb.term('col','foo%')",
		},
		{
			name:  "NOT LIKE",
			query: "col NOT LIKE '%bar%'",
			// BuildMustNot(BuildTermExpr(...))
			want: "paradedb.boolean(must_not => ARRAY[paradedb.term('col','%bar%')])",
		},
		{
			name:  "GreaterThan",
			query: "col > 100",
			// BuildRangeExpr(...)
			want: "paradedb.range('col', '(\"100\", null)')",
		},
		{
			name:  "GreaterEqual",
			query: "col >= 1.5",
			want:  "paradedb.range('col', '[\"1.5\", null)')",
		},
		{
			name:  "LessThan",
			query: "col < 'z'",
			want:  "paradedb.range('col', '(null, \"z\")')",
		},
		{
			name:  "LessEqual",
			query: "col <= 999",
			want:  "paradedb.range('col', '(null, \"999\"]')",
		},
		{
			name:    "Unsupported operator",
			query:   "col REGEXP 'abc'",
			wantErr: true,
		},
		{
			name:  "Parentheses",
			query: "(col = 'test')",
			// Just returns term
			want: "paradedb.term('col','test')",
		},
		{
			name:  "Boolean literal true",
			query: "true",
			want:  "true",
		},
		{
			name:  "Boolean literal false",
			query: "false",
			want:  "false",
		},
		{
			name:  "Null literal",
			query: "NULL",
			want:  "null",
		},
		{
			name:  "Integer literal",
			query: "col = 42",
			want:  "paradedb.term('col','42')",
		},
		{
			name:  "Float literal",
			query: "col = 3.14",
			want:  "paradedb.term('col','3.14')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			fullSQL := "SELECT col FROM mytable WHERE " + tt.query
			stmt, err := sqlparser.Parse(fullSQL)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			var expr sqlparser.Expr
			switch s := stmt.(type) {
			case *sqlparser.Select:
				if s.Where != nil {
					expr = s.Where.Expr
				} else {
					expr = nil
				}
			default:
				t.Fatalf("Unexpected statement type: %T", stmt)
			}

			if expr == nil && !tt.wantErr {
				t.Fatalf("No sqlparser found for query: %s", tt.query)
			}

			got, err := c.ToParadeDBQuery(expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToParadeDBQuery() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Normalize spacing for comparison
			gotNorm := strings.ReplaceAll(got, " ", "")
			wantNorm := strings.ReplaceAll(tt.want, " ", "")

			if gotNorm != wantNorm {
				t.Errorf("ToParadeDBQuery() got = %q, want = %q", got, tt.want)
			}
		})
	}
}
