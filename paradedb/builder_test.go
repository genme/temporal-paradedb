package paradedb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
)

func TestBaseBuilders(t *testing.T) {
	t.Run("BuildTermExpr", func(t *testing.T) {
		gotExpr := BuildTermExpr("title", "shoes")
		got := sqlparser.String(gotExpr)
		want := "paradedb.term('title', 'shoes')"
		require.Equal(t, want, got, "BuildTermExpr() mismatch")
	})

	t.Run("BuildRangeExpr", func(t *testing.T) {
		tests := []struct {
			field    string
			val      string
			operator string
			want     string
		}{
			{
				field:    "price",
				val:      "100",
				operator: ">",
				want:     "paradedb.range('price', '(\"100\", null)')",
			},
			{
				field:    "price",
				val:      "50",
				operator: ">=",
				want:     "paradedb.range('price', '[\"50\", null)')",
			},
			{
				field:    "price",
				val:      "200",
				operator: "<",
				want:     "paradedb.range('price', '(null, \"200\")')",
			},
			{
				field:    "price",
				val:      "75",
				operator: "<=",
				want:     "paradedb.range('price', '(null, \"75\"]')",
			},
		}

		for _, tt := range tests {
			gotExpr := BuildRangeExpr(tt.field, tt.val, tt.operator)
			got := sqlparser.String(gotExpr)
			require.Equalf(t, tt.want, got, "BuildRangeExpr(%q, %q, %q) mismatch", tt.field, tt.val, tt.operator)
		}
	})
}

func TestSugarFunctions(t *testing.T) {
	t.Run("BuildAll", func(t *testing.T) {
		gotExpr := BuildAll()
		got := sqlparser.String(gotExpr)
		want := "paradedb.all()"
		require.Equal(t, want, got, "BuildAll() mismatch")
	})

	t.Run("BuildEmpty", func(t *testing.T) {
		gotExpr := BuildEmpty()
		got := sqlparser.String(gotExpr)
		want := "paradedb.empty()"
		require.Equal(t, want, got, "BuildEmpty() mismatch")
	})
}

func TestNewSingleClauseBooleanFunctions(t *testing.T) {
	t.Run("BuildMust", func(t *testing.T) {
		q1 := BuildTermExpr("product_name", "shoes")
		q2 := BuildTermExpr("category", "electronics")
		gotExpr := BuildMust(q1, q2)
		got := sqlparser.String(gotExpr)
		want := "paradedb.boolean(must => ARRAY[paradedb.term('product_name', 'shoes'), paradedb.term('category', 'electronics')])"
		require.Equal(t, want, got, "BuildMust() mismatch")
	})

	t.Run("BuildShould", func(t *testing.T) {
		q1 := BuildTermExpr("product_name", "shoes")
		q2 := BuildTermExpr("product_name", "sandals")
		gotExpr := BuildShould(q1, q2)
		got := sqlparser.String(gotExpr)
		want := "paradedb.boolean(should => ARRAY[paradedb.term('product_name', 'shoes'), paradedb.term('product_name', 'sandals')])"
		require.Equal(t, want, got, "BuildShould() mismatch")
	})
}

func TestExistsRegex(t *testing.T) {
	t.Run("BuildExists", func(t *testing.T) {
		gotExpr := BuildExists("description")
		got := sqlparser.String(gotExpr)
		want := "paradedb.exists('description')"
		require.Equal(t, want, got, "BuildExists() mismatch")
	})

	t.Run("BuildRegex", func(t *testing.T) {
		gotExpr := BuildRegex("product_name", "sh.*")
		got := sqlparser.String(gotExpr)
		want := "paradedb.regex('product_name', 'sh.*')"
		require.Equal(t, want, got, "BuildRegex() mismatch")
	})
}

func TestNegationFunctions(t *testing.T) {
	t.Run("BuildNot", func(t *testing.T) {
		term := BuildTermExpr("product_name", "shoes")
		gotExpr := BuildNot(term)
		got := sqlparser.String(gotExpr)
		want := "paradedb.not(paradedb.term('product_name', 'shoes'))"
		require.Equal(t, want, got, "BuildNot() mismatch")
	})

	t.Run("BuildMustNot", func(t *testing.T) {
		q1 := BuildTermExpr("product_name", "shoes")
		q2 := BuildTermExpr("category", "electronics")
		gotExpr := BuildMustNot(q1, q2)
		got := sqlparser.String(gotExpr)
		want := "paradedb.boolean(must_not => ARRAY[paradedb.term('product_name', 'shoes'), paradedb.term('category', 'electronics')])"
		require.Equal(t, want, got, "BuildMustNot() mismatch")
	})
}

func TestRangeExpressions(t *testing.T) {
	t.Run("BuildRangeGreaterThan", func(t *testing.T) {
		gotExpr := BuildRangeGreaterThan("price", "100")
		got := sqlparser.String(gotExpr)
		want := "paradedb.range('price', '(\"100\", null)')"
		require.Equal(t, want, got, "BuildRangeGreaterThan() mismatch")
	})

	t.Run("BuildRangeGreaterThanOrEqual", func(t *testing.T) {
		gotExpr := BuildRangeGreaterThanOrEqual("price", "50")
		got := sqlparser.String(gotExpr)
		want := "paradedb.range('price', '[\"50\", null)')"
		require.Equal(t, want, got, "BuildRangeGreaterThanOrEqual() mismatch")
	})

	t.Run("BuildRangeLessThan", func(t *testing.T) {
		gotExpr := BuildRangeLessThan("price", "200")
		got := sqlparser.String(gotExpr)
		want := "paradedb.range('price', '(null, \"200\")')"
		require.Equal(t, want, got, "BuildRangeLessThan() mismatch")
	})

	t.Run("BuildRangeLessThanOrEqual", func(t *testing.T) {
		gotExpr := BuildRangeLessThanOrEqual("price", "75")
		got := sqlparser.String(gotExpr)
		want := "paradedb.range('price', '(null, \"75\"]')"
		require.Equal(t, want, got, "BuildRangeLessThanOrEqual() mismatch")
	})

	t.Run("BuildRangeExpr", func(t *testing.T) {
			tests := []struct {
				field    string
				val      string
				operator string
				want     string
			}{
				{
					field:    "price",
					val:      "100",
					operator: ">",
					want:     "paradedb.range('price', '(\"100\", null)')",
				},
				{
					field:    "price",
					val:      "50",
					operator: ">=",
					want:     "paradedb.range('price', '[\"50\", null)')",
				},
				{
					field:    "price",
					val:      "200",
					operator: "<",
					want:     "paradedb.range('price', '(null, \"200\")')",
				},
				{
					field:    "price",
					val:      "75",
					operator: "<=",
					want:     "paradedb.range('price', '(null, \"75\"]')",
				},
			}

			for _, tt := range tests {
				gotExpr := BuildRangeExpr(tt.field, tt.val, tt.operator)
				got := sqlparser.String(gotExpr)
				require.Equalf(t, tt.want, got, "BuildRangeExpr(%q, %q, %q) mismatch", tt.field, tt.val, tt.operator)
			}
		})
	}
