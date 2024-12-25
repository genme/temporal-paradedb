// File: converter.go
//
// The ExpressionConverter converts parsed SQL expressions (using the "sqlparser" library)
// into ParadeDB-compatible query strings. It interprets basic SQL operations (AND, OR, NOT,
// IS NULL, comparison operators, etc.) and uses builder functions (from builder.go and related files)
// to produce ParadeDB queries.
//
// By utilizing the sugar functions (BuildMust, BuildShould, BuildMustNot, etc.), we simplify
// the construction of boolean queries while preserving functionality and readability.

package paradedb

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
)

// ExpressionConverter converts a parsed sqlparser.Expr into a ParadeDB query string.
type ExpressionConverter struct{}

// NewExpressionConverter returns a new ExpressionConverter instance.
func NewExpressionConverter() *ExpressionConverter {
	return &ExpressionConverter{}
}

// funcExprToString safely converts a *sqlparser.FuncExpr to a ParadeDB query string using sqlparser.String.
// Centralizing this call avoids repetition and improves maintainability.
func (c *ExpressionConverter) funcExprToString(expr *sqlparser.FuncExpr) (string, error) {
	if expr == nil {
		return "", fmt.Errorf("failed to convert sqlparser: sqlparser is nil")
	}
	return sqlparser.String(expr), nil
}

// ToParadeDBQuery converts a sqlparser.Expr into a ParadeDB-compatible query string.
// Supported constructs include: AND, OR, NOT, comparison operators (=, !=, >, <, etc.),
// IS NULL, and IS NOT NULL. For unsupported constructs, an error is returned.
func (c *ExpressionConverter) ToParadeDBQuery(expr sqlparser.Expr) (string, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		return c.convertAndExpr(e)
	case *sqlparser.OrExpr:
		return c.convertOrExpr(e)
	case *sqlparser.NotExpr:
		return c.convertNotExpr(e)
	case *sqlparser.ComparisonExpr:
		return c.convertComparison(e)
	case *sqlparser.IsExpr:
		return c.convertIsExpr(e)
	case *sqlparser.ParenExpr:
		// Strip parentheses and reprocess
		return c.ToParadeDBQuery(e.Expr)
	case *sqlparser.SQLVal:
		return c.convertSQLVal(e), nil
	case *sqlparser.ColName:
		// Column names are returned as-is
		return e.Name.String(), nil
	case sqlparser.BoolVal:
		return c.convertBoolVal(e), nil
	case *sqlparser.NullVal:
		return "null", nil
	default:
		return "", fmt.Errorf("unsupported sqlparser type: %T", expr)
	}
}

// convertAndExpr handles logical AND. It constructs a ParadeDB boolean query
// that must match both sides.
func (c *ExpressionConverter) convertAndExpr(e *sqlparser.AndExpr) (string, error) {
	left, err := c.ToParadeDBQuery(e.Left)
	if err != nil {
		return "", err
	}
	right, err := c.ToParadeDBQuery(e.Right)
	if err != nil {
		return "", err
	}
	leftExpr := BuildTermExpr("expr", left)
	rightExpr := BuildTermExpr("expr", right)
	return c.funcExprToString(BuildMust(leftExpr, rightExpr))
}

// convertOrExpr handles logical OR. It constructs a ParadeDB boolean query
// that should match either side.
func (c *ExpressionConverter) convertOrExpr(e *sqlparser.OrExpr) (string, error) {
	left, err := c.ToParadeDBQuery(e.Left)
	if err != nil {
		return "", err
	}
	right, err := c.ToParadeDBQuery(e.Right)
	if err != nil {
		return "", err
	}
	leftExpr := BuildTermExpr("expr", left)
	rightExpr := BuildTermExpr("expr", right)
	return c.funcExprToString(BuildShould(leftExpr, rightExpr))
}

// convertNotExpr handles logical NOT. It negates the inner sqlparser
// using a must_not clause.
func (c *ExpressionConverter) convertNotExpr(e *sqlparser.NotExpr) (string, error) {
	inner, err := c.ToParadeDBQuery(e.Expr)
	if err != nil {
		return "", err
	}
	innerExpr := BuildTermExpr("expr", inner)
	return c.funcExprToString(BuildMustNot(innerExpr))
}

// convertComparison handles comparison operators (=, !=, >, <, IN, NOT IN, LIKE, NOT LIKE, etc.)
// and constructs the corresponding ParadeDB queries.
func (c *ExpressionConverter) convertComparison(e *sqlparser.ComparisonExpr) (string, error) {
	left, err := c.ToParadeDBQuery(e.Left)
	if err != nil {
		return "", err
	}
	right, err := c.ToParadeDBQuery(e.Right)
	if err != nil {
		return "", err
	}
	operator := e.Operator

	switch operator {
	case sqlparser.EqualStr:
		return c.funcExprToString(BuildTermExpr(left, right))
	case sqlparser.NotEqualStr:
		return c.funcExprToString(BuildMustNot(BuildTermExpr(left, right)))
	case sqlparser.InStr, sqlparser.NotInStr:
		vals := c.splitValTuple(e.Right)
		termExprs := make([]*sqlparser.FuncExpr, 0, len(vals))
		for _, v := range vals {
			termExprs = append(termExprs, BuildTermExpr(left, v))
		}
		inQuery := BuildShould(termExprs...)
		if operator == sqlparser.NotInStr {
			return c.funcExprToString(BuildMustNot(inQuery))
		}
		return c.funcExprToString(inQuery)
	case sqlparser.LikeStr, sqlparser.NotLikeStr:
		likeExpr := BuildTermExpr(left, right)
		if operator == sqlparser.NotLikeStr {
			return c.funcExprToString(BuildMustNot(likeExpr))
		}
		return c.funcExprToString(likeExpr)
	case sqlparser.GreaterThanStr, sqlparser.GreaterEqualStr,
		sqlparser.LessThanStr, sqlparser.LessEqualStr:
		return c.funcExprToString(BuildRangeExpr(left, right, operator))
	default:
		return "", fmt.Errorf("operator '%s' not supported", operator)
	}
}

// convertIsExpr handles IS NULL and IS NOT NULL.
func (c *ExpressionConverter) convertIsExpr(e *sqlparser.IsExpr) (string, error) {
	left, err := c.ToParadeDBQuery(e.Expr)
	if err != nil {
		return "", err
	}

	switch e.Operator {
	case sqlparser.IsNullStr:
		// IS NULL can be represented as a scenario where the field must NOT exist.
		// BuildExists(left) creates paradedb.exists(...)
		// BuildMustNot(...) creates a must_not boolean sqlparser
		// BuildAll(...) wraps them, ensuring the condition is treated as a single combined sqlparser.
		return c.funcExprToString(BuildAll(BuildMustNot(BuildExists(left))))

	case sqlparser.IsNotNullStr:
		// IS NOT NULL is a scenario where the field must exist.
		// Again, use BuildAll(...) for consistency and flexibility.
		return c.funcExprToString(BuildAll(BuildExists(left)))

	default:
		return "", fmt.Errorf("IS operator '%s' not supported", e.Operator)
	}
}

// convertSQLVal converts a SQLVal into a ParadeDB-compatible string.
// It handles string, int, and float values.
func (c *ExpressionConverter) convertSQLVal(val *sqlparser.SQLVal) string {
	switch val.Type {
	case sqlparser.StrVal:
		s := string(val.Val)
		return strings.Trim(s, "'")
	case sqlparser.IntVal, sqlparser.FloatVal:
		return string(val.Val)
	default:
		return string(val.Val)
	}
}

// convertBoolVal converts a boolean SQL value into the string "true" or "false".
func (c *ExpressionConverter) convertBoolVal(b sqlparser.BoolVal) string {
	if b {
		return "true"
	}
	return "false"
}

// splitValTuple extracts multiple values from an IN(...) or NOT IN(...) tuple.
func (c *ExpressionConverter) splitValTuple(expr sqlparser.Expr) []string {
	vTuple, ok := expr.(sqlparser.ValTuple)
	if !ok {
		str, _ := c.ToParadeDBQuery(expr)
		return []string{str}
	}
	var results []string
	for _, ex := range vTuple {
		s, _ := c.ToParadeDBQuery(ex)
		results = append(results, s)
	}
	return results
}
