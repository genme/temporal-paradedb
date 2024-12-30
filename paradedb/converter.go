// The MIT License
//
// Copyright (c) 2024 vivaneiona
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	case sqlparser.ValTuple:
		// Handle IN lists
		var results []string
		for _, ex := range e {
			s, err := c.ToParadeDBQuery(ex)
			if err != nil {
				return "", err
			}
			results = append(results, s)
		}
		return strings.Join(results, ","), nil
	default:
		return "", fmt.Errorf("unsupported sqlparser type: %T", expr)
	}
}

// convertAndExpr handles logical AND. It constructs a ParadeDB boolean query
// that must match both sides.
func (c *ExpressionConverter) convertAndExpr(e *sqlparser.AndExpr) (string, error) {
	leftExpr, err := c.convertComparisonToFuncExpr(e.Left)
	if err != nil {
		return "", err
	}
	rightExpr, err := c.convertComparisonToFuncExpr(e.Right)
	if err != nil {
		return "", err
	}
	return c.funcExprToString(BuildMust(leftExpr, rightExpr))
}

// convertOrExpr handles logical OR. It constructs a ParadeDB boolean query
// that should match either side.
func (c *ExpressionConverter) convertOrExpr(e *sqlparser.OrExpr) (string, error) {
	leftExpr, err := c.convertComparisonToFuncExpr(e.Left)
	if err != nil {
		return "", err
	}
	rightExpr, err := c.convertComparisonToFuncExpr(e.Right)
	if err != nil {
		return "", err
	}
	return c.funcExprToString(BuildShould(leftExpr, rightExpr))
}

// convertComparisonToFuncExpr converts a comparison expression to a ParadeDB function expression.
func (c *ExpressionConverter) convertComparisonToFuncExpr(expr sqlparser.Expr) (*sqlparser.FuncExpr, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		left, err := c.ToParadeDBQuery(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := c.ToParadeDBQuery(e.Right)
		if err != nil {
			return nil, err
		}
		return BuildTermExpr(left, right), nil
	default:
		str, err := c.ToParadeDBQuery(expr)
		if err != nil {
			return nil, err
		}
		return BuildTermExpr("col", str), nil
	}
}

// convertNotExpr handles logical NOT. It negates the inner sqlparser
// using a must_not clause.
func (c *ExpressionConverter) convertNotExpr(e *sqlparser.NotExpr) (string, error) {
	innerExpr, err := c.convertComparisonToFuncExpr(e.Expr)
	if err != nil {
		return "", err
	}
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
		rangeExpr := BuildRangeExpr(left, right, operator)
		if funcExpr, ok := rangeExpr.(*sqlparser.FuncExpr); ok {
			return c.funcExprToString(funcExpr)
		}
		return "", fmt.Errorf("failed to convert range expression to FuncExpr")
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
		return c.funcExprToString(BuildMustNot(BuildExists(left)))

	case sqlparser.IsNotNullStr:
		// IS NOT NULL is a scenario where the field must exist.
		return c.funcExprToString(BuildExists(left))

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
