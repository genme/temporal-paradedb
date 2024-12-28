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
	"github.com/temporalio/sqlparser"
)

type ArrayExpr struct {
	sqlparser.Expr
	Exprs sqlparser.Exprs
}

// Format implements the SQLNode interface.
func (node *ArrayExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("ARRAY[%v]", node.Exprs)
}

// walkSubtree implements the SQLNode interface.
func (node *ArrayExpr) walkSubtree(visit sqlparser.Visit) error {
	if node == nil {
		return nil
	}
	return sqlparser.Walk(visit, node.Exprs)
}

// replace implements the replace logic for expressions.
func (node *ArrayExpr) replace(from, to sqlparser.Expr) bool {
	for i := range node.Exprs {
		if sqlparser.ReplaceExpr(node.Exprs[i], from, to) != node.Exprs[i] {
			node.Exprs[i] = to
			return true
		}
	}
	return false
}

// newUnsafeSQLString creates a SQLVal node of type StrVal from a string.
// This should be used for values known to be safe, as it doesn't perform
// additional escaping beyond simple quoting.
func newUnsafeSQLString(s string) *sqlparser.SQLVal {
	return &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(s)}
}

// newFuncExpr creates a FuncExpr with the given name and a variadic list of Expr arguments.
func newFuncExpr(name string, exprs ...sqlparser.Expr) *sqlparser.FuncExpr {
	args := make(sqlparser.SelectExprs, len(exprs))
	for i, e := range exprs {
		args[i] = &sqlparser.AliasedExpr{Expr: e}
	}
	return &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent(name),
		Exprs: args,
	}
}

// newFuncExprWithNamedParams creates a FuncExpr from a map of named parameters.
// All params must be sqlparser.Expr.
func newFuncExprWithNamedParams(name string, params map[string]sqlparser.Expr) *sqlparser.FuncExpr {
	exprs := sqlparser.SelectExprs{}
	for k, v := range params {
		exprs = append(exprs, &sqlparser.AliasedExpr{
			Expr: &sqlparser.ComparisonExpr{
				Operator: "=>",
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(k)},
				Right:    v,
			},
		})
	}
	return &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent(name),
		Exprs: exprs,
	}
}

// namedArrayParam constructs a "name => ARRAY[...]" parameter expression.
func namedArrayParam(name string, exprs sqlparser.Exprs) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: "=>",
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(name)},
		Right:    &ArrayExpr{Exprs: exprs},
	}
}

// BuildExists returns a paradedb.exists(...) function call for checking a field's existence.
func BuildExists(field string) *sqlparser.FuncExpr {
	return newFuncExprWithNamedParams("paradedb.exists", map[string]sqlparser.Expr{
		"field": newUnsafeSQLString(field),
	})
}

// BuildRegex returns a paradedb.regex(...) function call to match a field against a pattern.
func BuildRegex(field string, pattern string) *sqlparser.FuncExpr {
	return newFuncExprWithNamedParams("paradedb.regex", map[string]sqlparser.Expr{
		"field":   newUnsafeSQLString(field),
		"pattern": newUnsafeSQLString(field),
	})
}

// BuildTermSet returns a paradedb.term_set(...) function call with a set of terms.
// Caller provides terms as sqlparser.Expr.
func BuildTermSet(terms ...sqlparser.Expr) *sqlparser.FuncExpr {
	return newFuncExprWithNamedParams("paradedb.term_set", map[string]sqlparser.Expr{
		"terms": &ArrayExpr{Exprs: terms},
	})
}

// BuildTermExpr returns a paradedb.term(...) function call.
func BuildTermExpr(field string, value string) *sqlparser.FuncExpr {
	return newFuncExprWithNamedParams("paradedb.term", map[string]sqlparser.Expr{
		"field": newUnsafeSQLString(field),
		"value": newUnsafeSQLString(value),
	})
}

// buildBooleanArrayParam constructs a paradedb.boolean(...) with a paramName => ARRAY[...].
func buildBooleanArrayParam(paramName string, exprs ...*sqlparser.FuncExpr) *sqlparser.FuncExpr {
	arrExprs := make(sqlparser.Exprs, len(exprs))
	for i, e := range exprs {
		arrExprs[i] = e
	}
	return newFuncExpr("paradedb.boolean", namedArrayParam(paramName, arrExprs))
}

// BuildMust returns a paradedb.boolean(...) function call with must => ARRAY[...].
func BuildMust(exprs ...*sqlparser.FuncExpr) *sqlparser.FuncExpr {
	return buildBooleanArrayParam("must", exprs...)
}

// BuildShould returns a paradedb.boolean(...) function call with should => ARRAY[...].
func BuildShould(exprs ...*sqlparser.FuncExpr) *sqlparser.FuncExpr {
	return buildBooleanArrayParam("should", exprs...)
}

// BuildMustNot returns a paradedb.boolean(...) function call with must_not => ARRAY[...].
func BuildMustNot(exprs ...*sqlparser.FuncExpr) *sqlparser.FuncExpr {
	return buildBooleanArrayParam("must_not", exprs...)
}

// BuildMatchExpr returns a comparison expression of the form:
// field IN (valuesExpr).
func BuildMatchExpr(field, valuesExpr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Left:     field,
		Operator: "@@@",
		Right:    valuesExpr,
	}
}

// BuildEmpty returns a paradedb.empty() function call.
func BuildEmpty() *sqlparser.FuncExpr {
	return newFuncExpr("paradedb.empty")
}

// BuildAll returns a paradedb.all(...) function call with multiple sub-expressions.
func BuildAll(exprs ...*sqlparser.FuncExpr) *sqlparser.FuncExpr {
	e := make([]sqlparser.Expr, len(exprs))
	for i, ex := range exprs {
		e[i] = ex
	}
	return newFuncExpr("paradedb.all", e...)
}

// BuildNot returns a paradedb.not(...) function call around a single expression.
func BuildNot(expr *sqlparser.FuncExpr) *sqlparser.FuncExpr {
	return newFuncExpr("paradedb.not", expr)
}

// BuildBetween creates a range filter with inclusive bounds by default,
// mimicking standard SQL BETWEEN. For example:
// field @@@ '[lower TO upper]'
func BuildBetween(field, lower, upper sqlparser.Expr) sqlparser.Expr {
	return BuildBetweenBounds(field, lower, upper, "[]")
}

// BuildBetweenBounds creates a range filter with custom bounds.
// bounds should be 2 characters:
// '(' or '[' for the lower bound
// ')' or ']' for the upper bound
// '(' or ')' indicates exclusive -> '{' or '}'
// '[' or ']' indicates inclusive -> '[' or ']'
//
// Examples:
// BuildBetweenBounds(field, lower, upper, "[]") -> field @@@ '[lower TO upper]'
// BuildBetweenBounds(field, lower, upper, "()") -> field @@@ '{lower TO upper}'
func BuildBetweenBounds(field, lower, upper sqlparser.Expr, bounds string) sqlparser.Expr {
	if len(bounds) != 2 {
		panic("bounds must be exactly 2 characters like '[]' or '()'")
	}

	leftChar := string(bounds[0])
	rightChar := string(bounds[1])

	leftMap := map[string]string{"(": "{", "[": "["}
	rightMap := map[string]string{")": "}", "]": "]"}

	leftBound, okL := leftMap[leftChar]
	rightBound, okR := rightMap[rightChar]

	if !okL || !okR {
		panic("invalid bounds characters. Use '(' or '[' for left; ')' or ']' for right.")
	}

	lowerStr := sqlparser.String(lower)
	upperStr := sqlparser.String(upper)
	rangeStr := fmt.Sprintf("%s%s TO %s%s", leftBound, lowerStr, upperStr, rightBound)

	return &sqlparser.ComparisonExpr{
		Left:     field,
		Operator: "@@@",
		Right: &sqlparser.SQLVal{
			Type: sqlparser.StrVal,
			Val:  []byte(rangeStr),
		},
	}
}
