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
	"github.com/temporalio/sqlparser"
)

// BuildBasicComparisonExpr creates a simple ComparisonExpr like `field = 'value'`.
func BuildBasicComparisonExpr(field string, operator string, right sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(field)},
		Operator: operator,
		Right:    right,
	}
}

// BuildInExpr creates `field IN ('val1','val2',...)`.
func BuildInExpr(field string, valuesExpr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(field)},
		Operator: sqlparser.InStr,
		Right:    valuesExpr,
	}
}

// BuildNotInExpr creates `field NOT IN ('val1','val2',...)`.
func BuildNotInExpr(field string, valuesExpr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(field)},
		Operator: sqlparser.NotInStr,
		Right:    valuesExpr,
	}
}
