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

package visibility

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	queryConverter struct{}
)

const (
	bm25Operator = "@@@"
)

func newParadeDBQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	return newQueryConverterInternal(
		&queryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
	)
}

func (c *queryConverter) convertKeywordListComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		newExpr := c.newJsonContainsExpr(expr.Left, expr.Right)
		if expr.Operator == sqlparser.NotEqualStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil

	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := expr.Right.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(expr.Right),
			)
		}
		var newExpr sqlparser.Expr = &sqlparser.ParenExpr{
			Expr: c.convertInExpr(expr.Left, valTupleExpr),
		}
		if expr.Operator == sqlparser.NotInStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil

	default:
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute",
			query.InvalidExpressionErrMessage,
			expr.Operator,
		)
	}
}

func (c *queryConverter) convertInExpr(
	leftExpr sqlparser.Expr,
	values sqlparser.ValTuple,
) sqlparser.Expr {
	exprs := make([]sqlparser.Expr, len(values))
	for i, value := range values {
		exprs[i] = c.newJsonContainsExpr(leftExpr, value)
	}
	for len(exprs) > 1 {
		k := 0
		for i := 0; i < len(exprs); i += 2 {
			if i+1 < len(exprs) {
				exprs[k] = &sqlparser.OrExpr{
					Left:  exprs[i],
					Right: exprs[i+1],
				}
			} else {
				exprs[k] = exprs[i]
			}
			k++
		}
		exprs = exprs[:k]
	}
	return exprs[0]
}

func (c *queryConverter) convertTextComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedTextOperator(expr.Operator) {
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for Text type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	query := fmt.Sprintf(
		"id %s paradedb.phrase_match('search_attributes', '$.%s', %s)",
		bm25Operator,
		getFieldName(expr.Left),
		sqlparser.String(expr.Right),
	)

	if expr.Operator == sqlparser.NotEqualStr {
		query = fmt.Sprintf("NOT (%s)", query)
	}

	return sqlparser.NewStrVal([]byte(query)), nil
}

func (c *queryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageToken,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.NamespaceID)),
	)
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		whereClauses = append(whereClauses, queryString)
	}

	if token != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"((%s = ? AND %s = ? AND %s > ?) OR (%s = ? AND %s < ?) OR %s < ?)",
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				searchattribute.GetSqlDbColName(searchattribute.RunID),
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
			),
		)
		queryArgs = append(
			queryArgs,
			token.CloseTime,
			token.StartTime,
			token.RunID,
			token.CloseTime,
			token.StartTime,
			token.CloseTime,
		)
	}

	queryArgs = append(queryArgs, pageSize)

	return fmt.Sprintf(
		`SELECT %s
		FROM executions_visibility
		WHERE %s
		ORDER BY %s DESC, %s DESC, %s
		LIMIT ?`,
		strings.Join(sqlplugin.DbFields, ", "),
		strings.Join(whereClauses, " AND "),
		sqlparser.String(c.getCoalesceCloseTimeExpr()),
		searchattribute.GetSqlDbColName(searchattribute.StartTime),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
	), queryArgs
}

func (c *queryConverter) buildCountStmt(
	namespaceID namespace.ID,
	queryString string,
	groupBy []string,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("(%s = ?)", searchattribute.GetSqlDbColName(searchattribute.NamespaceID)),
	)
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		whereClauses = append(whereClauses, queryString)
	}

	groupByClause := ""
	if len(groupBy) > 0 {
		groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(groupBy, ", "))
	}

	return fmt.Sprintf(
		"SELECT %s FROM executions_visibility WHERE %s %s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	), queryArgs
}

func (c *queryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *queryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		"COALESCE",
		closeTimeSaColName,
		newUnsafeSQLString(maxDatetimeValue.Format(c.getDatetimeFormat())),
	)
}

func (c *queryConverter) newJsonContainsExpr(
	jsonExpr sqlparser.Expr,
	valueExpr sqlparser.Expr,
) sqlparser.Expr {
	return sqlparser.NewStrVal([]byte(fmt.Sprintf(
		"id %s paradedb.json_term('search_attributes', '$.%s', %s)",
		bm25Operator,
		getFieldName(jsonExpr),
		sqlparser.String(valueExpr),
	)))
}

func getFieldName(expr sqlparser.Expr) string {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String()
	case *saColName:
		return e.fieldName
	case *colName:
		return e.Name
	default:
		return ""
	}
}
