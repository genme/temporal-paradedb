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
	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"strings"
)

type (
	paradedbQueryConverter struct{}
)

// newParadeDBQueryConverter creates a new instance of query converter for ParadeDB.
// This converter translates Temporal visibility queries into ParadeDB-compatible queries
// that leverage BM25 full-text search capabilities.
//
// Parameters:
//   - namespaceName: Name of the Temporal namespace
//   - namespaceID: ID of the Temporal namespace
//   - saTypeMap: Map of search attribute names to their types
//   - saMapper: Search attribute mapper for alias resolution
//   - queryString: Original query string to be converted
//
// Returns:
//   - *QueryConverter configured for ParadeDB
func newParadeDBQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	return newQueryConverterInternal(
		&paradedbQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
	)
}

// convertKeywordListComparisonExpr converts comparison expressions for KeywordList type search attributes
// into ParadeDB-compatible queries. It handles equality, inequality, IN, and NOT IN operators.
//
// Parameters:
//   - expr: The comparison expression to convert
//
// Returns:
//   - sqlparser.Expr: Converted expression for ParadeDB
//   - error: If the operator is not supported or conversion fails
func (c *paradedbQueryConverter) convertKeywordListComparisonExpr(
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
		newExpr := c.newJsonFieldQuery(expr.Left, expr.Right)
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

// convertInExpr converts IN expressions into a ParadeDB boolean query with should clauses,
// effectively implementing OR logic between multiple possible values.
//
// Parameters:
//   - leftExpr: The field expression
//   - values: Tuple of values to match against
//
// Returns:
//   - sqlparser.Expr: ParadeDB boolean query expression
func (c *paradedbQueryConverter) convertInExpr(
	leftExpr sqlparser.Expr,
	values sqlparser.ValTuple,
) sqlparser.Expr {
	exprs := make([]sqlparser.Expr, len(values))
	for i, value := range values {
		exprs[i] = c.newJsonFieldQuery(leftExpr, value)
	}

	// Use ParadeDB boolean query with should clauses for OR operations
	shouldClauses := make([]string, len(exprs))
	for i, expr := range exprs {
		shouldClauses[i] = sqlparser.String(expr)
	}

	query := fmt.Sprintf(
		"id @@@ paradedb.boolean(should => ARRAY[%s])",
		strings.Join(shouldClauses, ", "),
	)

	return sqlparser.NewStrVal([]byte(query))
}

// convertTextComparisonExpr converts text comparison expressions into ParadeDB phrase search queries.
// This is used for text search attributes and supports equality and inequality operators.
//
// Parameters:
//   - expr: The comparison expression to convert
//
// Returns:
//   - sqlparser.Expr: ParadeDB phrase search expression
//   - error: If the operator is not supported or conversion fails
func (c *paradedbQueryConverter) convertTextComparisonExpr(
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

	// For text fields, use ParadeDB's phrase search
	query := fmt.Sprintf(
		"(namespace_id, run_id) @@@ paradedb.phrase('%s', ARRAY[%s])",
		getFieldName(expr.Left),
		sqlparser.String(expr.Right),
	)

	if expr.Operator == sqlparser.NotEqualStr {
		query = fmt.Sprintf("NOT (%s)", query)
	}

	return sqlparser.NewStrVal([]byte(query)), nil
}

// buildSelectStmt builds a SELECT statement for querying workflow executions with pagination and sorting.
//
// Parameters:
//   - namespaceID: ID of the namespace to filter by
//   - queryString: Additional query conditions
//   - pageSize: Maximum number of results to return
//   - token: Pagination token for continuing from a previous query
//
// Returns:
//   - string: The complete SQL query
//   - []any: Query parameters to be used with the statement
func (c *paradedbQueryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageToken,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	// Add namespace filter using ParadeDB term query
	namespaceFilter := fmt.Sprintf(
		"search_attributes @@@ paradedb.term('namespace_id', %s)",
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
	)
	whereClauses = append(whereClauses, namespaceFilter)
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		whereClauses = append(whereClauses, queryString)
	}

	if token != nil {
		tokenFilter := fmt.Sprintf(
			"search_attributes @@@ %s",
			c.buildPaginationQuery(token),
		)
		whereClauses = append(whereClauses, tokenFilter)
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

	// Include score for relevance-based sorting
	scoreExpr := "paradedb.score((namespace_id, run_id)) as query_score"
	selectFields := append(sqlplugin.DbFields, scoreExpr)

	queryArgs = append(queryArgs, pageSize)

	// For consistent pagination, we need a deterministic sort order
	// We use fast fields for efficient sorting
	return fmt.Sprintf(
		`SELECT %s 
		FROM pdb_executions_visibility 
		WHERE %s 
		ORDER BY 
			COALESCE(close_time, '9999-12-31 23:59:59.999999') DESC,  -- Use max datetime for NULL close_time
			start_time DESC,
			run_id DESC,
			query_score DESC
		LIMIT ?`,
		strings.Join(selectFields, ", "),
		strings.Join(whereClauses, " AND "),
	), queryArgs
}

// buildCountStmt builds a SELECT COUNT statement for counting workflow executions,
// optionally with grouping.
//
// Parameters:
//   - namespaceID: ID of the namespace to filter by
//   - queryString: Additional query conditions
//   - groupBy: Optional list of fields to group by
//
// Returns:
//   - string: The complete SQL query
//   - []any: Query parameters to be used with the statement
func (c *paradedbQueryConverter) buildCountStmt(
	namespaceID namespace.ID,
	queryString string,
	groupBy []string,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	// Add namespace filter using ParadeDB term query
	namespaceFilter := fmt.Sprintf(
		"search_attributes @@@ paradedb.term('namespace_id', %s)",
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
	)
	whereClauses = append(whereClauses, namespaceFilter)
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		whereClauses = append(whereClauses, queryString)
	}

	groupByClause := ""
	if len(groupBy) > 0 {
		groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(groupBy, ", "))
	}

	return fmt.Sprintf(
		"SELECT %s FROM pdb_executions_visibility WHERE %s %s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	), queryArgs
}

// buildPaginationQuery constructs a ParadeDB query for pagination that maintains correct ordering
// across multiple pages of results.
//
// Parameters:
//   - token: Pagination token containing the last seen values
//
// Returns:
//   - string: ParadeDB boolean query for pagination
func (c *paradedbQueryConverter) buildPaginationQuery(token *pageToken) string {
	// 1. (close_time = token.CloseTime AND start_time = token.StartTime AND run_id > token.RunID) OR
	// 2. (close_time = token.CloseTime AND start_time < token.StartTime) OR
	// 3. close_time < token.CloseTime
	return fmt.Sprintf(`paradedb.boolean(
        should => ARRAY[
            paradedb.boolean(
                must => ARRAY[
                    paradedb.term('close_time', %s),
                    paradedb.term('start_time', %s),
                    paradedb.range('run_id', '(%s, null)')
                ]
            ),
            paradedb.boolean(
                must => ARRAY[
                    paradedb.term('close_time', %s),
                    paradedb.range('start_time', '[null, %s)')
                ]
            ),
            paradedb.range('close_time', '[null, %s)')
        ]
    )`,
		token.CloseTime,
		token.StartTime,
		token.RunID,
		token.CloseTime,
		token.StartTime,
		token.CloseTime)
}

// getDatetimeFormat returns the datetime format string used for timestamp conversions.
//
// Returns:
//   - string: Datetime format string
func (c *paradedbQueryConverter) getDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

// getCoalesceCloseTimeExpr returns an expression that coalesces NULL close_time values
// with a maximum datetime value.
//
// Returns:
//   - sqlparser.Expr: Coalesce expression
func (c *paradedbQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		"COALESCE",
		closeTimeSaColName,
		newUnsafeSQLString(maxDatetimeValue.Format(c.getDatetimeFormat())),
	)
}

// newJsonFieldQuery creates a ParadeDB term query for JSON field search.
//
// Parameters:
//   - jsonExpr: Expression representing the JSON field to search
//   - valueExpr: Expression representing the value to search for
//
// Returns:
//   - sqlparser.Expr: ParadeDB term query expression
func (c *paradedbQueryConverter) newJsonFieldQuery(
	jsonExpr sqlparser.Expr,
	valueExpr sqlparser.Expr,
) sqlparser.Expr {
	// Use search_attributes directly since it's a JSONB column in the schema
	query := fmt.Sprintf(
		"search_attributes @@@ paradedb.term('$.%s', %s)",
		getFieldName(jsonExpr),
		sqlparser.String(valueExpr),
	)
	return sqlparser.NewStrVal([]byte(query))
}
