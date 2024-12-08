// Package visibility provides functionality to handle and convert user queries into ParadeDB-compatible
// SQL statements that integrate with Temporal's visibility store.
//
// This converter leverages ParadeDB's query primitives (e.g., paradedb.term, paradedb.boolean, etc.)
// as documented in the ParadeDB advanced and full-text querying documentation.
//
// References:
// - Full-Text Querying: https://docs.example.com/documentation/full-text/overview
// - Advanced Queries (boolean, phrase, parse): See documentation sections on boolean, phrase, etc.
// - JSON and Range Queries: See documentation sections on arrays, range, fuzzy_phrase, etc.
//
// This code maps user-provided SQL-like queries (via sqlparser) into ParadeDB function calls such as:
// paradedb.term(), paradedb.boolean(), paradedb.phrase(), paradedb.range(), etc. combined with the '@@@' operator.
//
// The '@@@' operator is ParadeDB’s full-text search operator that integrates with the underlying BM25 index.
// Terms, boolean queries, and phrases are constructed following ParadeDB docs.
//
// For example, a condition `run_id @@@ paradedb.term('field', 'value')` searches documents where 'field' matches 'value'.
// For keyword lists or text comparisons, custom logic is implemented here to build the correct ParadeDB function calls.
package visibility

import (
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

// allowedComparisonOperators defines which comparison operators are accepted
// in the incoming SQL-like queries. Only these operators can be translated into ParadeDB constructs.
// Reference: Queries often contain operators =, !=, IN, NOT IN, <, <=, etc.
var allowedComparisonOperators = map[string]struct{}{
	sqlparser.EqualStr:        {},
	sqlparser.NotEqualStr:     {},
	sqlparser.GreaterThanStr:  {},
	sqlparser.GreaterEqualStr: {},
	sqlparser.LessThanStr:     {},
	sqlparser.LessEqualStr:    {},
	sqlparser.InStr:           {},
	sqlparser.NotInStr:        {},
}

// ParadeDB requires RFC3339 microsecond precision for datetime string formats.
const timeFormatForDatetime = "2006-01-02 15:04:05.999999"

// dbCoreFields are the core columns selected from the pdb_executions_visibility table.
// These map to fundamental workflow attributes stored in the ParadeDB index.
var dbCoreFields = []string{
	"namespace_id",
	"run_id",
	"start_time",
	"execution_time",
	"workflow_id",
	"workflow_type_name",
	"status",
	"close_time",
	"history_length",
	"history_size_bytes",
	"execution_duration",
	"state_transition_count",
	"memo",
	"encoding",
	"task_queue",
	"search_attributes",
	"parent_workflow_id",
	"parent_run_id",
	"root_workflow_id",
	"root_run_id",
}

// paradeDBQueryConverter implements the PluginQueryConverter interface, using a baseConverter
// and extending it for ParadeDB-specific query building. This includes constructing paradedb.*
// function calls for text, keyword lists, range queries, boolean queries, etc.
type paradeDBQueryConverter struct {
	baseConverter *query.Converter
}

// getCoalesceCloseTimeExpr is intended to provide a coalesce expression for close_time, and we do not need it with
// ParadeDB
func (c *paradeDBQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return nil
}

// newParadeDBQueryConverter creates a QueryConverter that uses ParadeDB logic. It sets up interceptors
// for field names and values, and configures where and comparison converters to handle search attributes
// and operators, ultimately producing queries that leverage paradedb.* functions and '@@@' operators.
//
// For example, if the user query references a TEXT field and operator `=`, it may become
// run_id @@@ paradedb.term('field', 'value') to match documents. If using a boolean query, it could produce
// run_id @@@ paradedb.boolean( ... ) constructs as per ParadeDB documentation.
func newParadeDBQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	fnInterceptor := &paradeDBFieldNameInterceptor{namespaceID: namespaceID, saMapper: saMapper}
	fvInterceptor := &paradeDBFieldValueInterceptor{saTypeMap: saTypeMap}

	// Converters for different expression types (range conditions, comparisons, IS conditions)
	rangeCond := query.NewRangeCondConverter(fnInterceptor, fvInterceptor, false)
	comparisonExpr := query.NewComparisonExprConverter(fnInterceptor, fvInterceptor, allowedComparisonOperators, saTypeMap)
	isConv := query.NewIsConverter(fnInterceptor)

	// Build a WhereConverter that uses these converters along with AND/OR logic
	whereConv := &query.WhereConverter{
		RangeCond:      rangeCond,
		ComparisonExpr: comparisonExpr,
		Is:             isConv,
	}
	whereConv.And = query.NewAndConverter(whereConv)
	whereConv.Or = query.NewOrConverter(whereConv)

	baseConv := query.NewConverter(fnInterceptor, whereConv)
	return &QueryConverter{
		PluginQueryConverter: &paradeDBQueryConverter{baseConverter: baseConv},
		namespaceName:        namespaceName,
		namespaceID:          namespaceID,
		saTypeMap:            saTypeMap,
		saMapper:             saMapper,
		queryString:          queryString,
	}
}

// convertKeywordListComparisonExpr handles conditions on keyword list fields (multi-value attributes).
// ParadeDB supports term-based queries on arrays of keywords. For equality or membership (IN), we translate
// to run_id @@@ paradedb.term(...) or paradedb.boolean(...) queries referencing `search_attributes`.
// This logic relates to ParadeDB doc sections on arrays, `term`, and boolean queries.
func (c *paradeDBQueryConverter) convertKeywordListComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, invalidOperatorErr(expr, "KeywordList")
	}

	field := strings.ToLower(getFieldName(expr.Left))
	buildTerm := func(field, val string) sqlparser.Expr {
		// Builds paradedb.term("$.field", val) calls
		return buildTermFunc("paradedb.term", "$."+field, val)
	}

	// Depending on =, !=, IN, NOT IN, build corresponding paradedb conditions.
	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		val := stripQuotes(sqlparser.String(expr.Right))
		// For equality: run_id @@@ paradedb.term("$.field", "val")
		// If not equal: NOT (run_id @@@ ...)
		binExpr := buildBinaryExpr("run_id", "@@@", buildTerm(field, val))
		if expr.Operator == sqlparser.NotEqualStr {
			return &sqlparser.NotExpr{Expr: binExpr}, nil
		}
		return binExpr, nil

	case sqlparser.InStr, sqlparser.NotInStr:
		// IN clauses become arrays of conditions combined with paradedb.boolean(... should => ARRAY[...]).
		valTupleExpr, ok := expr.Right.(sqlparser.ValTuple)
		if !ok {
			return nil, query.NewConverterError(
				"%s: expected tuple for IN operator in %s",
				query.InvalidExpressionErrMessage,
				formatComparisonExprStringForError(*expr),
			)
		}

		if len(valTupleExpr) == 0 {
			// Empty set IN means condition always false: 0=1
			return &sqlparser.BinaryExpr{
				Left:     &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("0")},
				Operator: "=",
				Right:    &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("1")},
			}, nil
		}

		// Build array of terms and wrap in paradedb.boolean(...).
		termExprs := make([]sqlparser.Expr, 0, len(valTupleExpr))
		for _, v := range valTupleExpr {
			val := stripQuotes(sqlparser.String(v))
			termExprs = append(termExprs, buildBinaryExpr("search_attributes", "@@@", buildTerm(field, val)))
		}

		// Create ARRAY[...] and paradedb.boolean(should => ARRAY[...]) to handle multiple terms.
		aliasedExprs := make(sqlparser.SelectExprs, 0, len(termExprs))
		for _, e := range termExprs {
			aliasedExprs = append(aliasedExprs, &sqlparser.AliasedExpr{Expr: e})
		}
		arrayExpr := &sqlparser.FuncExpr{
			Name:  sqlparser.NewColIdent("ARRAY"),
			Exprs: aliasedExprs,
		}

		shouldBinary := &sqlparser.BinaryExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("should")},
			Operator: "=>",
			Right:    arrayExpr,
		}

		booleanFunc := &sqlparser.FuncExpr{
			Name: sqlparser.NewColIdent("paradedb.boolean"),
			Exprs: sqlparser.SelectExprs{
				&sqlparser.AliasedExpr{Expr: shouldBinary},
			},
		}

		binExpr := &sqlparser.BinaryExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("run_id")},
			Operator: "@@@",
			Right:    booleanFunc,
		}

		if expr.Operator == sqlparser.NotInStr {
			return &sqlparser.NotExpr{Expr: binExpr}, nil
		}
		return binExpr, nil

	default:
		return nil, invalidOperatorErr(expr, "KeywordList")
	}
}

// convertTextComparisonExpr handles text fields. For TEXT fields, single terms become run_id @@@ paradedb.term(...).
// Multiple terms become run_id @@@ paradedb.phrase(...) for phrase searching.
// Not equal logic is handled by negating the expression. This maps to full-text doc sections on `term`, `phrase`, and `@@@`.
func (c *paradeDBQueryConverter) convertTextComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error) {
	if !isSupportedTextOperator(expr.Operator) {
		return nil, invalidOperatorErr(expr, "Text")
	}

	fieldName := strings.ToLower(getFieldName(expr.Left))
	values := extractStringTerms(expr.Right)
	if len(values) == 0 {
		values = []string{sqlparser.String(expr.Right)}
	}

	var finalExpr sqlparser.Expr
	switch {
	case len(values) == 1:
		// Single term: run_id @@@ paradedb.term(fieldName, val)
		val := values[0]
		finalExpr = buildBinaryExpr("run_id", "@@@", buildTermFunc("paradedb.term", fieldName, val))
		if expr.Operator == sqlparser.NotEqualStr {
			finalExpr = &sqlparser.NotExpr{Expr: finalExpr}
		}
		return finalExpr, nil

	default:
		// Multiple terms: run_id @@@ paradedb.phrase(fieldName, ARRAY[val1, val2, ...])
		// Matches documents containing these terms in sequence, referencing the phrase doc sections.
		phraseFunc := buildPhraseFunc(fieldName, values)
		finalExpr = buildBinaryExpr("run_id", "@@@", phraseFunc)
		if expr.Operator == sqlparser.NotEqualStr {
			finalExpr = &sqlparser.NotExpr{Expr: finalExpr}
		}
		return finalExpr, nil
	}
}

// buildSelectStmt constructs the final SELECT query for ParadeDB executions visibility.
// It uses the queryString and optional pagination token. The ordering by close_time, start_time, etc. is applied,
// and paradedb.score(...) is included to rank results by relevance (see docs on scoring).
func (c *paradeDBQueryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageToken,
) (string, []any) {
	baseWhere, baseArgs := buildBaseWhereClauses(namespaceID, queryString)
	whereClauses := []string{baseWhere}
	args := baseArgs

	if token != nil {
		// Pagination clause uses a complex paradedb.boolean(...) query as documented in ParadeDB advanced usage.
		tokenQuery, tokenArgs := c.buildPaginationClause(token)
		whereClauses = append(whereClauses, tokenQuery)
		args = append(args, tokenArgs...)
	}

	scoreExpr := "paradedb.score(run_id) as query_score"
	selectFields := append(dbCoreFields, scoreExpr)
	args = append(args, pageSize)

	// The query orders by close_time DESC, start_time DESC, run_id DESC, and query_score DESC for stable sorting.
	queryStr := fmt.Sprintf(
		`SELECT %s
		 FROM pdb_executions_visibility
		 WHERE %s
		 ORDER BY
		   COALESCE(close_time, '9999-12-31 23:59:59.999999') DESC,
		   start_time DESC,
		   run_id DESC,
		   query_score DESC
		 LIMIT ?`,
		strings.Join(selectFields, ", "),
		strings.Join(whereClauses, " AND "),
	)
	return queryStr, args
}

// buildCountStmt constructs a COUNT query, possibly grouped by certain fields.
// It also applies the base WHERE conditions. This is useful for counting results that match certain ParadeDB-based criteria.
func (c *paradeDBQueryConverter) buildCountStmt(
	namespaceID namespace.ID,
	queryString string,
	groupBy []string,
) (string, []any) {
	baseWhere, baseArgs := buildBaseWhereClauses(namespaceID, queryString)
	whereClauses := []string{baseWhere}

	groupByClause := ""
	if len(groupBy) > 0 {
		groupByClause = "GROUP BY " + strings.Join(groupBy, ", ")
	}

	selectFields := append(groupBy, "COUNT(*)")
	queryStr := fmt.Sprintf(
		"SELECT %s FROM pdb_executions_visibility WHERE %s %s",
		strings.Join(selectFields, ", "),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	)
	return queryStr, baseArgs
}

// getDatetimeFormat returns the datetime format used by ParadeDB queries.
func (c *paradeDBQueryConverter) getDatetimeFormat() string {
	return timeFormatForDatetime
}

// buildPaginationClause creates additional conditions to filter results beyond the given pageToken.
// It uses paradedb.boolean(...) with multiple must and range conditions to ensure correct paging.
func (c *paradeDBQueryConverter) buildPaginationClause(token *pageToken) (string, []any) {
	return c.buildPaginationQuery(token),
		[]any{
			token.CloseTime,
			token.StartTime,
			token.RunID,
			token.CloseTime,
			token.StartTime,
			token.CloseTime,
		}
}

// buildPaginationQuery returns a complex paradedb.boolean(...) expression ensuring that results are paged correctly.
// It combines multiple paradedb.term and paradedb.range calls over close_time, start_time, run_id fields,
// enforcing ordering as documented in advanced queries.
func (c *paradeDBQueryConverter) buildPaginationQuery(token *pageToken) string {
	return fmt.Sprintf(`run_id @@@ paradedb.boolean(
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

// paradeDBFieldNameInterceptor maps user-supplied field names to actual database fields or indexed search attributes.
// If a search attribute is known, it returns its DB alias as used by ParadeDB.
type paradeDBFieldNameInterceptor struct {
	namespaceID namespace.ID
	saMapper    searchattribute.Mapper
}

// Name attempts to resolve the field name using the SA mapper. If found, returns its DB alias, else returns original name.
func (p *paradeDBFieldNameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	if alias, err := p.saMapper.GetFieldName(name, p.namespaceID.String()); err == nil {
		return alias, nil
	}
	return name, nil
}

// paradeDBFieldValueInterceptor converts values to ParadeDB-compatible strings, quoting text,
// formatting datetimes, and leaving numeric and boolean values as is. This ensures values are
// properly escaped and formatted before building paradedb.* function calls.
type paradeDBFieldValueInterceptor struct {
	saTypeMap searchattribute.NameTypeMap
}

// Values delegates to ConvertFieldValues to handle any needed conversions based on SA type.
func (p *paradeDBFieldValueInterceptor) Values(name string, fieldName string, values ...interface{}) ([]interface{}, error) {
	return p.ConvertFieldValues(fieldName, values...)
}

// ConvertFieldValues modifies values according to their search attribute type.
// For TEXT/KEYWORD: quote and escape.
// For INT/DOUBLE/BOOL: no change.
// For DATETIME: format as quoted RFC3339 strings.
func (p *paradeDBFieldValueInterceptor) ConvertFieldValues(fieldName string, values ...interface{}) ([]interface{}, error) {
	fieldType, err := p.saTypeMap.GetType(fieldName)
	if err != nil {
		// Unknown field type, return as is
		return values, nil
	}

	converted := make([]interface{}, len(values))
	for i, v := range values {
		switch fieldType {
		case enumspb.INDEXED_VALUE_TYPE_TEXT, enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			converted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''"))
		case enumspb.INDEXED_VALUE_TYPE_INT, enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			enumspb.INDEXED_VALUE_TYPE_BOOL:
			converted[i] = v
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			converted[i] = fmt.Sprintf("'%v'", v)
		default:
			converted[i] = v
		}
	}
	return converted, nil
}

// buildBaseWhereClauses returns a base WHERE clause including namespace_id and optional query string conditions.
// This ensures that all queries are namespace-scoped, as required by Temporal and ParadeDB setup.
func buildBaseWhereClauses(namespaceID namespace.ID, queryString string) (string, []any) {
	whereClauses := []string{"namespace_id = ?"}
	args := []any{namespaceID.String()}
	if queryString != "" {
		whereClauses = append(whereClauses, queryString)
	}
	return strings.Join(whereClauses, " AND "), args
}

// buildTermFunc constructs a paradedb.term(...) function call.
// Refer to "term" queries in ParadeDB docs for how paradedb.term works for textual or keyword fields.
func buildTermFunc(funcName, field, val string) *sqlparser.FuncExpr {
	return &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent(funcName),
		Exprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{
				Expr: &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(field)},
			},
			&sqlparser.AliasedExpr{
				Expr: &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(val)},
			},
		},
	}
}

// buildPhraseFunc constructs a paradedb.phrase(...) function call to handle phrase matching.
// For multiple tokens, paradedb.phrase(field, ARRAY[...]) matches documents containing the given tokens in sequence.
// See docs on "phrase" queries.
func buildPhraseFunc(field string, vals []string) *sqlparser.FuncExpr {
	arrayArgs := make(sqlparser.SelectExprs, len(vals))
	for i, v := range vals {
		arrayArgs[i] = &sqlparser.AliasedExpr{Expr: &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(v)}}
	}
	arrayFunc := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("ARRAY"), Exprs: arrayArgs}
	return &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("paradedb.phrase"),
		Exprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{Expr: &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(field)}},
			&sqlparser.AliasedExpr{Expr: arrayFunc},
		},
	}
}

// buildBinaryExpr constructs a simple binary expression like "colName @@@ expression".
// For example, run_id @@@ paradedb.term(...) integrates with ParadeDB's '@@@' operator as per docs.
func buildBinaryExpr(left string, operator string, right sqlparser.Expr) *sqlparser.BinaryExpr {
	return &sqlparser.BinaryExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(left)},
		Operator: operator,
		Right:    right,
	}
}

// buildBooleanArrayExpr constructs a paradedb.boolean(...) call with a should array.
// This is used when multiple conditions need to be combined as per ParadeDB boolean query documentation.
func buildBooleanArrayExpr(colName string, exprs []sqlparser.Expr) sqlparser.Expr {
	arrayArgs := make(sqlparser.SelectExprs, len(exprs))
	for i, t := range exprs {
		arrayArgs[i] = &sqlparser.AliasedExpr{Expr: t}
	}
	arrayFunc := &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("ARRAY"), Exprs: arrayArgs}
	shouldBinary := &sqlparser.BinaryExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("should")},
		Operator: "=>",
		Right:    arrayFunc,
	}
	booleanFunc := &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("paradedb.boolean"),
		Exprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{Expr: shouldBinary},
		},
	}

	return &sqlparser.BinaryExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(colName)},
		Operator: "@@@",
		Right:    booleanFunc,
	}
}

// invalidOperatorErr returns a detailed error message when a user tries to use an unsupported operator for the field type.
// This ensures queries align with allowed operators and ParadeDB's capabilities.
func invalidOperatorErr(expr *sqlparser.ComparisonExpr, fieldType string) error {
	return query.NewConverterError(
		"%s: operator '%s' not supported for %s in %s",
		query.InvalidExpressionErrMessage,
		expr.Operator,
		fieldType,
		formatComparisonExprStringForError(*expr),
	)
}

// extractStringTerms splits a single SQLVal into multiple tokens if whitespace-separated.
// This helps when building phrase or multiple-term conditions (e.g. paradedb.phrase).
func extractStringTerms(expr sqlparser.Expr) []string {
	strVal, ok := expr.(*sqlparser.SQLVal)
	if !ok {
		return []string{sqlparser.String(expr)}
	}
	val := string(strVal.Val)
	tokens := strings.Fields(val)
	if len(tokens) == 0 {
		return []string{val}
	}
	return tokens
}

// stripQuotes removes single quotes from around a string.
// ParadeDB queries often require unquoted strings as arguments for paradedb.term(), etc.
func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 1 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
