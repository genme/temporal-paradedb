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

const timeFormatForDatetime = "2006-01-02 15:04:05.999999"

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

type paradeDBQueryConverter struct {
	baseConverter *query.Converter
}

func (c *paradeDBQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	// Placeholder for now, implement as needed.
	panic("getCoalesceCloseTimeExpr not implemented")
}

func newParadeDBQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	fnInterceptor := &paradeDBFieldNameInterceptor{namespaceID: namespaceID, saMapper: saMapper}
	fvInterceptor := &paradeDBFieldValueInterceptor{saTypeMap: saTypeMap}

	rangeCond := query.NewRangeCondConverter(fnInterceptor, fvInterceptor, false)
	comparisonExpr := query.NewComparisonExprConverter(fnInterceptor, fvInterceptor, allowedComparisonOperators, saTypeMap)
	isConv := query.NewIsConverter(fnInterceptor)

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

func (c *paradeDBQueryConverter) convertKeywordListComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, invalidOperatorErr(expr, "KeywordList")
	}

	field := strings.ToLower(getFieldName(expr.Left))
	buildTerm := func(field, val string) sqlparser.Expr {
		return buildTermFunc("paradedb.term", "$."+field, val)
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		val := stripQuotes(sqlparser.String(expr.Right))
		binExpr := buildBinaryExpr("run_id", "@@@", buildTerm(field, val))
		if expr.Operator == sqlparser.NotEqualStr {
			return &sqlparser.NotExpr{Expr: binExpr}, nil
		}
		return binExpr, nil

	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, ok := expr.Right.(sqlparser.ValTuple)
		if !ok {
			return nil, query.NewConverterError(
				"%s: expected tuple for IN operator in %s",
				query.InvalidExpressionErrMessage,
				formatComparisonExprStringForError(*expr),
			)
		}

		// Handle empty tuple case now by returning a condition that is always false.
		if len(valTupleExpr) == 0 {
			return &sqlparser.BinaryExpr{
				Left:     &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("0")},
				Operator: "=",
				Right:    &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("1")},
			}, nil
		}

		termExprs := make([]sqlparser.Expr, 0, len(valTupleExpr))
		for _, v := range valTupleExpr {
			val := stripQuotes(sqlparser.String(v))
			termExprs = append(termExprs, buildBinaryExpr("search_attributes", "@@@", buildTerm(field, val)))
		}

		// FIX: Replace ArrayExpr with a FuncExpr named ARRAY
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
		// Single term: run_id @@@ paradedb.term(field, val)
		val := values[0]
		finalExpr = buildBinaryExpr("run_id", "@@@", buildTermFunc("paradedb.term", fieldName, val))
		if expr.Operator == sqlparser.NotEqualStr {
			finalExpr = &sqlparser.NotExpr{Expr: finalExpr}
		}
		return finalExpr, nil

	default:
		// Multiple terms: run_id @@@ paradedb.phrase(field, ARRAY[val1, val2, ...])
		phraseFunc := buildPhraseFunc(fieldName, values)
		finalExpr = buildBinaryExpr("run_id", "@@@", phraseFunc)
		if expr.Operator == sqlparser.NotEqualStr {
			finalExpr = &sqlparser.NotExpr{Expr: finalExpr}
		}
		return finalExpr, nil
	}
}

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
		tokenQuery, tokenArgs := c.buildPaginationClause(token)
		whereClauses = append(whereClauses, tokenQuery)
		args = append(args, tokenArgs...)
	}

	scoreExpr := "paradedb.score(run_id) as query_score"
	selectFields := append(dbCoreFields, scoreExpr)
	args = append(args, pageSize)

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

func (c *paradeDBQueryConverter) getDatetimeFormat() string {
	return timeFormatForDatetime
}

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

type paradeDBFieldNameInterceptor struct {
	namespaceID namespace.ID
	saMapper    searchattribute.Mapper
}

func (p *paradeDBFieldNameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	if alias, err := p.saMapper.GetFieldName(name, p.namespaceID.String()); err == nil {
		return alias, nil
	}
	return name, nil
}

type paradeDBFieldValueInterceptor struct {
	saTypeMap searchattribute.NameTypeMap
}

func (p *paradeDBFieldValueInterceptor) Values(name string, fieldName string, values ...interface{}) ([]interface{}, error) {
	return p.ConvertFieldValues(fieldName, values...)
}

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

func buildBaseWhereClauses(namespaceID namespace.ID, queryString string) (string, []any) {
	whereClauses := []string{"namespace_id = ?"}
	args := []any{namespaceID.String()}
	if queryString != "" {
		whereClauses = append(whereClauses, queryString)
	}
	return strings.Join(whereClauses, " AND "), args
}

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

func buildBinaryExpr(left string, operator string, right sqlparser.Expr) *sqlparser.BinaryExpr {
	return &sqlparser.BinaryExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(left)},
		Operator: operator,
		Right:    right,
	}
}

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

func invalidOperatorErr(expr *sqlparser.ComparisonExpr, fieldType string) error {
	return query.NewConverterError(
		"%s: operator '%s' not supported for %s in %s",
		query.InvalidExpressionErrMessage,
		expr.Operator,
		fieldType,
		formatComparisonExprStringForError(*expr),
	)
}

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

func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 1 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
