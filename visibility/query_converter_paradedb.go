package visibility

import (
	"fmt"
	"github.com/genme/temporal-paradedb/paradedb"
	"github.com/temporalio/sqlparser"
	"strings"

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

// ParadeDB requires RFC3339 microsecond precision for datetime formats.
const timeFormatForDatetime = "2006-01-02 15:04:05.999999"

// Core fields stored in the ParadeDB index.
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

// paradeDBQueryConverter implements pluginQueryConverter interface.
type paradeDBQueryConverter struct {
	baseConverter     *query.Converter
	expressionConvert *paradedb.ExpressionConverter
}

func (c *paradeDBQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return nil
}

// newParadeDBQueryConverter creates a QueryConverter that uses ParadeDB logic.
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
		PluginQueryConverter: &paradeDBQueryConverter{
			baseConverter:     baseConv,
			expressionConvert: paradedb.NewExpressionConverter(),
		},
		namespaceName: namespaceName,
		namespaceID:   namespaceID,
		saTypeMap:     saTypeMap,
		saMapper:      saMapper,
		queryString:   queryString,
	}
}

// convertKeywordListComparisonExpr produces a simple SQL sqlparser for keyword lists,
// letting expression_converter handle turning it into ParadeDB queries.
func (c *paradeDBQueryConverter) convertKeywordListComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, invalidOperatorErr(expr, "KeywordList")
	}

	field := getFieldName(expr.Left)

	switch expr.Operator {
	case sqlparser.EqualStr:
		// field = 'value'
		return paradedb.BuildBasicComparisonExpr(field, "=", expr.Right), nil
	case sqlparser.NotEqualStr:
		// field != 'value'
		return paradedb.BuildBasicComparisonExpr(field, "!=", expr.Right), nil
	case sqlparser.InStr:
		// field IN ('val1','val2',...)
		return paradedb.BuildInExpr(field, expr.Right), nil
	case sqlparser.NotInStr:
		// field NOT IN ('val1','val2',...)
		return paradedb.BuildNotInExpr(field, expr.Right), nil
	default:
		return nil, invalidOperatorErr(expr, "KeywordList")
	}
}

// convertTextComparisonExpr handles text fields.
func (c *paradeDBQueryConverter) convertTextComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error) {
	if !isSupportedTextOperator(expr.Operator) {
		return nil, invalidOperatorErr(expr, "Text")
	}

	field := getFieldName(expr.Left)
	values := extractStringTerms(expr.Right)
	if len(values) == 0 {
		values = []string{sqlparser.String(expr.Right)}
	}

	// For a single term: field = 'value'
	if len(values) == 1 {
		compareExpr := paradedb.BuildBasicComparisonExpr(field, "=", &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(values[0])})
		if expr.Operator == sqlparser.NotEqualStr {
			return &sqlparser.NotExpr{Expr: compareExpr}, nil
		}
		return compareExpr, nil
	}

	// For multiple terms, represent them as a chain of AND conditions: (field = 'val1' AND field = 'val2')
	// This simulates a "phrase" or multi-term must query. expression_converter will turn AND conditions into must queries.
	var andExpr sqlparser.Expr
	for i, v := range values {
		termExpr := paradedb.BuildBasicComparisonExpr(field, "=", &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte(v)})
		if i == 0 {
			andExpr = termExpr
		} else {
			andExpr = &sqlparser.AndExpr{
				Left:  andExpr,
				Right: termExpr,
			}
		}
	}
	if expr.Operator == sqlparser.NotEqualStr {
		return &sqlparser.NotExpr{Expr: andExpr}, nil
	}
	return &sqlparser.ParenExpr{Expr: andExpr}, nil
}

// convertWhereExpr passes the final AST to ExpressionConverter.
func (c *paradeDBQueryConverter) convertWhereExpr(expr sqlparser.Expr) (string, error) {
	if expr == nil {
		return "", nil
	}
	return c.expressionConvert.ToParadeDBQuery(expr)
}

// buildSelectStmt constructs the final SELECT query.
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
		tokenExpr, tokenArgs := c.buildPaginationClause(token)
		whereClauses = append(whereClauses, sqlparser.String(tokenExpr))
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

// buildCountStmt constructs a COUNT query.
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

// Pagination clause remains the same, producing a ParadeDB boolean query as a raw string.
func (c *paradeDBQueryConverter) buildPaginationClause(token *pageToken) (sqlparser.Expr, []any) {
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

// buildPaginationQuery returns the original pagination boolean query.
func (c *paradeDBQueryConverter) buildPaginationQuery(token *pageToken) sqlparser.Expr {
	// must1 corresponds to:
	// paradedb.term('close_time', token.CloseTime),
	// paradedb.term('start_time', token.StartTime),
	// paradedb.range('run_id', '(%s, null)')
	// '(%s, null)' means run_id > token.RunID
	must1 := paradedb.BuildMust(
		paradedb.BuildTermExpr("close_time", token.CloseTime),
		paradedb.BuildTermExpr("start_time", token.StartTime),
		paradedb.BuildRangeExpr("run_id", token.RunID, ">"),
	)

	// must2 corresponds to:
	// paradedb.term('close_time', token.CloseTime),
	// paradedb.range('start_time', '[null, %s)')
	// '[null, %s)' means start_time < token.StartTime
	must2 := paradedb.BuildMust(
		paradedb.BuildTermExpr("close_time", token.CloseTime),
		paradedb.BuildRangeExpr("start_time", token.StartTime, "<"),
	)

	// The last condition in should array:
	// paradedb.range('close_time', '[null, %s)')
	// '[null, %s)' means close_time < token.CloseTime
	rangeCloseTime := paradedb.BuildRangeExpr("close_time", token.CloseTime, "<")

	// Combine must1, must2, and rangeCloseTime into a should array:
	// paradedb.boolean(should => ARRAY[must1, must2, rangeCloseTime])
	shouldQuery := paradedb.BuildShould(must1, must2, rangeCloseTime)

	// Prepend run_id @@@ to integrate with the ParadeDB operator:
	return paradedb.BuildMatchExpr("run_id", shouldQuery)
}

// FieldNameInterceptor
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
