package visibility

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

// Provided test mapper implementation
func newMapper(
	getAlias func(fieldName, ns string) (string, error),
	getFieldName func(alias, ns string) (string, error),
) searchattribute.Mapper {
	return &FlexibleMapper{
		GetAliasFunc:     getAlias,
		GetFieldNameFunc: getFieldName,
	}
}

type FlexibleMapper struct {
	GetAliasFunc     func(fieldName, namespace string) (string, error)
	GetFieldNameFunc func(alias, namespace string) (string, error)
}

func (m *FlexibleMapper) GetAlias(fieldName, ns string) (string, error) {
	return m.GetAliasFunc(fieldName, ns)
}

func (m *FlexibleMapper) GetFieldName(alias, ns string) (string, error) {
	return m.GetFieldNameFunc(alias, ns)
}

// testCase defines a common structure for testing comparison sqlparser conversions
type testCase struct {
	name   string
	input  string
	output string
	err    error
}

// paradeDBQueryConverterSuite is the test suite for paradeDBQueryConverter
type paradeDBQueryConverterSuite struct {
	suite.Suite
	queryConverter *QueryConverter
	saTypeMap      searchattribute.NameTypeMap
}

// Ensure that the test suite runs with `go test`
func TestParadeDBQueryConverterSuite(t *testing.T) {
	suite.Run(t, new(paradeDBQueryConverterSuite))
}

// SetupTest initializes the QueryConverter with test parameters
func (s *paradeDBQueryConverterSuite) SetupTest() {
	nsName := namespace.Name("test-namespace")
	nsID := namespace.ID("test-namespace-id")

	// Define a simple NameTypeMap for testing fields
	s.saTypeMap = searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"KeywordListField": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"TextField":        enumspb.INDEXED_VALUE_TYPE_TEXT,
		"IntField":         enumspb.INDEXED_VALUE_TYPE_INT,
	})

	// Use the provided test mapper implementation
	saMapper := newMapper(
		func(fieldName, ns string) (string, error) {
			return fieldName, nil
		},
		func(alias, ns string) (string, error) {
			return alias, nil
		},
	)

	// Use the exported constructor to create the QueryConverter for ParadeDB
	s.queryConverter = newParadeDBQueryConverter(
		nsName,
		nsID,
		s.saTypeMap,
		saMapper,
		"",
	)
}

func (s *paradeDBQueryConverterSuite) TestConvertKeywordListComparisonExpr() {
	c := s.queryConverter.PluginQueryConverter.(*paradeDBQueryConverter)

	tests := []testCase{
		{
			name:  "invalid operator",
			input: "KeywordListField < 'foo'",
			err: query.NewConverterError(
				"%s: operator '%s' not supported for KeywordList in %s",
				query.InvalidExpressionErrMessage,
				sqlparser.LessThanStr,
				"KeywordListField < 'foo'",
			),
		},
		{
			name:   "valid EQUAL sqlparser",
			input:  "KeywordListField = 'foo'",
			output: "run_id @@@ paradedb.term('$.keywordlistfield', 'foo')",
		},
		{
			name:   "valid NOT EQUAL sqlparser",
			input:  "KeywordListField != 'foo'",
			output: "not run_id @@@ paradedb.term('$.keywordlistfield', 'foo')",
		},
		{
			name:   "valid IN sqlparser",
			input:  "KeywordListField in ('foo','bar')",
			output: "run_id @@@ paradedb.boolean(should => ARRAY[search_attributes @@@ paradedb.term('$.keywordlistfield', 'foo'), search_attributes @@@ paradedb.term('$.keywordlistfield', 'bar')])",
		},
		{
			name:   "valid NOT IN sqlparser",
			input:  "KeywordListField not in ('foo','bar')",
			output: "not run_id @@@ paradedb.boolean(should => ARRAY[search_attributes @@@ paradedb.term('$.keywordlistfield', 'foo'), search_attributes @@@ paradedb.term('$.keywordlistfield', 'bar')])",
		},
		{
			name:   "Unknown field EQUAL",
			input:  "UnknownField = 'foo'",
			output: "run_id @@@ paradedb.term('$.unknownfield', 'foo')",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sqlStr := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sqlStr)
			s.NoError(err)

			expr := stmt.(*sqlparser.Select).Where.Expr
			compExpr, ok := expr.(*sqlparser.ComparisonExpr)
			s.Require().True(ok)

			newExpr, convErr := c.convertKeywordListComparisonExpr(compExpr)
			if tc.err == nil {
				s.NoError(convErr)
				s.Equal(tc.output, sqlparser.String(newExpr))
			} else {
				s.Error(convErr)
				s.Equal(tc.err.Error(), convErr.Error())
			}
		})
	}
}

func (s *paradeDBQueryConverterSuite) TestConvertTextComparisonExpr() {
	c := s.queryConverter.PluginQueryConverter.(*paradeDBQueryConverter)

	tests := []testCase{
		{
			name:  "invalid operator",
			input: "TextField < 'foo'",
			err: query.NewConverterError(
				"%s: operator '%s' not supported for Text in %s",
				query.InvalidExpressionErrMessage,
				sqlparser.LessThanStr,
				"TextField < 'foo'",
			),
		},
		{
			name:   "valid EQUAL single term",
			input:  "TextField = 'foo'",
			output: "run_id @@@ paradedb.term('textfield', 'foo')",
		},
		{
			name:   "valid EQUAL multiple terms",
			input:  "TextField = 'foo bar'",
			output: "run_id @@@ paradedb.phrase('textfield', ARRAY['foo','bar'])",
		},
		{
			name:   "valid NOT EQUAL multiple terms",
			input:  "TextField != 'hello world'",
			output: "not run_id @@@ paradedb.phrase('textfield', ARRAY['hello','world'])",
		},
		{
			name:   "Unknown field EQUAL",
			input:  "UnknownTextField = 'test val'",
			output: "run_id @@@ paradedb.term('$.unknowntextfield', 'test val')",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sqlStr := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sqlStr)
			s.NoError(err)

			expr := stmt.(*sqlparser.Select).Where.Expr
			compExpr, ok := expr.(*sqlparser.ComparisonExpr)
			s.Require().True(ok)

			newExpr, convErr := c.convertTextComparisonExpr(compExpr)
			if tc.err == nil {
				s.NoError(convErr)
				s.Equal(tc.output, sqlparser.String(newExpr))
			} else {
				s.Error(convErr)
				s.Equal(tc.err.Error(), convErr.Error())
			}
		})
	}
}

func (s *paradeDBQueryConverterSuite) TestPagination() {
	c := s.queryConverter.PluginQueryConverter.(*paradeDBQueryConverter)

	nsID := namespace.ID("test-namespace-id")
	queryString := "run_id @@@ paradedb.term('$.keywordlistfield', 'foo')"

	// No pagination token, no query
	sqlNoQueryNoToken, argsNoQueryNoToken := c.buildSelectStmt(nsID, "", 10, nil)
	s.Contains(sqlNoQueryNoToken, "WHERE namespace_id = ?")
	s.NotContains(sqlNoQueryNoToken, "paradedb.term('$.keywordlistfield', 'foo')")
	s.Equal([]any{"test-namespace-id", 10}, argsNoQueryNoToken)

	// Query, no token
	sqlNoToken, argsNoToken := c.buildSelectStmt(nsID, queryString, 10, nil)
	s.Contains(sqlNoToken, "WHERE namespace_id = ? AND run_id @@@ paradedb.term('$.keywordlistfield', 'foo')")
	s.Equal([]any{"test-namespace-id", 10}, argsNoToken)

	// With token
	closeTime, _ := time.Parse("2006-01-02 15:04:05.000000", "2024-01-01 00:00:00.000000")
	startTime, _ := time.Parse("2006-01-02 15:04:05.000000", "2023-01-01 12:34:56.000000")

	token := &pageToken{
		CloseTime: closeTime,
		StartTime: startTime,
		RunID:     "some-run-id",
	}
	sqlWithToken, argsWithToken := c.buildSelectStmt(nsID, queryString, 10, token)
	s.Contains(sqlWithToken, "run_id @@@ paradedb.boolean(")
	s.Equal([]any{
		"test-namespace-id",
		token.CloseTime,
		token.StartTime,
		token.RunID,
		token.CloseTime,
		token.StartTime,
		token.CloseTime,
		10,
	}, argsWithToken)

	// Empty token fields
	emptyToken := &pageToken{}
	sqlEmptyToken, argsEmptyToken := c.buildSelectStmt(nsID, queryString, 10, emptyToken)
	s.Contains(sqlEmptyToken, "run_id @@@ paradedb.boolean(")
	s.Equal([]any{
		"test-namespace-id",
		emptyToken.CloseTime, emptyToken.StartTime, emptyToken.RunID,
		emptyToken.CloseTime, emptyToken.StartTime, emptyToken.CloseTime,
		10,
	}, argsEmptyToken)

	// Very large page size
	sqlLargePage, argsLargePage := c.buildSelectStmt(nsID, queryString, 1000000, nil)
	s.Contains(sqlLargePage, "LIMIT ?")
	s.Equal([]any{"test-namespace-id", 1000000}, argsLargePage)

	// Zero page size
	sqlZeroPage, argsZeroPage := c.buildSelectStmt(nsID, queryString, 0, nil)
	s.Contains(sqlZeroPage, "LIMIT ?")
	s.Equal([]any{"test-namespace-id", 0}, argsZeroPage)

	// Negative page size
	sqlNegativePage, argsNegativePage := c.buildSelectStmt(nsID, queryString, -1, nil)
	s.Contains(sqlNegativePage, "LIMIT ?")
	s.Equal([]any{"test-namespace-id", -1}, argsNegativePage)

	// Token but no query
	sqlTokenNoQuery, argsTokenNoQuery := c.buildSelectStmt(nsID, "", 5, token)
	s.Contains(sqlTokenNoQuery, "WHERE namespace_id = ? AND run_id @@@ paradedb.boolean(")
	s.Equal([]any{
		"test-namespace-id",
		token.CloseTime,
		token.StartTime,
		token.RunID,
		token.CloseTime,
		token.StartTime,
		token.CloseTime,
		5,
	}, argsTokenNoQuery)
}

func (s *paradeDBQueryConverterSuite) TestEmptyAndNoopQueries() {
	c := s.queryConverter.PluginQueryConverter.(*paradeDBQueryConverter)

	nsID := namespace.ID("test-namespace-id")

	// Empty query string, no token, just the namespace condition
	sqlEmpty, argsEmpty := c.buildSelectStmt(nsID, "", 10, nil)
	s.Contains(sqlEmpty, "WHERE namespace_id = ?")
	s.NotContains(sqlEmpty, "AND")
	s.Equal([]any{"test-namespace-id", 10}, argsEmpty)
}
