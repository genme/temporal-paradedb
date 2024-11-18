package visibility

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

type (
	queryConverterSuite struct {
		suite.Suite
		*require.Assertions

		pqc            PluginQueryConverter
		queryConverter *QueryConverter
	}

	testCase struct {
		name     string
		input    string
		args     map[string]interface{}
		output   interface{}
		retValue interface{}
		err      error
		setup    func()
	}

	paradedbQueryConverterSuite struct {
		queryConverterSuite
	}
)

func TestParadeDBQueryConverterSuite(t *testing.T) {
	s := &paradedbQueryConverterSuite{
		queryConverterSuite: queryConverterSuite{
			pqc: &paradedbQueryConverter{},
		},
	}
	suite.Run(t, s)
}

// SetupTest initializes the Assertions before each test
func (s *queryConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *paradedbQueryConverterSuite) TestGetCoalesceCloseTimeExpr() {
	expr := s.pqc.getCoalesceCloseTimeExpr()
	s.Equal(
		"coalesce(close_time, '9999-12-31 23:59:59.999999')",
		sqlparser.String(expr),
	)
}

func (s *paradedbQueryConverterSuite) TestConvertKeywordListComparisonExpr() {
	tests := []testCase{
		{
			name:   "invalid operator",
			input:  "AliasForKeywordList01 < 'foo'",
			output: "",
			err: query.NewConverterError(
				"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
				query.InvalidExpressionErrMessage,
				sqlparser.LessThanStr,
				"AliasForKeywordList01 < 'foo'",
			),
		},
		{
			name:   "valid equal expression",
			input:  "AliasForKeywordList01 = 'foo'",
			output: "search_attributes @@@ paradedb.term('$.KeywordList01', 'foo')",
			err:    nil,
		},
		{
			name:   "valid not equal expression",
			input:  "AliasForKeywordList01 != 'foo'",
			output: "not (search_attributes @@@ paradedb.term('$.KeywordList01', 'foo'))",
			err:    nil,
		},
		{
			name:   "valid in expression",
			input:  "AliasForKeywordList01 in ('foo', 'bar')",
			output: "(search_attributes @@@ paradedb.boolean(should => ARRAY[paradedb.term('$.KeywordList01', 'foo'), paradedb.term('$.KeywordList01', 'bar')]))",
			err:    nil,
		},
		{
			name:   "valid not in expression",
			input:  "AliasForKeywordList01 not in ('foo', 'bar')",
			output: "not (search_attributes @@@ paradedb.boolean(should => ARRAY[paradedb.term('$.KeywordList01', 'foo'), paradedb.term('$.KeywordList01', 'bar')]))",
			err:    nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr)
			result, err := s.pqc.convertKeywordListComparisonExpr(expr)

			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, sqlparser.String(result))
			} else {
				s.Error(err)
				s.Equal(tc.err.Error(), err.Error())
			}
		})
	}
}

func (s *paradedbQueryConverterSuite) TestConvertTextComparisonExpr() {
	tests := []testCase{
		{
			name:   "invalid operator",
			input:  "AliasForText01 < 'foo'",
			output: "",
			err: query.NewConverterError(
				"%s: operator '%s' not supported for Text type search attribute in `%s`",
				query.InvalidExpressionErrMessage,
				sqlparser.LessThanStr,
				"AliasForText01 < 'foo'",
			),
		},
		{
			name:   "valid equal expression",
			input:  "AliasForText01 = 'foo bar'",
			output: "(namespace_id, run_id) @@@ paradedb.phrase('Text01', ARRAY['foo', 'bar'])",
			err:    nil,
		},
		{
			name:   "valid not equal expression",
			input:  "AliasForText01 != 'foo bar'",
			output: "not ((namespace_id, run_id) @@@ paradedb.phrase('Text01', ARRAY['foo', 'bar']))",
			err:    nil,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			sql := fmt.Sprintf("select * from table1 where %s", tc.input)
			stmt, err := sqlparser.Parse(sql)
			s.NoError(err)
			expr := stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr)
			result, err := s.pqc.convertTextComparisonExpr(expr)

			if tc.err == nil {
				s.NoError(err)
				s.Equal(tc.output, sqlparser.String(result))
			} else {
				s.Error(err)
				s.Equal(tc.err.Error(), err.Error())
			}
		})
	}
}

func (s *paradedbQueryConverterSuite) TestBuildPaginationQuery() {
	// Implement test cases for buildPaginationQuery if needed
}

func (s *paradedbQueryConverterSuite) TestBuildCountStmt() {
	namespaceID := namespace.ID("test-namespace")
	queryString := "status = 1" // 1 = RUNNING in WorkflowExecutionStatus enum

	// Test without group by
	query, args := s.pqc.buildCountStmt(namespaceID, queryString, nil)
	expectedQuery := "SELECT COUNT(*) FROM pdb_executions_visibility WHERE search_attributes @@@ paradedb.term('namespace_id', ?) AND status = 1"
	s.Equal(expectedQuery, query)
	s.Equal([]interface{}{namespaceID.String()}, args)

	// Test with group by
	groupBy := []string{"status", "workflow_type_name"}
	query, args = s.pqc.buildCountStmt(namespaceID, queryString, groupBy)
	expectedGroupByQuery := "SELECT status, workflow_type_name, COUNT(*) FROM pdb_executions_visibility WHERE search_attributes @@@ paradedb.term('namespace_id', ?) AND status = 1 GROUP BY status, workflow_type_name"
	s.Equal(expectedGroupByQuery, query)
	s.Equal([]interface{}{namespaceID.String()}, args)
}

func (s *paradedbQueryConverterSuite) TestBuildSelectStmt() {
	namespaceID := namespace.ID("test-namespace")
	queryString := "status = 1" // 1 = RUNNING in WorkflowExecutionStatus enum
	pageSize := 10

	// Test without pagination token
	query, args := s.pqc.buildSelectStmt(namespaceID, queryString, pageSize, nil)

	expectedQuery := `SELECT execution_fields, paradedb.score((namespace_id, run_id)) as query_score 
FROM pdb_executions_visibility 
WHERE search_attributes @@@ paradedb.term('namespace_id', ?) 
AND status = 1 
ORDER BY 
	COALESCE(close_time, '9999-12-31 23:59:59.999999') DESC,  
	start_time DESC,
	run_id DESC,
	query_score DESC
LIMIT ?`

	s.Equal(expectedQuery, query)
	s.Equal([]interface{}{namespaceID.String(), pageSize}, args)

	// Test with pagination token
	token := &pageToken{
		StartTime: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		CloseTime: time.Date(2024, time.February, 1, 0, 0, 0, 0, time.UTC),
		RunID:     "test-run-id",
	}

	query, args = s.pqc.buildSelectStmt(namespaceID, queryString, pageSize, token)
	expectedArgsWithToken := []interface{}{
		namespaceID.String(),
		token.CloseTime,
		token.StartTime,
		token.RunID,
		token.CloseTime,
		token.StartTime,
		token.CloseTime,
		pageSize,
	}

	s.Contains(query, "WHERE")
	s.Contains(query, "ORDER BY")
	s.Equal(expectedArgsWithToken, args)
}

func (s *paradedbQueryConverterSuite) TestBuildSearchAttributesQuery() {
	namespaceID := namespace.ID("test-namespace")
	// Query using custom search attributes stored in search_attributes JSONB
	queryString := "search_attributes @@@ paradedb.term('$.CustomDateField', '2024-01-01')"
	pageSize := 10

	query, args := s.pqc.buildSelectStmt(namespaceID, queryString, pageSize, nil)

	expectedQuery := `SELECT execution_fields, paradedb.score((namespace_id, run_id)) as query_score 
FROM pdb_executions_visibility 
WHERE search_attributes @@@ paradedb.term('namespace_id', ?) 
AND search_attributes @@@ paradedb.term('$.CustomDateField', '2024-01-01')
ORDER BY 
	COALESCE(close_time, '9999-12-31 23:59:59.999999') DESC,  
	start_time DESC,
	run_id DESC,
	query_score DESC
LIMIT ?`

	s.Equal(expectedQuery, query)
	s.Equal([]interface{}{namespaceID.String(), pageSize}, args)
}
