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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	paradedbQueryConverterSuite struct {
		suite.Suite
		queryConverter *paradedbQueryConverter
		namespaceName  namespace.Name
		namespaceID    namespace.ID
	}
)

func TestParadeDBQueryConverterSuite(t *testing.T) {
	s := &paradedbQueryConverterSuite{
		queryConverter: &paradedbQueryConverter{},
		namespaceName:  "test-namespace",
		namespaceID:    "test-namespace-id",
	}
	suite.Run(t, s)
}

func (s *paradedbQueryConverterSuite) TestConvertKeywordListComparisonExpr() {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
		errMsg   string
	}{
		{
			name:    "invalid operator",
			input:   "KeywordList01 < 'foo'",
			wantErr: true,
			errMsg:  "operator '<' not supported for KeywordList type",
		},
		{
			name:     "equal",
			input:    "KeywordList01 = 'foo'",
			expected: "id @@@ paradedb.json_term('search_attributes', '$.KeywordList01', 'foo')",
		},
		{
			name:     "not equal",
			input:    "KeywordList01 != 'foo'",
			expected: "NOT (id @@@ paradedb.json_term('search_attributes', '$.KeywordList01', 'foo'))",
		},
		{
			name:     "in clause",
			input:    "KeywordList01 in ('foo', 'bar')",
			expected: "(id @@@ paradedb.json_term('search_attributes', '$.KeywordList01', 'foo') OR id @@@ paradedb.json_term('search_attributes', '$.KeywordList01', 'bar'))",
		},
		{
			name:     "not in",
			input:    "KeywordList01 not in ('foo', 'bar')",
			expected: "NOT (id @@@ paradedb.json_term('search_attributes', '$.KeywordList01', 'foo') OR id @@@ paradedb.json_term('search_attributes', '$.KeywordList01', 'bar'))",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			expr := s.buildComparisonExpr(tc.input)
			result, err := s.queryConverter.convertKeywordListComparisonExpr(expr)

			if tc.wantErr {
				s.Error(err)
				s.Contains(err.Error(), tc.errMsg)
			} else {
				s.NoError(err)
				s.Equal(tc.expected, sqlparser.String(result))
			}
		})
	}
}

func (s *paradedbQueryConverterSuite) TestConvertTextComparisonExpr() {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
		errMsg   string
	}{
		{
			name:    "invalid operator",
			input:   "Text01 < 'foo'",
			wantErr: true,
			errMsg:  "operator '<' not supported for Text type",
		},
		{
			name:     "equal",
			input:    "Text01 = 'foo'",
			expected: "id @@@ paradedb.phrase_match('search_attributes', '$.Text01', 'foo')",
		},
		{
			name:     "not equal",
			input:    "Text01 != 'foo'",
			expected: "NOT (id @@@ paradedb.phrase_match('search_attributes', '$.Text01', 'foo'))",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			expr := s.buildComparisonExpr(tc.input)
			result, err := s.queryConverter.convertTextComparisonExpr(expr)

			if tc.wantErr {
				s.Error(err)
				s.Contains(err.Error(), tc.errMsg)
			} else {
				s.NoError(err)
				s.Equal(tc.expected, sqlparser.String(result))
			}
		})
	}
}

func (s *paradedbQueryConverterSuite) TestBuildSelectStmt() {
	tests := []struct {
		name          string
		query         string
		pageSize      int
		token         *pageToken
		expectedQuery string
		expectedArgs  int
	}{
		{
			name:     "basic query",
			query:    "WorkflowType = 'test-workflow'",
			pageSize: 10,
			token:    nil,
			expectedQuery: "SELECT * FROM executions_visibility " +
				"WHERE namespace_id = ? AND WorkflowType = 'test-workflow' " +
				"ORDER BY COALESCE(close_time, '9999-12-31 23:59:59.999999') DESC, start_time DESC, run_id LIMIT ?",
			expectedArgs: 2,
		},
		{
			name:     "with pagination",
			query:    "WorkflowType = 'test-workflow'",
			pageSize: 10,
			token: &pageToken{
				StartTime: time.Now(),
				CloseTime: time.Now(),
				RunID:     "test-run-id",
			},
			expectedQuery: "SELECT * FROM executions_visibility " +
				"WHERE namespace_id = ? AND WorkflowType = 'test-workflow' " +
				"AND ((COALESCE(close_time, '9999-12-31 23:59:59.999999') = ? AND start_time = ? AND run_id > ?) " +
				"OR (COALESCE(close_time, '9999-12-31 23:59:59.999999') = ? AND start_time < ?) " +
				"OR COALESCE(close_time, '9999-12-31 23:59:59.999999') < ?) " +
				"ORDER BY COALESCE(close_time, '9999-12-31 23:59:59.999999') DESC, start_time DESC, run_id LIMIT ?",
			expectedArgs: 8,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			query, args := s.queryConverter.buildSelectStmt(
				s.namespaceID,
				tc.query,
				tc.pageSize,
				tc.token,
			)
			s.Contains(query, tc.expectedQuery)
			s.Equal(tc.expectedArgs, len(args))
		})
	}
}

func (s *paradedbQueryConverterSuite) TestBuildCountStmt() {
	tests := []struct {
		name          string
		query         string
		groupBy       []string
		expectedQuery string
		expectedArgs  int
	}{
		{
			name:    "simple count",
			query:   "WorkflowType = 'test-workflow'",
			groupBy: nil,
			expectedQuery: "SELECT COUNT(*) FROM executions_visibility " +
				"WHERE (namespace_id = ?) AND WorkflowType = 'test-workflow'",
			expectedArgs: 1,
		},
		{
			name:    "count with group by",
			query:   "WorkflowType = 'test-workflow'",
			groupBy: []string{"ExecutionStatus"},
			expectedQuery: "SELECT ExecutionStatus, COUNT(*) FROM executions_visibility " +
				"WHERE (namespace_id = ?) AND WorkflowType = 'test-workflow' " +
				"GROUP BY ExecutionStatus",
			expectedArgs: 1,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			query, args := s.queryConverter.buildCountStmt(
				s.namespaceID,
				tc.query,
				tc.groupBy,
			)
			s.Contains(query, tc.expectedQuery)
			s.Equal(tc.expectedArgs, len(args))
		})
	}
}

func (s *paradedbQueryConverterSuite) TestGetFieldName() {
	tests := []struct {
		name     string
		expr     sqlparser.Expr
		expected string
	}{
		{
			name: "saColName",
			expr: newSAColName(
				"db_col_name",
				"alias",
				"field_name",
				enumspb.INDEXED_VALUE_TYPE_TEXT,
			),
			expected: "field_name",
		},
		{
			name:     "colName",
			expr:     newColName("test_name"),
			expected: "test_name",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			result := getFieldName(tc.expr)
			s.Equal(tc.expected, result)
		})
	}
}

// Helper function to build comparison expressions for testing
func (s *paradedbQueryConverterSuite) buildComparisonExpr(query string) *sqlparser.ComparisonExpr {
	sql := fmt.Sprintf("select * from table1 where %s", query)
	stmt, err := sqlparser.Parse(sql)
	s.NoError(err)

	whereExpr := stmt.(*sqlparser.Select).Where.Expr
	expr, ok := whereExpr.(*sqlparser.ComparisonExpr)
	s.True(ok)

	return expr
}
