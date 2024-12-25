package paradedb

import "github.com/temporalio/sqlparser"

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
