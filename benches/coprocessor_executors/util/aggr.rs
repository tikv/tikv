// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
use tipb::expression::{Expr, ExprType};

use tikv_util::codec::number::NumberEncoder;

/// Creates an aggregate expression for `COUNT(1)`.
pub fn create_expr_count_1() -> Expr {
    let mut aggr_expr = Expr::new();
    aggr_expr.set_tp(ExprType::Count);
    aggr_expr.mut_children().push({
        let mut expr = Expr::new();
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.set_tp(ExprType::Int64);
        expr.mut_val().encode_i64(1).unwrap();
        expr
    });
    aggr_expr
}

/// Creates an aggregate expression for `COUNT(col)`.
pub fn create_expr_count_column(column_offset: usize, column_field_type: FieldTypeTp) -> Expr {
    let mut aggr_expr = Expr::new();
    aggr_expr.set_tp(ExprType::Count);
    aggr_expr.mut_children().push({
        let mut expr = Expr::new();
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(column_field_type);
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(column_offset as i64).unwrap();
        expr
    });
    aggr_expr
}
