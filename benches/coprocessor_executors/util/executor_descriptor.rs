// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf::RepeatedField;

use tipb::executor::{ExecType, Executor as PbExecutor};
use tipb::expression::Expr;
use tipb::schema::ColumnInfo;

/// Builds a table scan executor descriptor.
pub fn table_scan(columns_info: &[ColumnInfo]) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeTableScan);
    exec.mut_tbl_scan()
        .set_columns(RepeatedField::from_slice(columns_info));
    exec
}

/// Builds a simple aggregate executor descriptor.
pub fn simple_aggregate(aggr_expr: &Expr) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeStreamAgg);
    exec.mut_aggregation()
        .mut_agg_func()
        .push(aggr_expr.clone());
    exec
}
