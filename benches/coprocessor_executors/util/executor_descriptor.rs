// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::executor::{ExecType, Executor as PbExecutor, TopN};
use tipb::expression::{ByItem, Expr};
use tipb::schema::ColumnInfo;

/// Builds a table scan executor descriptor.
pub fn table_scan(columns_info: &[ColumnInfo]) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeTableScan);
    exec.mut_tbl_scan()
        .set_columns(columns_info.to_vec().into());
    exec
}

/// Builds a index scan executor descriptor.
pub fn index_scan(columns_info: &[ColumnInfo], unique: bool) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeIndexScan);
    exec.mut_idx_scan()
        .set_columns(columns_info.to_vec().into());
    exec.mut_idx_scan().set_unique(unique);
    exec
}

/// Builds a selection executor descriptor.
pub fn selection(exprs: &[Expr]) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeSelection);
    exec.mut_selection().set_conditions(exprs.to_vec().into());
    exec
}

/// Builds a simple aggregation executor descriptor.
pub fn simple_aggregate(aggr_exprs: &[Expr]) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeStreamAgg);
    exec.mut_aggregation()
        .set_agg_func(aggr_exprs.to_vec().into());
    exec
}

/// Builds a hash aggregation executor descriptor.
pub fn hash_aggregate(aggr_exprs: &[Expr], group_bys: &[Expr]) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeAggregation);
    exec.mut_aggregation()
        .set_agg_func(aggr_exprs.to_vec().into());
    exec.mut_aggregation()
        .set_group_by(group_bys.to_vec().into());
    exec
}

/// Builds a stream aggregation executor descriptor.
pub fn stream_aggregate(aggr_exprs: &[Expr], group_bys: &[Expr]) -> PbExecutor {
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeStreamAgg);
    exec.mut_aggregation()
        .set_agg_func(aggr_exprs.to_vec().into());
    exec.mut_aggregation()
        .set_group_by(group_bys.to_vec().into());
    exec
}

pub fn top_n(order_by_expr: &[Expr], order_is_desc: &[bool], n: usize) -> PbExecutor {
    let mut meta = TopN::new();
    meta.set_limit(n as u64);
    meta.set_order_by(
        order_by_expr
            .iter()
            .zip(order_is_desc)
            .map(|(expr, desc)| {
                let mut item = ByItem::new();
                item.set_expr(expr.clone());
                item.set_desc(*desc);
                item
            })
            .collect(),
    );
    let mut exec = PbExecutor::new();
    exec.set_tp(ExecType::TypeTopN);
    exec.set_topN(meta);
    exec
}
