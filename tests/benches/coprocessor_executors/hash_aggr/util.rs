// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::{black_box, measurement::Measurement};
use tidb_query_datatype::expr::EvalConfig;
use tidb_query_executors::{
    interface::*, BatchFastHashAggregationExecutor, BatchSlowHashAggregationExecutor,
};
use tikv::storage::Statistics;
use tipb::{Aggregation, Expr};

use crate::util::{bencher::Bencher, FixtureBuilder};

pub trait HashAggrBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> &'static str;

    fn bench(
        &self,
        b: &mut criterion::Bencher<'_, M>,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    );

    fn box_clone(&self) -> Box<dyn HashAggrBencher<M>>;
}

impl<M> Clone for Box<dyn HashAggrBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use batch hash aggregation executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl<M> HashAggrBencher<M> for BatchBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<'_, M>,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            let mut meta = Aggregation::default();
            meta.set_agg_func(aggr_expr.to_vec().into());
            meta.set_group_by(group_by_expr.to_vec().into());
            if BatchFastHashAggregationExecutor::check_supported(&meta).is_ok() {
                let ex = BatchFastHashAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(group_by_expr.to_vec()),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap();
                Box::new(ex) as Box<dyn BatchExecutor<StorageStats = Statistics>>
            } else {
                let ex = BatchSlowHashAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(group_by_expr.to_vec()),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap();
                Box::new(ex) as Box<dyn BatchExecutor<StorageStats = Statistics>>
            }
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn HashAggrBencher<M>> {
        Box::new(Self)
    }
}
