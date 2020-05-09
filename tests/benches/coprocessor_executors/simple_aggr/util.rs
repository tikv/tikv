// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;
use criterion::measurement::Measurement;

use tipb::Expr;

use tidb_query_datatype::expr::EvalConfig;
use tidb_query_normal_executors::{Executor, StreamAggExecutor};
use tidb_query_vec_executors::interface::BatchExecutor;
use tidb_query_vec_executors::BatchSimpleAggregationExecutor;
use tikv::storage::Statistics;

use crate::util::bencher::Bencher;
use crate::util::executor_descriptor::simple_aggregate;
use crate::util::FixtureBuilder;

pub trait SimpleAggrBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> &'static str;

    fn bench(&self, b: &mut criterion::Bencher<M>, fb: &FixtureBuilder, aggr_expr: &[Expr]);

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher<M>>;
}

impl<M> Clone for Box<dyn SimpleAggrBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal stream aggregation executor without a group by to bench the
/// giving aggregate expression.
pub struct NormalBencher;

impl<M> SimpleAggrBencher<M> for NormalBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(&self, b: &mut criterion::Bencher<M>, fb: &FixtureBuilder, aggr_expr: &[Expr]) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let meta = simple_aggregate(aggr_expr).take_aggregation();
            let src = fb.clone().build_normal_fixture_executor();
            Box::new(
                StreamAggExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(meta),
                )
                .unwrap(),
            ) as Box<dyn Executor<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher<M>> {
        Box::new(Self)
    }
}

/// A bencher that will use batch simple aggregation executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl<M> SimpleAggrBencher<M> for BatchBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(&self, b: &mut criterion::Bencher<M>, fb: &FixtureBuilder, aggr_expr: &[Expr]) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchSimpleAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap(),
            ) as Box<dyn BatchExecutor<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher<M>> {
        Box::new(Self)
    }
}
