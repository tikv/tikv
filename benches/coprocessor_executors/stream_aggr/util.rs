// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;

use tipb::expression::Expr;

use tikv::coprocessor::dag::batch::executors::BatchStreamAggregationExecutor;
use tikv::coprocessor::dag::batch::interface::BatchExecutor;
use tikv::coprocessor::dag::executor::{Executor, StreamAggExecutor};
use tikv::coprocessor::dag::expr::EvalConfig;

use crate::util::bencher::Bencher;
use crate::util::executor_descriptor::stream_aggregate;
use crate::util::FixtureBuilder;

pub trait StreamAggrBencher {
    fn name(&self) -> &'static str;

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    );

    fn box_clone(&self) -> Box<dyn StreamAggrBencher>;
}

impl Clone for Box<dyn StreamAggrBencher> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal stream aggregation executor to bench the giving aggregate
/// expression.
pub struct NormalBencher;

impl StreamAggrBencher for NormalBencher {
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let meta = stream_aggregate(aggr_expr, group_by_expr).take_aggregation();
            let src = fb.clone().build_normal_fixture_executor();
            Box::new(
                StreamAggExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(meta),
                )
                .unwrap(),
            ) as Box<dyn Executor>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn StreamAggrBencher> {
        Box::new(Self)
    }
}

/// A bencher that will use batch stream aggregation executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl StreamAggrBencher for BatchBencher {
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        fb: &FixtureBuilder,
        group_by_expr: &[Expr],
        aggr_expr: &[Expr],
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchStreamAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(group_by_expr.to_vec()),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap(),
            ) as Box<dyn BatchExecutor>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn StreamAggrBencher> {
        Box::new(Self)
    }
}
