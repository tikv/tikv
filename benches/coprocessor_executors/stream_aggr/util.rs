// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;

use tipb::executor::Aggregation;
use tipb::expression::Expr;

use tikv::coprocessor::dag::batch::executors::BatchStreamAggregationExecutor;
use tikv::coprocessor::dag::executor::StreamAggExecutor;
use tikv::coprocessor::dag::expr::EvalConfig;

use crate::util::bencher::Bencher;
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
            let mut meta = Aggregation::new();
            meta.set_agg_func(aggr_expr.to_vec().into());
            meta.set_group_by(group_by_expr.to_vec().into());
            let src = fb.clone().build_normal_fixture_executor();
            StreamAggExecutor::new(
                black_box(Arc::new(EvalConfig::default())),
                black_box(Box::new(src)),
                black_box(meta),
            )
            .unwrap()
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
            BatchStreamAggregationExecutor::new(
                black_box(Arc::new(EvalConfig::default())),
                black_box(Box::new(src)),
                black_box(group_by_expr.to_vec()),
                black_box(aggr_expr.to_vec()),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn StreamAggrBencher> {
        Box::new(Self)
    }
}
