// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;

use tipb::expression::Expr;

use tikv::coprocessor::dag::batch::executors::BatchSimpleAggregationExecutor;
use tikv::coprocessor::dag::batch::interface::BatchExecutor;
use tikv::coprocessor::dag::executor::{Executor, StreamAggExecutor};
use tikv::coprocessor::dag::expr::EvalConfig;

use crate::util::bencher::Bencher;
use crate::util::executor_descriptor::simple_aggregate;
use crate::util::FixtureBuilder;

pub trait SimpleAggrBencher {
    fn name(&self) -> &'static str;

    fn bench(&self, b: &mut criterion::Bencher, fb: &FixtureBuilder, aggr_expr: &[Expr]);

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher>;
}

impl Clone for Box<dyn SimpleAggrBencher> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal stream aggregation executor without a group by to bench the
/// giving aggregate expression.
pub struct NormalBencher;

impl SimpleAggrBencher for NormalBencher {
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(&self, b: &mut criterion::Bencher, fb: &FixtureBuilder, aggr_expr: &[Expr]) {
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
            ) as Box<dyn Executor>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher> {
        Box::new(Self)
    }
}

/// A bencher that will use batch simple aggregation executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl SimpleAggrBencher for BatchBencher {
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(&self, b: &mut criterion::Bencher, fb: &FixtureBuilder, aggr_expr: &[Expr]) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchSimpleAggregationExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(aggr_expr.to_vec()),
                )
                .unwrap(),
            ) as Box<dyn BatchExecutor>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher> {
        Box::new(Self)
    }
}
