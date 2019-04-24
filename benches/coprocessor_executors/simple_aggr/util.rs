// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;

use tipb::executor::Aggregation;
use tipb::expression::Expr;

use tikv::coprocessor::dag::batch::executors::BatchSimpleAggregationExecutor;
use tikv::coprocessor::dag::batch::statistics::*;
use tikv::coprocessor::dag::executor::StreamAggExecutor;
use tikv::coprocessor::dag::expr::EvalConfig;

use crate::util::bencher::Bencher;

pub trait SimpleAggrBencher {
    fn name(&self) -> &'static str;

    fn bench(&self, b: &mut criterion::Bencher, aggr_expr: &Expr, src_rows: usize);

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

    fn bench(&self, b: &mut criterion::Bencher, aggr_expr: &Expr, src_rows: usize) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let mut meta = Aggregation::new();
            meta.mut_agg_func().push(aggr_expr.clone());
            let src = crate::util::fixture_executor::EncodedFixtureNormalExecutor::new(src_rows);
            StreamAggExecutor::new(
                black_box(Arc::new(EvalConfig::default())),
                black_box(Box::new(src)),
                black_box(meta),
            )
            .unwrap()
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

    fn bench(&self, b: &mut criterion::Bencher, aggr_expr: &Expr, src_rows: usize) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = crate::util::fixture_executor::EncodedFixtureBatchExecutor::new(src_rows);
            BatchSimpleAggregationExecutor::new(
                ExecSummaryCollectorDisabled,
                black_box(Arc::new(EvalConfig::default())),
                black_box(Box::new(src)),
                black_box(vec![aggr_expr.clone()]),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SimpleAggrBencher> {
        Box::new(Self)
    }
}
