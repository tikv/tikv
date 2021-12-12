// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;
use criterion::measurement::Measurement;

use tipb::Expr;

use tidb_query_datatype::expr::EvalConfig;
use tidb_query_executors::BatchTopNExecutor;

use crate::util::bencher::Bencher;

use crate::util::FixtureBuilder;

pub trait TopNBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> &'static str;

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        fb: &FixtureBuilder,
        order_by_expr: &[Expr],
        order_is_desc: &[bool],
        n: usize,
    );

    fn box_clone(&self) -> Box<dyn TopNBencher<M>>;
}

impl<M> Clone for Box<dyn TopNBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use batch top N executor to bench the giving aggregate
/// expression.
pub struct BatchBencher;

impl<M> TopNBencher<M> for BatchBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        fb: &FixtureBuilder,
        order_by_expr: &[Expr],
        order_is_desc: &[bool],
        n: usize,
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchTopNExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(order_by_expr.to_vec()),
                    black_box(order_is_desc.to_vec()),
                    black_box(n),
                )
                .unwrap(),
            )
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TopNBencher<M>> {
        Box::new(Self)
    }
}
