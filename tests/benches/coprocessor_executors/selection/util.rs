// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::{black_box, measurement::Measurement};
use tidb_query_datatype::expr::EvalConfig;
use tidb_query_executors::{interface::BatchExecutor, BatchSelectionExecutor};
use tikv::storage::Statistics;
use tipb::Expr;

use crate::util::{bencher::Bencher, FixtureBuilder};

pub trait SelectionBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> &'static str;

    fn bench(&self, b: &mut criterion::Bencher<'_, M>, fb: &FixtureBuilder, exprs: &[Expr]);

    fn box_clone(&self) -> Box<dyn SelectionBencher<M>>;
}

impl<M> Clone for Box<dyn SelectionBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use batch selection aggregation executor to bench the giving expressions.
pub struct BatchBencher;

impl<M> SelectionBencher<M> for BatchBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(&self, b: &mut criterion::Bencher<'_, M>, fb: &FixtureBuilder, exprs: &[Expr]) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchSelectionExecutor::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(exprs.to_vec()),
                )
                .unwrap(),
            ) as Box<dyn BatchExecutor<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SelectionBencher<M>> {
        Box::new(Self)
    }
}
