// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;

use tipb::Expr;

use tidb_query::batch::executors::BatchSelectionExecutor;
use tidb_query::batch::interface::BatchExecutor;
use tidb_query::executor::{Executor, SelectionExecutor};
use tidb_query::expr::EvalConfig;
use tikv::storage::Statistics;

use crate::util::bencher::Bencher;
use crate::util::executor_descriptor::selection;
use crate::util::FixtureBuilder;

pub trait SelectionBencher {
    fn name(&self) -> &'static str;

    fn bench(&self, b: &mut criterion::Bencher, fb: &FixtureBuilder, exprs: &[Expr]);

    fn box_clone(&self) -> Box<dyn SelectionBencher>;
}

impl Clone for Box<dyn SelectionBencher> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal selection executor to bench the giving expressions.
pub struct NormalBencher;

impl SelectionBencher for NormalBencher {
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(&self, b: &mut criterion::Bencher, fb: &FixtureBuilder, exprs: &[Expr]) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let meta = selection(exprs).take_selection();
            let src = fb.clone().build_normal_fixture_executor();
            Box::new(
                SelectionExecutor::new(
                    black_box(meta),
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                )
                .unwrap(),
            ) as Box<dyn Executor<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SelectionBencher> {
        Box::new(Self)
    }
}

/// A bencher that will use batch selection aggregation executor to bench the giving expressions.
pub struct BatchBencher;

impl SelectionBencher for BatchBencher {
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(&self, b: &mut criterion::Bencher, fb: &FixtureBuilder, exprs: &[Expr]) {
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

    fn box_clone(&self) -> Box<dyn SelectionBencher> {
        Box::new(Self)
    }
}
