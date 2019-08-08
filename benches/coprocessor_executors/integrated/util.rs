// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use kvproto::coprocessor::KeyRange;
use tipb::Executor as PbExecutor;

use test_coprocessor::*;
use tidb_query::execute_stats::ExecSummaryCollectorDisabled;
use tidb_query::expr::EvalConfig;
use tikv::coprocessor::dag::TiKVStorage;
use tikv::storage::{RocksEngine, Store as TxnStore};

use crate::util::bencher::Bencher;
use crate::util::store::StoreDescriber;

pub trait IntegratedBencher {
    fn name(&self) -> String;

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    );

    fn box_clone(&self) -> Box<dyn IntegratedBencher>;
}

impl Clone for Box<dyn IntegratedBencher> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal executor to execute the given request.
pub struct NormalBencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> NormalBencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> IntegratedBencher for NormalBencher<T> {
    fn name(&self) -> String {
        format!("{}/normal", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            tidb_query::executor::runner::build_executors::<_, ExecSummaryCollectorDisabled>(
                black_box(executors.to_vec()),
                black_box(TiKVStorage::from(ToTxnStore::<T>::to_store(store))),
                black_box(ranges.to_vec()),
                black_box(Arc::new(EvalConfig::default())),
                black_box(false),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher> {
        Box::new(Self::new())
    }
}

/// A bencher that will use batch executor to execute the given request.
pub struct BatchBencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> BatchBencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> IntegratedBencher for BatchBencher<T> {
    fn name(&self) -> String {
        format!("{}/batch", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            tidb_query::batch::runner::build_executors::<_, ExecSummaryCollectorDisabled>(
                black_box(executors.to_vec()),
                black_box(TiKVStorage::from(ToTxnStore::<T>::to_store(store))),
                black_box(ranges.to_vec()),
                black_box(Arc::new(EvalConfig::default())),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher> {
        Box::new(Self::new())
    }
}

pub struct DAGBencher<T: TxnStore + 'static> {
    pub batch: bool,
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> DAGBencher<T> {
    pub fn new(batch: bool) -> Self {
        Self {
            batch,
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> IntegratedBencher for DAGBencher<T> {
    fn name(&self) -> String {
        let tag = if self.batch { "batch" } else { "normal" };
        format!("{}/{}/with_dag", <T as StoreDescriber>::name(), tag)
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            crate::util::build_dag_handler::<T>(executors, ranges, store, self.batch)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher> {
        Box::new(Self::new(self.batch))
    }
}
