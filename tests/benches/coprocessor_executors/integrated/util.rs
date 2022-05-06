// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc};

use criterion::{black_box, measurement::Measurement};
use kvproto::coprocessor::KeyRange;
use test_coprocessor::*;
use tidb_query_datatype::expr::EvalConfig;
use tikv::{
    coprocessor::dag::TiKvStorage,
    storage::{RocksEngine, Store as TxnStore},
};
use tipb::Executor as PbExecutor;

use crate::util::{bencher::Bencher, store::StoreDescriber};

pub trait IntegratedBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> String;

    fn bench(
        &self,
        b: &mut criterion::Bencher<'_, M>,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    );

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>>;
}

impl<M> Clone for Box<dyn IntegratedBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
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

impl<T, M> IntegratedBencher<M> for BatchBencher<T>
where
    T: TxnStore + 'static,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/batch", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<'_, M>,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            tidb_query_executors::runner::build_executors(
                black_box(executors.to_vec()),
                black_box(TiKvStorage::new(ToTxnStore::<T>::to_store(store), false)),
                black_box(ranges.to_vec()),
                black_box(Arc::new(EvalConfig::default())),
                black_box(false),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>> {
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

impl<T, M> IntegratedBencher<M> for DAGBencher<T>
where
    T: TxnStore + 'static,
    M: Measurement,
{
    fn name(&self) -> String {
        let tag = if self.batch { "batch" } else { "normal" };
        format!("{}/{}/with_dag", <T as StoreDescriber>::name(), tag)
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<'_, M>,
        executors: &[PbExecutor],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            crate::util::build_dag_handler::<T>(executors, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>> {
        Box::new(Self::new(self.batch))
    }
}
