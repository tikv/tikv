// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc};

use api_version::ApiV1;
use criterion::black_box;
use futures::executor::block_on;
use kvproto::coprocessor::KeyRange;
use test_coprocessor::*;
use tidb_query_datatype::expr::EvalConfig;
use tidb_query_executors::{interface::*, BatchIndexScanExecutor};
use tikv::{
    coprocessor::{dag::TikvStorage, RequestHandler},
    storage::{RocksEngine, Statistics, Store as TxnStore},
};
use tipb::ColumnInfo;

use crate::util::{executor_descriptor::index_scan, scan_bencher};

pub type IndexScanParam = bool;

pub struct BatchIndexScanExecutorBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorBuilder for BatchIndexScanExecutorBuilder<T> {
    type T = T;
    type E = Box<dyn BatchExecutor<StorageStats = Statistics>>;
    type P = IndexScanParam;

    fn build(
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        unique: bool,
    ) -> Self::E {
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            black_box(TikvStorage::new(
                ToTxnStore::<Self::T>::to_store(store),
                false,
            )),
            black_box(Arc::new(EvalConfig::default())),
            black_box(columns.to_vec()),
            black_box(ranges.to_vec()),
            black_box(0),
            black_box(false),
            black_box(unique),
            black_box(false),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        block_on(executor.next_batch(1));
        Box::new(executor) as Box<dyn BatchExecutor<StorageStats = Statistics>>
    }
}

pub struct IndexScanExecutorDagBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorDagHandlerBuilder
    for IndexScanExecutorDagBuilder<T>
{
    type T = T;
    type P = IndexScanParam;

    fn build(
        _batch: bool,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        unique: bool,
    ) -> Box<dyn RequestHandler> {
        let exec = index_scan(columns, unique);
        crate::util::build_dag_handler::<T>(&[exec], ranges, store)
    }
}

pub type BatchIndexScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchIndexScanExecutorBuilder<T>>;
pub type IndexScanDagBencher<T> = scan_bencher::ScanDagBencher<IndexScanExecutorDagBuilder<T>>;
