// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc};

use api_version::ApiV1;
use criterion::black_box;
use futures::executor::block_on;
use kvproto::coprocessor::KeyRange;
use test_coprocessor::*;
use tidb_query_datatype::expr::EvalConfig;
use tidb_query_executors::{interface::*, BatchTableScanExecutor};
use tikv::{
    coprocessor::{dag::TikvStorage, RequestHandler},
    storage::{RocksEngine, Statistics, Store as TxnStore},
};
use tipb::ColumnInfo;

use crate::util::{executor_descriptor::table_scan, scan_bencher};

pub type TableScanParam = ();

pub struct BatchTableScanExecutorBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorBuilder for BatchTableScanExecutorBuilder<T> {
    type T = T;
    type E = Box<dyn BatchExecutor<StorageStats = Statistics>>;
    type P = TableScanParam;

    fn build(
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        _: (),
    ) -> Self::E {
        let mut executor = BatchTableScanExecutor::<_, ApiV1>::new(
            black_box(TikvStorage::new(
                ToTxnStore::<Self::T>::to_store(store),
                false,
            )),
            black_box(Arc::new(EvalConfig::default())),
            black_box(columns.to_vec()),
            black_box(ranges.to_vec()),
            black_box(vec![]),
            black_box(false),
            black_box(false),
            black_box(vec![]),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        block_on(executor.next_batch(1));
        Box::new(executor) as Box<dyn BatchExecutor<StorageStats = Statistics>>
    }
}

pub struct TableScanExecutorDagBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorDagHandlerBuilder
    for TableScanExecutorDagBuilder<T>
{
    type T = T;
    type P = TableScanParam;

    fn build(
        _batch: bool,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        _: (),
    ) -> Box<dyn RequestHandler> {
        let exec = table_scan(columns);
        crate::util::build_dag_handler::<T>(&[exec], ranges, store)
    }
}

pub type BatchTableScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchTableScanExecutorBuilder<T>>;
pub type TableScanDagBencher<T> = scan_bencher::ScanDagBencher<TableScanExecutorDagBuilder<T>>;
