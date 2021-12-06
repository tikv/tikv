// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use kvproto::coprocessor::KeyRange;
use tipb::ColumnInfo;

use test_coprocessor::*;
use tidb_query_datatype::expr::EvalConfig;
use tidb_query_executors::interface::*;
use tidb_query_executors::BatchTableScanExecutor;
use tikv::coprocessor::dag::TiKVStorage;
use tikv::coprocessor::RequestHandler;
use tikv::storage::{RocksEngine, Statistics, Store as TxnStore};

use crate::util::executor_descriptor::table_scan;
use crate::util::scan_bencher;

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
        let mut executor = BatchTableScanExecutor::new(
            black_box(TiKVStorage::new(
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
        executor.next_batch(1);
        Box::new(executor) as Box<dyn BatchExecutor<StorageStats = Statistics>>
    }
}

pub struct TableScanExecutorDAGBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorDAGHandlerBuilder
    for TableScanExecutorDAGBuilder<T>
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
pub type TableScanDAGBencher<T> = scan_bencher::ScanDAGBencher<TableScanExecutorDAGBuilder<T>>;
