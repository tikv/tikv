// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use kvproto::coprocessor::KeyRange;
use tipb::ColumnInfo;
use tipb::TableScan;

use test_coprocessor::*;
use tidb_query::batch::executors::BatchTableScanExecutor;
use tidb_query::batch::interface::*;
use tidb_query::executor::Executor;
use tidb_query::executor::TableScanExecutor;
use tidb_query::expr::EvalConfig;
use tikv::coprocessor::dag::TiKVStorage;
use tikv::coprocessor::RequestHandler;
use tikv::storage::{RocksEngine, Statistics, Store as TxnStore};

use crate::util::executor_descriptor::table_scan;
use crate::util::scan_bencher;

pub type TableScanParam = ();

pub struct NormalTableScanExecutorBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorBuilder
    for NormalTableScanExecutorBuilder<T>
{
    type T = T;
    type E = Box<dyn Executor<StorageStats = Statistics>>;
    type P = TableScanParam;

    fn build(
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        _: (),
    ) -> Self::E {
        let mut req = TableScan::default();
        req.set_columns(columns.into());

        let mut executor = TableScanExecutor::table_scan(
            black_box(req),
            black_box(ranges.to_vec()),
            black_box(TiKVStorage::from(ToTxnStore::<Self::T>::to_store(store))),
            black_box(false),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next().unwrap().unwrap();
        Box::new(executor) as Box<dyn Executor<StorageStats = Statistics>>
    }
}

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
            black_box(TiKVStorage::from(ToTxnStore::<Self::T>::to_store(store))),
            black_box(Arc::new(EvalConfig::default())),
            black_box(columns.to_vec()),
            black_box(ranges.to_vec()),
            black_box(false),
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
        batch: bool,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        _: (),
    ) -> Box<dyn RequestHandler> {
        let exec = table_scan(columns);
        crate::util::build_dag_handler::<T>(&[exec], ranges, store, batch)
    }
}

pub type NormalTableScanNext1Bencher<T> =
    scan_bencher::NormalScanNext1Bencher<NormalTableScanExecutorBuilder<T>>;
pub type NormalTableScanNext1024Bencher<T> =
    scan_bencher::NormalScanNext1024Bencher<NormalTableScanExecutorBuilder<T>>;
pub type BatchTableScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchTableScanExecutorBuilder<T>>;
pub type TableScanDAGBencher<T> = scan_bencher::ScanDAGBencher<TableScanExecutorDAGBuilder<T>>;
