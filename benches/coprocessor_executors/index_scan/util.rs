// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::IndexScan;
use tipb::schema::ColumnInfo;

use test_coprocessor::*;
use tikv::coprocessor::dag::batch::executors::BatchIndexScanExecutor;
use tikv::coprocessor::dag::batch::interface::*;
use tikv::coprocessor::dag::executor::{Executor, IndexScanExecutor};
use tikv::coprocessor::dag::expr::EvalConfig;
use tikv::coprocessor::RequestHandler;
use tikv::storage::{RocksEngine, Store as TxnStore};

use crate::util::executor_descriptor::index_scan;
use crate::util::scan_bencher;

pub type IndexScanParam = bool;

pub struct NormalIndexScanExecutorBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorBuilder
    for NormalIndexScanExecutorBuilder<T>
{
    type T = T;
    type E = Box<dyn Executor>;
    type P = IndexScanParam;

    fn build(
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        unique: bool,
    ) -> Self::E {
        let mut req = IndexScan::new();
        req.set_columns(RepeatedField::from_slice(columns));

        let mut executor = IndexScanExecutor::index_scan(
            black_box(req),
            black_box(ranges.to_vec()),
            black_box(ToTxnStore::<Self::T>::to_store(store)),
            black_box(unique),
            false,
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next().unwrap().unwrap();
        Box::new(executor) as Box<dyn Executor>
    }
}

pub struct BatchIndexScanExecutorBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorBuilder for BatchIndexScanExecutorBuilder<T> {
    type T = T;
    type E = Box<dyn BatchExecutor>;
    type P = IndexScanParam;

    fn build(
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        unique: bool,
    ) -> Self::E {
        let mut executor = BatchIndexScanExecutor::new(
            black_box(ToTxnStore::<Self::T>::to_store(store)),
            black_box(Arc::new(EvalConfig::default())),
            black_box(columns.to_vec()),
            black_box(ranges.to_vec()),
            black_box(false),
            black_box(unique),
        )
        .unwrap();
        // There is a step of building scanner in the first `next()` which cost time,
        // so we next() before hand.
        executor.next_batch(1);
        Box::new(executor) as Box<dyn BatchExecutor>
    }
}

pub struct IndexScanExecutorDAGBuilder<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> scan_bencher::ScanExecutorDAGHandlerBuilder
    for IndexScanExecutorDAGBuilder<T>
{
    type T = T;
    type P = IndexScanParam;

    fn build(
        batch: bool,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        unique: bool,
    ) -> Box<dyn RequestHandler> {
        let exec = index_scan(columns, unique);
        crate::util::build_dag_handler::<T>(&[exec], ranges, store, batch)
    }
}

pub type NormalIndexScanNext1Bencher<T> =
    scan_bencher::NormalScanNext1Bencher<NormalIndexScanExecutorBuilder<T>>;
pub type NormalIndexScanNext1024Bencher<T> =
    scan_bencher::NormalScanNext1024Bencher<NormalIndexScanExecutorBuilder<T>>;
pub type BatchIndexScanNext1024Bencher<T> =
    scan_bencher::BatchScanNext1024Bencher<BatchIndexScanExecutorBuilder<T>>;
pub type IndexScanDAGBencher<T> = scan_bencher::ScanDAGBencher<IndexScanExecutorDAGBuilder<T>>;
