// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use criterion::measurement::Measurement;

use kvproto::coprocessor::KeyRange;
use tipb::ColumnInfo;

use test_coprocessor::*;
use tidb_query::batch::interface::*;
use tidb_query::executor::Executor;
use tikv::coprocessor::RequestHandler;
use tikv::storage::{RocksEngine, Store as TxnStore};

use crate::util::bencher::Bencher;
use crate::util::store::StoreDescriber;

pub trait ScanExecutorBuilder: 'static {
    type T: TxnStore + 'static;
    type E;
    type P: Copy + 'static;
    fn build(
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: Self::P,
    ) -> Self::E;
}

pub trait ScanExecutorDAGHandlerBuilder: 'static {
    type T: TxnStore + 'static;
    type P: Copy + 'static;
    fn build(
        batch: bool,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: Self::P,
    ) -> Box<dyn RequestHandler>;
}

/// Benchers shared for table scan and index scan.
pub trait ScanBencher<P, M>: 'static
where
    P: Copy + 'static,
    M: Measurement,
{
    fn name(&self) -> String;

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: P,
    );

    fn box_clone(&self) -> Box<dyn ScanBencher<P, M>>;
}

impl<P, M> Clone for Box<dyn ScanBencher<P, M>>
where
    P: Copy + 'static,
    M: Measurement + 'static,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

pub struct NormalScanNext1Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: Executor,
{
    _phantom: PhantomData<B>,
}

impl<B> NormalScanNext1Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: Executor,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for NormalScanNext1Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: Executor,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/normal/next=1", <B::T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::NormalNext1Bencher::new(|| {
            B::build(columns, ranges, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new())
    }
}

pub struct NormalScanNext1024Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: Executor,
{
    _phantom: PhantomData<B>,
}

impl<B> NormalScanNext1024Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: Executor,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for NormalScanNext1024Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: Executor,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/normal/next=1024", <B::T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::NormalNext1024Bencher::new(|| {
            B::build(columns, ranges, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new())
    }
}

pub struct BatchScanNext1024Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: BatchExecutor,
{
    _phantom: PhantomData<B>,
}

impl<B> BatchScanNext1024Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: BatchExecutor,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for BatchScanNext1024Bencher<B>
where
    B: ScanExecutorBuilder,
    B::E: BatchExecutor,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/batch/next=1024", <B::T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::BatchNext1024Bencher::new(|| {
            B::build(columns, ranges, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new())
    }
}

pub struct ScanDAGBencher<B: ScanExecutorDAGHandlerBuilder> {
    batch: bool,
    display_table_rows: usize,
    _phantom: PhantomData<B>,
}

impl<B: ScanExecutorDAGHandlerBuilder> ScanDAGBencher<B> {
    pub fn new(batch: bool, display_table_rows: usize) -> Self {
        Self {
            batch,
            display_table_rows,
            _phantom: PhantomData,
        }
    }
}

impl<B, M> ScanBencher<B::P, M> for ScanDAGBencher<B>
where
    B: ScanExecutorDAGHandlerBuilder,
    M: Measurement,
{
    fn name(&self) -> String {
        let tag = if self.batch { "batch" } else { "normal" };
        format!(
            "{}/{}/with_dag/rows={}",
            <B::T as StoreDescriber>::name(),
            tag,
            self.display_table_rows
        )
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
        parameters: B::P,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            B::build(self.batch, columns, ranges, store, parameters)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn ScanBencher<B::P, M>> {
        Box::new(Self::new(self.batch, self.display_table_rows))
    }
}
