// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use criterion::black_box;

use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::Executor as PbExecutor;
use tipb::executor::{ExecType, TableScan};
use tipb::schema::ColumnInfo;

use test_coprocessor::*;
use tikv::coprocessor::dag::batch_executor::executors::BatchTableScanExecutor;
use tikv::coprocessor::dag::batch_executor::interface::*;
use tikv::coprocessor::dag::batch_executor::statistics::*;
use tikv::coprocessor::dag::executor::Executor;
use tikv::coprocessor::dag::executor::TableScanExecutor;
use tikv::coprocessor::dag::expr::EvalConfig;
use tikv::storage::{FixtureStore, RocksEngine};

use crate::util::bencher::Bencher;

fn create_table_scan_executor(
    columns: &[ColumnInfo],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) -> TableScanExecutor<FixtureStore> {
    let mut req = TableScan::new();
    req.set_columns(RepeatedField::from_slice(columns));

    let mut executor = TableScanExecutor::table_scan(
        black_box(req),
        black_box(ranges.to_vec()),
        black_box(store.to_fixture_store()),
        false,
    )
    .unwrap();
    // There is a step of building scanner in the first `next()` which cost time,
    // so we next() before hand.
    executor.next().unwrap().unwrap();
    executor
}

fn create_batch_table_scan_executor(
    columns: &[ColumnInfo],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) -> BatchTableScanExecutor<ExecSummaryCollectorDisabled, FixtureStore> {
    let mut executor = BatchTableScanExecutor::new(
        ExecSummaryCollectorDisabled,
        black_box(store.to_fixture_store()),
        black_box(Arc::new(EvalConfig::default())),
        black_box(columns.to_vec()),
        black_box(ranges.to_vec()),
        black_box(false),
    )
    .unwrap();
    // There is a step of building scanner in the first `next()` which cost time,
    // so we next() before hand.
    executor.next_batch(1);
    executor
}

pub trait TableScanBencher {
    fn name(&self) -> &'static str;

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    );

    fn box_clone(&self) -> Box<dyn TableScanBencher>;
}

pub struct NormalTableScanExecutorNext1Bencher;

impl TableScanBencher for NormalTableScanExecutorNext1Bencher {
    fn name(&self) -> &'static str {
        "normal/next=1"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::NormalExecutorNext1Bencher::new(|| {
            create_table_scan_executor(columns, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self)
    }
}

pub struct NormalTableScanExecutorNext1024Bencher;

impl TableScanBencher for NormalTableScanExecutorNext1024Bencher {
    fn name(&self) -> &'static str {
        "normal/next=1024"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::NormalExecutorNext1024Bencher::new(|| {
            create_table_scan_executor(columns, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self)
    }
}

pub struct BatchTableScanExecutorNext1024Bencher;

impl TableScanBencher for BatchTableScanExecutorNext1024Bencher {
    fn name(&self) -> &'static str {
        "batch/next=1024"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::BatchExecutorNext1024Bencher::new(|| {
            create_batch_table_scan_executor(columns, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self)
    }
}

#[derive(Clone)]
pub struct TableScanExecutorDAGBencher {
    pub batch: bool,
}

impl TableScanBencher for TableScanExecutorDAGBencher {
    fn name(&self) -> &'static str {
        if self.batch {
            "normal/with_dag"
        } else {
            "batch/with_dag"
        }
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.mut_tbl_scan()
                .set_columns(RepeatedField::from_slice(columns));
            crate::util::build_dag_handler(&[exec], ranges, store, self.batch)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(self.clone())
    }
}
