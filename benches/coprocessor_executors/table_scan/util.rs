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

use kvproto::coprocessor::KeyRange;
use tipb::executor::Executor as PbExecutor;
use tipb::executor::{ExecType, TableScan};

use test_coprocessor::*;
use tikv::coprocessor::dag::batch_executor::executors::BatchTableScanExecutor;
use tikv::coprocessor::dag::batch_executor::interface::*;
use tikv::coprocessor::dag::batch_executor::statistics::*;
use tikv::coprocessor::dag::executor::Executor;
use tikv::coprocessor::dag::executor::TableScanExecutor;
use tikv::coprocessor::dag::expr::EvalConfig;
use tikv::storage::{FixtureStore, RocksEngine};

use crate::util::Bencher;

fn create_table_scan_executor(
    req: &TableScan,
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) -> TableScanExecutor<FixtureStore> {
    let mut executor = TableScanExecutor::table_scan(
        req.clone(),
        ranges.to_vec(),
        store.to_fixture_store(),
        false,
    )
    .unwrap();
    // There is a step of building scanner in the first `next()` which cost time,
    // so we next() before hand.
    executor.next().unwrap().unwrap();
    executor
}

fn create_batch_table_scan_executor(
    req: &TableScan,
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) -> BatchTableScanExecutor<ExecSummaryCollectorDisabled, FixtureStore> {
    let mut executor = BatchTableScanExecutor::new(
        ExecSummaryCollectorDisabled,
        store.to_fixture_store(),
        Arc::new(EvalConfig::default()),
        req.get_columns().to_vec(),
        ranges.to_vec(),
        false,
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
        req: &TableScan,
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    );

    fn box_clone(&self) -> Box<dyn TableScanBencher>;
}

pub struct NormalTableScanExecutorNext1Bencher;

impl TableScanBencher for NormalTableScanExecutorNext1Bencher {
    fn name(&self) -> &'static str {
        "normal_next_1"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        req: &TableScan,
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::NormalExecutorNext1Bencher::new(|| {
            create_table_scan_executor(req, ranges, store)
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
        "normal_next_1024"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        req: &TableScan,
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::NormalExecutorNext1024Bencher::new(|| {
            create_table_scan_executor(req, ranges, store)
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
        "batch_next_1024"
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        req: &TableScan,
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::BatchExecutorNext1024Bencher::new(|| {
            create_batch_table_scan_executor(req, ranges, store)
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
            "normal_dag"
        } else {
            "batch_dag"
        }
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        req: &TableScan,
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::DAGHandleBencher::new(|| {
            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(req.clone());
            crate::util::build_dag_handler(&[exec], ranges, store, self.batch)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(self.clone())
    }
}
