// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod bencher;
pub mod executor_descriptor;
pub mod fixture_executor;
pub mod store;

use criterion::black_box;

use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::Executor as PbExecutor;

use test_coprocessor::*;
use tikv::coprocessor::RequestHandler;
use tikv::storage::{RocksEngine, Store as TxnStore};

/// Gets the value of `TIKV_BENCH_LEVEL`. The larger value it is, the more comprehensive benchmarks
/// will be.
pub fn bench_level() -> usize {
    if let Ok(s) = std::env::var("TIKV_BENCH_LEVEL") {
        s.parse::<usize>().unwrap()
    } else {
        0
    }
}

/// A simple helper function to build the DAG handler.
pub fn build_dag_handler<TargetTxnStore: TxnStore + 'static>(
    executors: &[PbExecutor],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
    enable_batch: bool,
) -> Box<dyn RequestHandler> {
    use tikv::coprocessor::dag::builder::DAGBuilder;
    use tikv::coprocessor::Deadline;
    use tipb::select::DAGRequest;

    let mut dag = DAGRequest::new();
    dag.set_executors(RepeatedField::from_vec(executors.to_vec()));

    DAGBuilder::build(
        black_box(dag),
        black_box(ranges.to_vec()),
        black_box(ToTxnStore::<TargetTxnStore>::to_store(store)),
        Deadline::from_now("", std::time::Duration::from_secs(10)),
        64,
        false,
        enable_batch,
    )
    .unwrap()
}
