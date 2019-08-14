// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod bencher;
pub mod executor_descriptor;
pub mod fixture;
pub mod scan_bencher;
pub mod store;

pub use self::fixture::FixtureBuilder;

use criterion::black_box;

use kvproto::coprocessor::KeyRange;
use tipb::Executor as PbExecutor;

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
    use tipb::DagRequest;

    let mut dag = DagRequest::default();
    dag.set_executors(executors.to_vec().into());

    tikv::coprocessor::dag::build_handler(
        black_box(dag),
        black_box(ranges.to_vec()),
        black_box(ToTxnStore::<TargetTxnStore>::to_store(store)),
        tikv_util::deadline::Deadline::from_now(std::time::Duration::from_secs(10)),
        64,
        false,
        enable_batch,
    )
    .unwrap()
}

pub struct BenchCase<I> {
    pub name: &'static str,
    pub f: Box<dyn Fn(&mut criterion::Bencher, &I) + 'static>,
}

impl<I> PartialEq for BenchCase<I> {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(other.name)
    }
}

impl<I> PartialOrd for BenchCase<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(other.name)
    }
}

impl<I> Ord for BenchCase<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(other.name)
    }
}

impl<I> Eq for BenchCase<I> {}

impl<I> BenchCase<I> {
    pub fn new(name: &'static str, f: impl Fn(&mut criterion::Bencher, &I) + 'static) -> Self {
        Self {
            name,
            f: Box::new(f),
        }
    }
}
