// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod bencher;
pub mod executor_descriptor;
pub mod fixture;
pub mod scan_bencher;
pub mod store;

pub use self::fixture::FixtureBuilder;

use criterion::black_box;
use criterion::measurement::Measurement;

use kvproto::coprocessor::KeyRange;
use tipb::Executor as PbExecutor;

use test_coprocessor::*;
use tikv::coprocessor::RequestHandler;
use tikv::storage::{RocksEngine, Store as TxnStore};

use std::marker::PhantomData;

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
        None,
        tikv_util::deadline::Deadline::from_now(std::time::Duration::from_secs(10)),
        64,
        false,
        enable_batch,
    )
    .unwrap()
}

pub struct InnerBenchCase<I, M, F>
where
    M: Measurement + 'static,
    F: Fn(&mut criterion::Bencher<M>, &I) + Copy + 'static,
{
    pub _phantom_input: PhantomData<I>,
    pub _phantom_measurement: PhantomData<M>,
    pub name: &'static str,
    pub f: F,
}

type BenchFn<M, I> = Box<dyn Fn(&mut criterion::Bencher<M>, &I) + 'static>;

pub trait IBenchCase {
    type M: Measurement + 'static;
    type I;

    fn get_fn(&self) -> BenchFn<Self::M, Self::I>;

    fn get_name(&self) -> &'static str;
}

impl<I, M, F> IBenchCase for InnerBenchCase<I, M, F>
where
    M: Measurement + 'static,
    F: Fn(&mut criterion::Bencher<M>, &I) + Copy + 'static,
{
    type M = M;
    type I = I;

    fn get_fn(&self) -> BenchFn<Self::M, Self::I> {
        Box::new(self.f)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

pub struct BenchCase<I, M>
where
    M: Measurement + 'static,
{
    inner: Box<dyn IBenchCase<I = I, M = M>>,
}

impl<I, M> BenchCase<I, M>
where
    M: Measurement + 'static,
    I: 'static,
{
    pub fn new<F>(name: &'static str, f: F) -> Self
    where
        F: Fn(&mut criterion::Bencher<M>, &I) + Copy + 'static,
    {
        Self {
            inner: Box::new(InnerBenchCase {
                _phantom_input: PhantomData,
                _phantom_measurement: PhantomData,
                name,
                f,
            }),
        }
    }

    pub fn get_name(&self) -> &'static str {
        self.inner.get_name()
    }

    pub fn get_fn(&self) -> BenchFn<M, I> {
        self.inner.get_fn()
    }
}

impl<I, M> PartialEq for BenchCase<I, M>
where
    M: Measurement + 'static,
    I: 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.get_name().eq(other.get_name())
    }
}

impl<I, M> PartialOrd for BenchCase<I, M>
where
    M: Measurement + 'static,
    I: 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get_name().partial_cmp(other.get_name())
    }
}

impl<I, M> Ord for BenchCase<I, M>
where
    M: Measurement + 'static,
    I: 'static,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_name().cmp(other.get_name())
    }
}

impl<I, M> Eq for BenchCase<I, M>
where
    M: Measurement,
    I: 'static,
{
}
