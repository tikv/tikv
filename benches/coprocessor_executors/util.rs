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

use criterion::black_box;
use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::Executor as PbExecutor;

use test_coprocessor::*;
use tikv::coprocessor::dag::batch_executor::interface::BatchExecutor;
use tikv::coprocessor::dag::executor::Executor;
use tikv::coprocessor::RequestHandler;
use tikv::storage::RocksEngine;

pub fn build_dag_handler(
    executors: &[PbExecutor],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
    enable_batch: bool,
) -> Box<dyn RequestHandler> {
    use tikv::coprocessor::dag::DAGRequestHandler;
    use tikv::coprocessor::Deadline;
    use tipb::select::DAGRequest;

    let mut dag = DAGRequest::new();
    dag.set_executors(RepeatedField::from_vec(executors.to_vec()));

    DAGRequestHandler::build(
        dag,
        ranges.to_vec(),
        store.to_fixture_store(),
        Deadline::from_now("", std::time::Duration::from_secs(10)),
        64,
        false,
        enable_batch,
    )
    .unwrap()
}

pub trait Bencher {
    fn bench(&mut self, b: &mut criterion::Bencher);
}

/// Invoke 1 next() of a normal executor.
pub struct NormalExecutorNext1Bencher<E: Executor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: Executor, F: FnMut() -> E> NormalExecutorNext1Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: Executor, F: FnMut() -> E> Bencher for NormalExecutorNext1Bencher<E, F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_with_setup(&mut self.executor_builder, |mut executor| {
            let executor = black_box(&mut executor);
            black_box(executor.next().unwrap());
        });
    }
}

/// Invoke 1024 next() of a normal executor.
pub struct NormalExecutorNext1024Bencher<E: Executor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: Executor, F: FnMut() -> E> NormalExecutorNext1024Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: Executor, F: FnMut() -> E> Bencher for NormalExecutorNext1024Bencher<E, F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_with_setup(&mut self.executor_builder, |mut executor| {
            let executor = black_box(&mut executor);
            let iter_times = black_box(1024);
            for _ in 0..iter_times {
                black_box(executor.next().unwrap());
            }
        });
    }
}

/// Invoke 1 next_batch(1024) for a batch executor.
pub struct BatchExecutorNext1024Bencher<E: BatchExecutor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: BatchExecutor, F: FnMut() -> E> BatchExecutorNext1024Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: BatchExecutor, F: FnMut() -> E> Bencher for BatchExecutorNext1024Bencher<E, F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_with_setup(&mut self.executor_builder, |mut executor| {
            let executor = black_box(&mut executor);
            let iter_times = black_box(1024);
            let r = black_box(executor.next_batch(iter_times));
            r.is_drained.unwrap();
        });
    }
}

/// Invoke handle request for a DAG handler.
pub struct DAGHandleBencher<F: FnMut() -> Box<dyn RequestHandler>> {
    handler_builder: F,
}

impl<F: FnMut() -> Box<dyn RequestHandler>> DAGHandleBencher<F> {
    pub fn new(handler_builder: F) -> Self {
        Self { handler_builder }
    }
}

impl<F: FnMut() -> Box<dyn RequestHandler>> Bencher for DAGHandleBencher<F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_with_setup(&mut self.handler_builder, |mut handler| {
            let handler = black_box(&mut handler);
            black_box(handler.handle_request().unwrap());
        });
    }
}
