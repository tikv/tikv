// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, measurement::Measurement};
use futures::executor::block_on;
use tidb_query_executors::interface::*;
use tikv::coprocessor::RequestHandler;

pub trait Bencher {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<'_, M>)
    where
        M: Measurement;
}

/// Invoke 1 next_batch(1024) for a batch executor.
pub struct BatchNext1024Bencher<E: BatchExecutor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: BatchExecutor, F: FnMut() -> E> BatchNext1024Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: BatchExecutor, F: FnMut() -> E> Bencher for BatchNext1024Bencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<'_, M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::start("./BatchNext1024Bencher.profile");
                let iter_times = black_box(1024);
                let r = black_box(executor.next_batch(iter_times));
                r.is_drained.unwrap();
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke next_batch(1024) for a batch executor until drained.
pub struct BatchNextAllBencher<E: BatchExecutor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: BatchExecutor, F: FnMut() -> E> BatchNextAllBencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: BatchExecutor, F: FnMut() -> E> Bencher for BatchNextAllBencher<E, F> {
    fn bench<M>(&mut self, b: &mut criterion::Bencher<'_, M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                profiler::start("./BatchNextAllBencher.profile");
                loop {
                    let r = executor.next_batch(1024);
                    black_box(&r);
                    if r.is_drained.unwrap() {
                        break;
                    }
                }
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
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
    fn bench<M>(&mut self, b: &mut criterion::Bencher<'_, M>)
    where
        M: Measurement,
    {
        b.iter_batched_ref(
            &mut self.handler_builder,
            |handler| {
                profiler::start("./DAGHandleBencher.profile");
                black_box(block_on(handler.handle_request()).unwrap());
                profiler::stop();
            },
            criterion::BatchSize::SmallInput,
        );
    }
}
