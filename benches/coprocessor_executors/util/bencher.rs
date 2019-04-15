// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::black_box;

use tikv::coprocessor::dag::batch::interface::*;
use tikv::coprocessor::dag::executor::Executor;
use tikv::coprocessor::RequestHandler;

pub trait Bencher {
    fn bench(&mut self, b: &mut criterion::Bencher);
}

/// Invoke 1 next() for a normal executor.
pub struct NormalNext1Bencher<E: Executor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: Executor, F: FnMut() -> E> NormalNext1Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: Executor, F: FnMut() -> E> Bencher for NormalNext1Bencher<E, F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                black_box(executor.next().unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke 1024 next() for a normal executor.
pub struct NormalNext1024Bencher<E: Executor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: Executor, F: FnMut() -> E> NormalNext1024Bencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: Executor, F: FnMut() -> E> Bencher for NormalNext1024Bencher<E, F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                let iter_times = black_box(1024);
                for _ in 0..iter_times {
                    black_box(executor.next().unwrap());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    }
}

/// Invoke next() for a normal executor until drained.
pub struct NormalNextAllBencher<E: Executor, F: FnMut() -> E> {
    executor_builder: F,
}

impl<E: Executor, F: FnMut() -> E> NormalNextAllBencher<E, F> {
    pub fn new(executor_builder: F) -> Self {
        Self { executor_builder }
    }
}

impl<E: Executor, F: FnMut() -> E> Bencher for NormalNextAllBencher<E, F> {
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| loop {
                let r = executor.next().unwrap();
                black_box(&r);
                if r.is_none() {
                    break;
                }
            },
            criterion::BatchSize::SmallInput,
        );
    }
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
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| {
                let iter_times = black_box(1024);
                let r = black_box(executor.next_batch(iter_times));
                r.is_drained.unwrap();
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
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_batched_ref(
            &mut self.executor_builder,
            |executor| loop {
                let r = executor.next_batch(1024);
                black_box(&r);
                if r.is_drained.unwrap() {
                    break;
                }
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
    fn bench(&mut self, b: &mut criterion::Bencher) {
        b.iter_batched_ref(
            &mut self.handler_builder,
            |handler| {
                black_box(handler.handle_request().unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    }
}
