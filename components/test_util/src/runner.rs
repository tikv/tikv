// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Useful functions for custom test framework.
//!
//! See https://doc.rust-lang.org/unstable-book/language-features/custom-test-frameworks.html.

use std::{cell::RefCell, env};

use crate::test::*;

/// A runner function for running general tests.
pub fn run_tests(cases: &[&TestDescAndFn]) {
    run_test_with_hook(cases, Nope)
}

/// Hooks for customize tests procedure.
pub trait TestHook {
    /// Called before running every case.
    fn setup(&mut self);
    /// Called after every case.
    fn teardown(&mut self);
}

/// A special TestHook that does nothing.
#[derive(Clone, Copy)]
struct Nope;

impl TestHook for Nope {
    fn setup(&mut self) {}
    fn teardown(&mut self) {}
}

struct CaseLifeWatcher<H: TestHook> {
    hook: H,
}

impl<H: TestHook + Send + 'static> CaseLifeWatcher<H> {
    fn new(mut hook: H) -> CaseLifeWatcher<H> {
        hook.setup();
        CaseLifeWatcher { hook }
    }
}

impl<H: TestHook> Drop for CaseLifeWatcher<H> {
    fn drop(&mut self) {
        self.hook.teardown();
    }
}

/// Connects std tests and custom test framework.
pub fn run_test_with_hook(
    cases: &[&TestDescAndFn],
    hook: impl TestHook + Send + Sync + Copy + 'static,
) {
    crate::setup_for_ci();
    let bench_as_test = std::env::var("TIKV_FORCE_BENCH_AS_TEST").is_ok();
    let cases: Vec<_> = cases
        .iter()
        .map(|case| {
            let h = hook.clone();
            let f = match case.testfn {
                TestFn::StaticTestFn(f) => TestFn::DynTestFn(Box::new(move || {
                    let _watcher = CaseLifeWatcher::new(h);
                    f();
                })),
                TestFn::StaticBenchFn(f) if bench_as_test => {
                    TestFn::DynTestFn(Box::new(move || {
                        let _watcher = CaseLifeWatcher::new(h);
                        bench::run_once(move |b| f(b));
                    }))
                }
                TestFn::StaticBenchFn(f) if !bench_as_test => {
                    TestFn::DynBenchFn(Box::new(move |b| {
                        let _watcher = CaseLifeWatcher::new(h);
                        f(b);
                    }))
                }
                ref f => panic!("unexpected testfn {:?}", f),
            };
            TestDescAndFn {
                desc: case.desc.clone(),
                testfn: f,
            }
        })
        .collect();
    let args = env::args().collect::<Vec<_>>();
    test_main(&args, cases, None)
}

thread_local!(static FS: RefCell<Option<fail::FailScenario<'static>>> = RefCell::new(None));

#[derive(Clone, Copy)]
struct FailpointHook;

impl TestHook for FailpointHook {
    fn setup(&mut self) {
        FS.with(|s| {
            s.borrow_mut().take();
            *s.borrow_mut() = Some(fail::FailScenario::setup());
        })
    }

    fn teardown(&mut self) {
        FS.with(|s| {
            s.borrow_mut().take();
        })
    }
}

/// During panic, due to drop order, failpoints will not be cleared before tests exit.
/// If tests wait for a sleep failpoint, the whole tests will hang. So we need a method
/// to clear failpoints explicitly besides teardown.
pub fn clear_failpoints() {
    FS.with(|s| s.borrow_mut().take());
}

/// A runner function for running failpoint tests.
pub fn run_failpoint_tests(cases: &[&TestDescAndFn]) {
    run_test_with_hook(cases, FailpointHook)
}
