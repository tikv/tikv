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
#[derive(Clone)]
struct Nope;

impl TestHook for Nope {
    fn setup(&mut self) {}
    fn teardown(&mut self) {}
}

struct CaseLifeWatcher<H: TestHook> {
    name: String,
    hook: H,
}

impl<H: TestHook + Send + 'static> CaseLifeWatcher<H> {
    fn new(name: String, mut hook: H) -> CaseLifeWatcher<H> {
        debug!("case start"; "name" => &name);
        hook.setup();
        CaseLifeWatcher { name, hook }
    }
}

impl<H: TestHook> Drop for CaseLifeWatcher<H> {
    fn drop(&mut self) {
        self.hook.teardown();
        debug!("case end"; "name" => &self.name);
    }
}

/// Connects std tests and custom test framework.
pub fn run_test_with_hook(cases: &[&TestDescAndFn], hook: impl TestHook + Send + Clone + 'static) {
    crate::setup_for_ci();
    let cases: Vec<_> = cases
        .iter()
        .map(|case| {
            let name = case.desc.name.as_slice().to_owned();
            let h = hook.clone();
            let f = match case.testfn {
                TestFn::StaticTestFn(f) => TestFn::DynTestFn(Box::new(move || {
                    let _watcher = CaseLifeWatcher::new(name, h);
                    f();
                })),
                TestFn::StaticBenchFn(f) => TestFn::DynTestFn(Box::new(move || {
                    let _watcher = CaseLifeWatcher::new(name, h);
                    bench::run_once(move |b| f(b));
                })),
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

#[derive(Clone)]
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
