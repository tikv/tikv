// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "100"]

#[macro_use(slog_debug)]
extern crate slog;
#[macro_use]
extern crate slog_global;

mod cases;

fn setup<'a>() -> fail::FailScenario<'a> {
    fail::FailScenario::setup()
}

// The prefix "_" here is to guarantee running this case first.
#[test]
fn _ci_setup() {
    test_util::setup_for_ci();
}

#[test]
fn test_setup() {
    let _ = std::thread::spawn(move || {
        panic_hook::mute();
        let _g = setup();
        panic!("Poison!");
    })
    .join();

    let _g = setup();
}
