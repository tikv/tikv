// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "100"]

#[macro_use(slog_kv, slog_debug, slog_log, slog_record, slog_b, slog_record_static)]
extern crate slog;
#[macro_use]
extern crate slog_global;

mod cases;

fn setup<'a>() -> fail::FailScenario<'a> {
    let guard = fail::FailScenario::setup();
    test_util::setup_for_ci();
    guard
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
