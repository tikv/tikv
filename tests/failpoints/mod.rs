// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "100"]

#[macro_use(slog_debug)]
extern crate slog;
#[macro_use]
extern crate slog_global;

mod cases;

use std::sync::Once;

static INIT: Once = Once::new();

fn setup<'a>() -> fail::FailScenario<'a> {
    INIT.call_once(test_util::setup_for_ci);
    fail::FailScenario::setup()
}

#[test]
fn test_setup() {
    let _ = std::thread::spawn(move || {
        let _ = setup();
        panic_hook::mute();
        let _g = setup();
        panic!("Poison!");
    })
    .join();

    let _g = setup();
}
