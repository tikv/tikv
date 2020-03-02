// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod test_cdc;

use std::sync::*;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}

fn setup_fail<'a>() -> fail::FailScenario<'a> {
    INIT.call_once(test_util::setup_for_ci);
    fail::FailScenario::setup()
}

#[test]
fn test_setup_fail() {
    let _ = std::thread::spawn(move || {
        let _ = setup_fail();
        panic_hook::mute();
        let _g = setup_fail();
        panic!("Poison!");
    })
    .join();

    let _g = setup_fail();
}
