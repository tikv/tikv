// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
mod test_cdc;

use fail;
use panic_hook;

fn setup_fail<'a>() -> fail::FailScenario<'a> {
    crate::init();
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
