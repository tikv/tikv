// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! __FUZZ_GENERATE_COMMENT__

#![no_main]

#[macro_use]
extern crate libfuzzer_sys;
extern crate fuzz_targets;

use fuzz_targets::__FUZZ_CLI_TARGET__ as fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = fuzz_target(data);
});
