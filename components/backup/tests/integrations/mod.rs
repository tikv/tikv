// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod test_backup;

pub fn init() {
    use std::sync::*;
    static INIT: Once = Once::new();
    INIT.call_once(test_util::setup_for_ci);
}
