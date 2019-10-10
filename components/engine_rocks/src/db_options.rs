// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::DBOptions;

pub struct RocksDBOptions;

impl DBOptions for RocksDBOptions {
    fn get_max_background_jobs(&self) -> i32 {
        panic!()
    }
}
