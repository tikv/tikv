// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub trait DBOptions {
    fn get_max_background_jobs(&self) -> i32;
}
