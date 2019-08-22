// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait Peekable {
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn get_value_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8])
        -> Result<Option<Vec<u8>>>;

    fn get_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_value_opt(&ReadOptions::default(), key)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_value_cf_opt(&ReadOptions::default(), cf, key)
    }
}
