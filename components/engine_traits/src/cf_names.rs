// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub trait CFNamesExt {
    fn cf_names(&self) -> Vec<&str>;
}
