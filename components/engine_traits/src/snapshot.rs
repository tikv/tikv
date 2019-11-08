// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::peekable::Peekable;
use std::fmt::Debug;

pub trait Snapshot: 'static + Peekable + Send + Sync + Debug {
    fn cf_names(&self) -> Vec<&str>;
}
