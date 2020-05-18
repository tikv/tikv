// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::iterable::Iterable;
use crate::peekable::Peekable;
use std::fmt::Debug;

pub trait Snapshot
where
    Self: 'static + Peekable + Iterable + Send + Sync + Sized + Debug,
{
    fn cf_names(&self) -> Vec<&str>;
}
