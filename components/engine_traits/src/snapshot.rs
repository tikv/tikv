// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::KvEngine;
use crate::iterable::Iterable;
use crate::peekable::Peekable;
use std::fmt::Debug;
use std::ops::Deref;

pub trait Snapshot: 'static + Peekable + Iterable + Send + Sync + Sized + Debug {
    type SyncSnapshot: SyncSnapshot<Self>;
    type KvEngine: KvEngine;

    fn cf_names(&self) -> Vec<&str>;

    fn into_sync(self) -> Self::SyncSnapshot;

    fn get_db(&self) -> &Self::KvEngine;
}

pub trait SyncSnapshot<T>: Clone + Send + Sync + Sized + Deref<Target = T> {
}
