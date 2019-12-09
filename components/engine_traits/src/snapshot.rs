// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::KvEngine;
use crate::iterable::Iterable;
use crate::peekable::Peekable;
use std::fmt::Debug;
use std::ops::Deref;

pub trait Snapshot<E>
where
    Self: 'static + Peekable + Iterable + Send + Sync + Sized + Debug,
    E: KvEngine,
for <'a> &'a E::TablePropertiesCollectionView: IntoIterator<Item = (E::TablePropertiesStringRef, E::TablePropertiesRef), IntoIter = E::TablePropertiesCollectionIter>,
{
    type SyncSnapshot: SyncSnapshot<Self>;

    fn cf_names(&self) -> Vec<&str>;

    fn into_sync(self) -> Self::SyncSnapshot;

    fn get_db(&self) -> &E;
}

pub trait SyncSnapshot<T>
where
    Self: Clone + Send + Sync + Sized + Debug + Deref<Target = T>,
{
}
