// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::*;

pub trait KvEngine:
    Peekable
    + Mutable
    + Iterable
    + DBOptionsExt
    + CFHandleExt
    + ImportExt
    + SstExt
    + TablePropertiesExt
    + Send
    + Sync
    + Clone
    + Debug
    + 'static
// FIXME: This higher-ranked trait bound needs to be propagated all over the place. It's really ugly and anti-abstraction. A solution that didn't require this would be nice.
where for <'a> &'a Self::TablePropertiesCollectionView: IntoIterator<Item = (Self::TablePropertiesStringRef, Self::TablePropertiesRef), IntoIter = Self::TablePropertiesCollectionIter>,
{
    type Snapshot: Snapshot<Self>;
    type WriteBatch: WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::WriteBatch) -> Result<()>;
    fn write(&self, wb: &Self::WriteBatch) -> Result<()> {
        self.write_opt(&WriteOptions::default(), wb)
    }
    fn write_batch(&self) -> Self::WriteBatch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch;
    fn snapshot(&self) -> Self::Snapshot;
    fn sync(&self) -> Result<()>;

    fn cf_names(&self) -> Vec<&str>;

    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}
