// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::*;

// FIXME: Revisit the remaining types and methods on KvEngine. Some of these are
// here for lack of somewhere better to put them at the time of writing.
// Consider moving everything into other traits and making KvEngine essentially
// a trait typedef.

pub trait KvEngine:
      Peekable
    + Mutable
    + Iterable
    + WriteBatchExt
    + DBOptionsExt
    + CFHandleExt
    + ImportExt
    + SstExt
    + TablePropertiesExt
    + MiscExt
    + Send
    + Sync
    + Clone
    + Debug
    + 'static
{
    type Snapshot: Snapshot<Self>;

    fn snapshot(&self) -> Self::Snapshot;
    fn sync(&self) -> Result<()>;

    fn cf_names(&self) -> Vec<&str>;

    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}
