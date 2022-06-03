// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};
use crate::*;

// FIXME: Revisit the remaining types and methods on KvEngine. Some of these are
// here for lack of somewhere better to put them at the time of writing.
// Consider moving everything into other traits and making KvEngine essentially
// a trait typedef.

/// A TiKV key-value store
pub trait KvEngine:
    Peekable
    + SyncMutable
    + Iterable
    + WriteBatchExt
    + DBOptionsExt
    + CFNamesExt
    + CFOptionsExt
    + ImportExt
    + SstExt
    + CompactExt
    + RangePropertiesExt
    + MvccPropertiesExt
    + TtlPropertiesExt
    + TablePropertiesExt
    + PerfContextExt
    + MiscExt
    + Send
    + Sync
    + Clone
    + Debug
    + Unpin
    + 'static
{
    /// A consistent read-only snapshot of the database
    type Snapshot: Snapshot;

    /// Create a snapshot
    fn snapshot(&self) -> Self::Snapshot;

    /// Syncs any writes to disk
    fn sync(&self) -> Result<()>;

    /// Flush metrics to prometheus
    ///
    /// `instance` is the label of the metric to flush.
    fn flush_metrics(&self, _instance: &str) {}

    /// Reset internal statistics
    fn reset_statistics(&self) {}

    /// Cast to a concrete engine type
    ///
    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}

/// A factory trait to create new engine.
///
// It should be named as `EngineFactory` for consistency, but we are about to rename
// engine to tablet, so always use tablet for new traits/types.
pub trait TabletFactory<EK> {
    fn loop_tablet_cache(&self, _f: Box<dyn FnMut(u64, u64, &EK) + '_>) {}
    fn destroy_tablet(&self, _id: u64, _suffix: u64) -> crate::Result<()> {
        Ok(())
    }
    fn create_tablet(&self, id: u64, suffix: u64) -> Result<EK>;
    fn open_tablet(&self, id: u64, suffix: u64) -> Result<EK> {
        self.open_tablet_raw(&self.tablet_path(id, suffix), false)
    }
    fn open_tablet_cache(&self, id: u64, suffix: u64) -> Result<EK> {
        self.open_tablet_raw(&self.tablet_path(id, suffix), false)
    }
    fn open_tablet_cache_any(&self, id: u64) -> Result<EK> {
        self.open_tablet_raw(&self.tablet_path(id, 0), false)
    }
    fn open_tablet_raw(&self, path: &Path, readonly: bool) -> Result<EK>;
    fn create_root_db(&self) -> Result<EK>;
    #[inline]
    fn exists(&self, id: u64, suffix: u64) -> bool {
        self.exists_raw(&self.tablet_path(id, suffix))
    }
    fn exists_raw(&self, path: &Path) -> bool;
    fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf;
    fn tablets_path(&self) -> PathBuf;
    fn clone(&self) -> Box<dyn TabletFactory<EK> + Send>;
    fn load_tablet(&self, _path: &Path, id: u64, suffix: u64) -> Result<EK> {
        self.open_tablet(id, suffix)
    }
    fn mark_tombstone(&self, _region_id: u64, _suffix: u64) {}
    fn is_tombstoned(&self, _region_id: u64, _suffix: u64) -> bool {
        false
    }
}

pub struct DummyFactory<EK>
where
    EK: KvEngine,
{
    pub engine: Option<EK>,
    pub root_path: String,
}

impl<EK> TabletFactory<EK> for DummyFactory<EK>
where
    EK: KvEngine,
{
    fn create_tablet(&self, _id: u64, _suffix: u64) -> Result<EK> {
        return Ok(self.engine.as_ref().unwrap().clone());
    }
    fn open_tablet_raw(&self, _path: &Path, _readonly: bool) -> Result<EK> {
        return Ok(self.engine.as_ref().unwrap().clone());
    }
    fn create_root_db(&self) -> Result<EK> {
        return Ok(self.engine.as_ref().unwrap().clone());
    }
    fn exists_raw(&self, _path: &Path) -> bool {
        return true;
    }
    fn tablet_path(&self, _id: u64, _suffix: u64) -> PathBuf {
        return PathBuf::from(&self.root_path);
    }
    fn tablets_path(&self) -> PathBuf {
        return PathBuf::from(&self.root_path);
    }

    fn clone(&self) -> Box<dyn TabletFactory<EK> + Send> {
        if self.engine.is_none() {
            return Box::<DummyFactory<EK>>::new(DummyFactory {
                engine: None,
                root_path: self.root_path.clone(),
            });
        }
        Box::<DummyFactory<EK>>::new(DummyFactory {
            engine: Some(self.engine.as_ref().unwrap().clone()),
            root_path: self.root_path.clone(),
        })
    }
}

impl<EK> DummyFactory<EK>
where
    EK: KvEngine,
{
    pub fn new() -> DummyFactory<EK> {
        DummyFactory {
            engine: None,
            root_path: "/dummy_root".to_string(),
        }
    }
}
