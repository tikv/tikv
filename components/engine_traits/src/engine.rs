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
    /// Create an tablet by id and suffix. If the tablet exists, it will fail.
    /// The id is likely the region Id, the suffix could be the current raft log index.
    /// They together could specify a unique path for a region's tablet.
    /// The reason to have suffix is that we can keep more than one tablet for a region.
    fn create_tablet(&self, id: u64, suffix: u64) -> Result<EK>;

    /// Open a tablet by id and suffix. If the tablet exists, it will open it.
    /// If the tablet does not exist, it will create it.
    fn open_tablet(&self, id: u64, suffix: u64) -> Result<EK> {
        self.open_tablet_raw(&self.tablet_path(id, suffix), false)
    }

    /// Open a tablet by id and suffix from cache---that means it should already be opened.
    fn open_tablet_cache(&self, id: u64, suffix: u64) -> Option<EK> {
        if let Ok(engine) = self.open_tablet_raw(&self.tablet_path(id, suffix), false) {
            return Some(engine);
        }
        None
    }

    /// Open a tablet by id and any suffix from cache
    fn open_tablet_cache_any(&self, id: u64) -> Option<EK> {
        self.open_tablet_cache(id, 0)
    }

    /// Open tablet by path and readonly flag
    fn open_tablet_raw(&self, path: &Path, readonly: bool) -> Result<EK>;

    /// Create the shared db for v1
    fn create_shared_db(&self) -> Result<EK>;

    /// Destroy the tablet and its data
    fn destroy_tablet(&self, id: u64, suffix: u64) -> crate::Result<()>;

    /// Check if the tablet with specified id/suffix exists
    #[inline]
    fn exists(&self, id: u64, suffix: u64) -> bool {
        self.exists_raw(&self.tablet_path(id, suffix))
    }

    /// Check if the tablet with specified path exists
    fn exists_raw(&self, path: &Path) -> bool;

    /// Get the tablet path by id and suffix
    fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf;

    /// Tablets root path
    fn tablets_path(&self) -> PathBuf;

    /// Clone the tablet factory instance
    /// Here we don't use Clone traint because it will break the trait's object safty
    fn clone(&self) -> Box<dyn TabletFactory<EK> + Send>;

    /// Loop visit all opened tablets cached by the specified function.
    /// Once the tablet is opened/created, it will be cached in a hashmap
    fn loop_tablet_cache(&self, _f: Box<dyn FnMut(u64, u64, &EK) + '_>);

    /// Load the tablet from path for id and suffix--for scenarios such as applying snapshot
    fn load_tablet(&self, _path: &Path, _id: u64, _suffix: u64) -> Result<EK> {
        unimplemented!();
    }

    /// Mark the tablet with specified id and suffix tombostone
    fn mark_tombstone(&self, _id: u64, _suffix: u64) {
        unimplemented!();
    }

    /// Check if the tablet with specified id and suffix tombostone
    fn is_tombstoned(&self, _region_id: u64, _suffix: u64) -> bool {
        unimplemented!();
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
        Ok(self.engine.as_ref().unwrap().clone())
    }
    fn open_tablet_raw(&self, _path: &Path, _readonly: bool) -> Result<EK> {
        Ok(self.engine.as_ref().unwrap().clone())
    }
    fn create_shared_db(&self) -> Result<EK> {
        Ok(self.engine.as_ref().unwrap().clone())
    }
    fn destroy_tablet(&self, _id: u64, _suffix: u64) -> crate::Result<()> {
        Ok(())
    }
    fn exists_raw(&self, _path: &Path) -> bool {
        true
    }
    fn tablet_path(&self, _id: u64, _suffix: u64) -> PathBuf {
        PathBuf::from(&self.root_path)
    }
    fn tablets_path(&self) -> PathBuf {
        PathBuf::from(&self.root_path)
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
    fn loop_tablet_cache(&self, _f: Box<dyn FnMut(u64, u64, &EK) + '_>) {}
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

impl<EK: KvEngine> Default for DummyFactory<EK> {
    fn default() -> Self {
        Self::new()
    }
}
