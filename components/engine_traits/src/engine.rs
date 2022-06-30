// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    io::Write,
    path::{Path, PathBuf},
    str,
    vec::Vec,
};

use tikv_util::error;

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

/// TabletAccessor is the trait to access all the tablets with provided accessor
///
/// For single rocksdb instance, it essentially accesses the global kvdb with the accessor
/// For multi rocksdb instances, it accesses all the tablets with the accessor
pub trait TabletAccessor<EK> {
    /// Loop visit all opened tablets by the specified function.
    fn for_each_opened_tablet(&self, _f: &mut (dyn FnMut(u64, u64, &EK)));

    /// return true if it's single engine;
    /// return false if it's a multi-tablet factory;
    fn is_single_engine(&self) -> bool;
}

/// max error count to log
const MAX_ERROR_COUNT: u32 = 5;

/// TabletErrorCollector is the facility struct to handle errors when using TabletAccessor::for_each_opened_tablet
///
/// It will choose the last failed result as the final result, meanwhile logging errors up to MAX_ERROR_COUNT.
pub struct TabletErrorCollector {
    errors: Vec<u8>,
    max_error_count: u32,
    error_count: u32,
    result: std::result::Result<(), Box<dyn std::error::Error>>,
}

impl TabletErrorCollector {
    pub fn new() -> Self {
        Self {
            errors: vec![],
            max_error_count: MAX_ERROR_COUNT,
            error_count: 0,
            result: Ok(()),
        }
    }

    pub fn add_result(&mut self, region_id: u64, suffix: u64, result: Result<()>) {
        if result.is_ok() {
            return;
        }
        self.result = Err(Box::from(result.err().unwrap()));
        self.error_count += 1;
        if self.error_count > self.max_error_count {
            return;
        }
        writeln!(
            &mut self.errors,
            "Tablet {}_{} encountered error: {:?}.",
            region_id, suffix, self.result
        )
        .unwrap();
    }

    fn flush_error(&self) {
        if self.error_count > 0 {
            error!(
                "Total count {}. Sample errors: {}",
                self.error_count,
                str::from_utf8(&self.errors).unwrap()
            );
        }
    }

    pub fn take_result(&mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        std::mem::replace(&mut self.result, Ok(()))
    }

    pub fn get_error_count(&self) -> u32 {
        self.error_count
    }
}

impl Default for TabletErrorCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TabletErrorCollector {
    fn drop(&mut self) {
        self.flush_error()
    }
}

/// A factory trait to create new engine.
///
// It should be named as `EngineFactory` for consistency, but we are about to rename
// engine to tablet, so always use tablet for new traits/types.
pub trait TabletFactory<EK>: TabletAccessor<EK> {
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
    EK: Clone + Send + 'static,
{
    pub engine: Option<EK>,
    pub root_path: String,
}

impl<EK> TabletFactory<EK> for DummyFactory<EK>
where
    EK: Clone + Send + 'static,
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
}
impl<EK> TabletAccessor<EK> for DummyFactory<EK>
where
    EK: Clone + Send + 'static,
{
    fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &EK)) {
        if let Some(engine) = &self.engine {
            f(0, 0, engine);
        }
    }

    fn is_single_engine(&self) -> bool {
        true
    }
}

impl<EK> DummyFactory<EK>
where
    EK: Clone + Send + 'static,
{
    pub fn new(engine: Option<EK>, root_path: String) -> DummyFactory<EK> {
        DummyFactory { engine, root_path }
    }
}

impl<EK: Clone + Send + 'static> Default for DummyFactory<EK> {
    fn default() -> Self {
        Self::new(None, "/tmp".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tablet_error_collector_ok() {
        let mut err = TabletErrorCollector::new();
        err.add_result(1, 1, Ok(()));
        assert!(err.take_result().is_ok());
        assert_eq!(err.get_error_count(), 0);
    }

    #[test]
    fn test_tablet_error_collector_err() {
        let mut err = TabletErrorCollector::new();
        err.add_result(1, 1, Ok(()));
        err.add_result(1, 1, Err("this is an error1".to_string().into()));
        err.add_result(1, 1, Err("this is an error2".to_string().into()));
        err.add_result(1, 1, Ok(()));
        let r = err.take_result();
        assert!(r.is_err());
        assert_eq!(err.get_error_count(), 2);
    }
}
