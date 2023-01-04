// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use collections::HashMap;
use kvproto::metapb::Region;
use tikv_util::box_err;

use crate::{Error, FlushState, Result};

#[derive(Debug)]
struct LatestTablet<EK> {
    data: Mutex<Option<EK>>,
    version: AtomicU64,
}

/// Tablet may change during split, merge and applying snapshot. So we need a
/// shared value to reflect the latest tablet. `CachedTablet` provide cache that
/// can speed up common access.
#[derive(Clone, Debug)]
pub struct CachedTablet<EK> {
    latest: Arc<LatestTablet<EK>>,
    cache: Option<EK>,
    version: u64,
}

impl<EK> CachedTablet<EK> {
    fn release(&mut self) {
        self.cache = None;
        self.version = 0;
    }
}

impl<EK: Clone> CachedTablet<EK> {
    #[inline]
    fn new(data: Option<EK>) -> Self {
        CachedTablet {
            latest: Arc::new(LatestTablet {
                data: Mutex::new(data.clone()),
                version: AtomicU64::new(0),
            }),
            cache: data,
            version: 0,
        }
    }

    pub fn set(&mut self, data: EK) -> Option<EK> {
        self.cache = Some(data.clone());
        let mut latest_data = self.latest.data.lock().unwrap();
        self.version = self.latest.version.fetch_add(1, Ordering::Relaxed) + 1;
        latest_data.replace(data)
    }

    /// Get the tablet from cache without checking if it's up to date.
    #[inline]
    pub fn cache(&self) -> Option<&EK> {
        self.cache.as_ref()
    }

    /// Get the latest tablet.
    #[inline]
    pub fn latest(&mut self) -> Option<&EK> {
        if self.latest.version.load(Ordering::Relaxed) > self.version {
            let latest_data = self.latest.data.lock().unwrap();
            self.version = self.latest.version.load(Ordering::Relaxed);
            self.cache = latest_data.clone();
        }
        self.cache()
    }
}

/// Context to be passed to `TabletFactory`.
#[derive(Clone)]
pub struct TabletContext {
    /// ID of the tablet. It is usually the region ID.
    pub id: u64,
    /// Suffix the tablet. It is usually the index that the tablet starts accept
    /// incremental modification. The reason to have suffix is that we can keep
    /// more than one tablet for a region.
    pub suffix: Option<u64>,
    /// The expected start key of the tablet. The key should be in the format
    /// tablet is actually stored, for example should have `z` prefix.
    ///
    /// Any key that is smaller than this key can be considered obsolete.
    pub start_key: Box<[u8]>,
    /// The expected end key of the tablet. The key should be in the format
    /// tablet is actually stored, for example should have `z` prefix.
    ///
    /// Any key that is larger than or equal to this key can be considered
    /// obsolete.
    pub end_key: Box<[u8]>,
    /// The states to be persisted when flush is triggered.
    ///
    /// If not set, apply may not be resumed correctly.
    pub flush_state: Option<Arc<FlushState>>,
}

impl Debug for TabletContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TabletContext")
            .field("id", &self.id)
            .field("suffix", &self.suffix)
            .field("start_key", &log_wrappers::Value::key(&self.start_key))
            .field("end_key", &log_wrappers::Value::key(&self.end_key))
            .finish()
    }
}

impl TabletContext {
    pub fn new(region: &Region, suffix: Option<u64>) -> Self {
        TabletContext {
            id: region.get_id(),
            suffix,
            start_key: keys::data_key(region.get_start_key()).into_boxed_slice(),
            end_key: keys::data_end_key(region.get_end_key()).into_boxed_slice(),
            flush_state: None,
        }
    }

    /// Create a context that assumes there is only one region and it covers the
    /// whole key space. Normally you should only use this in tests.
    pub fn with_infinite_region(id: u64, suffix: Option<u64>) -> Self {
        let mut region = Region::default();
        region.set_id(id);
        Self::new(&region, suffix)
    }
}

/// A factory trait to create new tablet for multi-rocksdb architecture.
// It should be named as `EngineFactory` for consistency, but we are about to
// rename engine to tablet, so always use tablet for new traits/types.
pub trait TabletFactory<EK>: Send + Sync {
    /// Open the tablet in `path`.
    fn open_tablet(&self, ctx: TabletContext, path: &Path) -> Result<EK>;

    /// Destroy the tablet and its data
    fn destroy_tablet(&self, ctx: TabletContext, path: &Path) -> Result<()>;

    /// Check if the tablet with specified path exists
    fn exists(&self, path: &Path) -> bool;
}

pub struct SingletonFactory<EK> {
    tablet: EK,
}

impl<EK> SingletonFactory<EK> {
    pub fn new(tablet: EK) -> Self {
        SingletonFactory { tablet }
    }
}

impl<EK: Clone + Send + Sync> TabletFactory<EK> for SingletonFactory<EK> {
    /// Open the tablet in `path`.
    ///
    /// `id` and `suffix` is used to mark the identity of tablet. The id is
    /// likely the region Id, the suffix could be the current raft log
    /// index. The reason to have suffix is that we can keep more than one
    /// tablet for a region.
    fn open_tablet(&self, _ctx: TabletContext, _path: &Path) -> Result<EK> {
        Ok(self.tablet.clone())
    }

    /// Destroy the tablet and its data
    fn destroy_tablet(&self, _ctx: TabletContext, _path: &Path) -> Result<()> {
        Ok(())
    }

    /// Check if the tablet with specified path exists
    fn exists(&self, _path: &Path) -> bool {
        true
    }
}

/// A global registry for all tablets.
struct TabletRegistryInner<EK> {
    // region_id, suffix -> tablet
    tablets: Mutex<HashMap<u64, CachedTablet<EK>>>,
    factory: Box<dyn TabletFactory<EK>>,
    root: PathBuf,
}

pub struct TabletRegistry<EK> {
    // One may consider to add cache to speed up access. But it also makes it more
    // difficult to gc stale cache.
    tablets: Arc<TabletRegistryInner<EK>>,
}

impl<EK> Clone for TabletRegistry<EK> {
    fn clone(&self) -> Self {
        Self {
            tablets: self.tablets.clone(),
        }
    }
}

impl<EK> TabletRegistry<EK> {
    pub fn new(factory: Box<dyn TabletFactory<EK>>, path: impl Into<PathBuf>) -> Result<Self> {
        let root = path.into();
        std::fs::create_dir_all(&root)?;
        Ok(TabletRegistry {
            tablets: Arc::new(TabletRegistryInner {
                tablets: Mutex::new(HashMap::default()),
                factory,
                root,
            }),
        })
    }

    /// Format the name as {prefix}_{id}_{suffix}. If prefix is empty, it will
    /// be format as {id}_{suffix}.
    pub fn tablet_name(&self, prefix: &str, id: u64, suffix: u64) -> String {
        format!(
            "{}{:_<width$}{}_{}",
            prefix,
            "",
            id,
            suffix,
            width = !prefix.is_empty() as usize
        )
    }

    /// Returns the prefix, id and suffix of the tablet name.
    pub fn parse_tablet_name<'a>(&self, path: &'a Path) -> Option<(&'a str, u64, u64)> {
        let name = path.file_name().unwrap().to_str().unwrap();
        let mut parts = name.rsplit('_');
        let suffix = parts.next()?.parse().ok()?;
        let id = parts.next()?.parse().ok()?;
        let prefix = parts.as_str();
        Some((prefix, id, suffix))
    }

    pub fn tablet_root(&self) -> &Path {
        &self.tablets.root
    }

    pub fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf {
        let name = self.tablet_name("", id, suffix);
        self.tablets.root.join(name)
    }

    /// Gets a tablet.
    pub fn get(&self, id: u64) -> Option<CachedTablet<EK>>
    where
        EK: Clone,
    {
        let tablets = self.tablets.tablets.lock().unwrap();
        tablets.get(&id).cloned()
    }

    /// Gets a tablet, create a default one if it doesn't exist.
    pub fn get_or_default(&self, id: u64) -> CachedTablet<EK>
    where
        EK: Clone,
    {
        let mut tablets = self.tablets.tablets.lock().unwrap();
        tablets
            .entry(id)
            .or_insert_with(|| CachedTablet::new(None))
            .clone()
    }

    pub fn tablet_factory(&self) -> &dyn TabletFactory<EK> {
        self.tablets.factory.as_ref()
    }

    pub fn remove(&self, id: u64) {
        self.tablets.tablets.lock().unwrap().remove(&id);
    }

    /// Load the tablet and set it as the latest.
    ///
    /// If the tablet doesn't exist, it will create an empty one.
    pub fn load(&self, ctx: TabletContext, create: bool) -> Result<CachedTablet<EK>>
    where
        EK: Clone,
    {
        assert!(ctx.suffix.is_some());
        let id = ctx.id;
        let path = self.tablet_path(id, ctx.suffix.unwrap());
        if !create && !self.tablets.factory.exists(&path) {
            return Err(Error::Other(box_err!(
                "tablet ({}, {:?}) doesn't exist",
                id,
                ctx.suffix
            )));
        }
        // TODO: use compaction filter to trim range.
        let tablet = self.tablets.factory.open_tablet(ctx, &path)?;
        let mut cached = self.get_or_default(id);
        cached.set(tablet);
        Ok(cached)
    }

    /// Loop over all opened tablets. Note, it's possible that the visited
    /// tablet is not the latest one. If latest one is required, you may
    /// either:
    /// - loop several times to make it likely to visit all tablets.
    /// - send commands to fsms instead, which can guarantee latest tablet is
    ///   visisted.
    pub fn for_each_opened_tablet(&self, mut f: impl FnMut(u64, &mut CachedTablet<EK>) -> bool) {
        let mut tablets = self.tablets.tablets.lock().unwrap();
        for (id, tablet) in tablets.iter_mut() {
            if !f(*id, tablet) {
                tablet.release();
                return;
            }
            tablet.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_tablet() {
        let mut cached_tablet = CachedTablet::new(None);
        assert_eq!(cached_tablet.cache(), None);
        assert_eq!(cached_tablet.latest(), None);

        cached_tablet = CachedTablet::new(Some(1));
        assert_eq!(cached_tablet.cache().cloned(), Some(1));
        assert_eq!(cached_tablet.latest().cloned(), Some(1));

        // Setting tablet will refresh cache immediately.
        cached_tablet.set(2);
        assert_eq!(cached_tablet.cache().cloned(), Some(2));

        // Test `latest()` will use cache.
        // Unsafe modify the data.
        let old_data = *cached_tablet.latest.data.lock().unwrap();
        *cached_tablet.latest.data.lock().unwrap() = Some(0);
        assert_eq!(cached_tablet.latest().cloned(), old_data);
        // Restore the data.
        *cached_tablet.latest.data.lock().unwrap() = old_data;

        let mut cloned = cached_tablet.clone();
        // Clone should reuse cache.
        assert_eq!(cloned.cache().cloned(), Some(2));
        cloned.set(1);
        assert_eq!(cloned.cache().cloned(), Some(1));
        assert_eq!(cloned.latest().cloned(), Some(1));

        // Local cache won't be refreshed until querying latest.
        assert_eq!(cached_tablet.cache().cloned(), Some(2));
        assert_eq!(cached_tablet.latest().cloned(), Some(1));
        assert_eq!(cached_tablet.cache().cloned(), Some(1));
    }

    #[test]
    fn test_singleton_factory() {
        let tablet = Arc::new(1);
        let singleton = SingletonFactory::new(tablet.clone());
        let registry = TabletRegistry::new(Box::new(singleton), "").unwrap();
        let mut ctx = TabletContext::with_infinite_region(1, Some(1));
        registry.load(ctx.clone(), true).unwrap();
        let mut cached = registry.get(1).unwrap();
        assert_eq!(cached.latest().cloned(), Some(tablet.clone()));

        ctx.id = 2;
        registry.load(ctx.clone(), true).unwrap();
        let mut count = 0;
        registry.for_each_opened_tablet(|id, cached| {
            assert!(&[1, 2].contains(&id), "{}", id);
            assert_eq!(cached.latest().cloned(), Some(tablet.clone()));
            count += 1;
            true
        });
        assert_eq!(count, 2);

        // Destroy should be ignored.
        registry
            .tablet_factory()
            .destroy_tablet(ctx.clone(), &registry.tablet_path(2, 1))
            .unwrap();

        // Exist check should always succeed.
        ctx.id = 3;
        registry.load(ctx, false).unwrap();
        let mut cached = registry.get(3).unwrap();
        assert_eq!(cached.latest().cloned(), Some(tablet));
    }

    type Record = Arc<(u64, u64)>;

    struct MemoryTablet {
        tablet: Mutex<HashMap<PathBuf, Record>>,
    }

    impl TabletFactory<Record> for MemoryTablet {
        fn open_tablet(&self, ctx: TabletContext, path: &Path) -> Result<Record> {
            let mut tablet = self.tablet.lock().unwrap();
            if tablet.contains_key(path) {
                return Err(Error::Other(box_err!("tablet is opened")));
            }
            tablet.insert(path.to_owned(), Arc::new((ctx.id, ctx.suffix.unwrap_or(0))));
            Ok(tablet[path].clone())
        }

        fn exists(&self, path: &Path) -> bool {
            let tablet = self.tablet.lock().unwrap();
            tablet.contains_key(path)
        }

        fn destroy_tablet(&self, ctx: TabletContext, path: &Path) -> Result<()> {
            let prev = self.tablet.lock().unwrap().remove(path).unwrap();
            assert_eq!((ctx.id, ctx.suffix.unwrap_or(0)), *prev);
            Ok(())
        }
    }

    #[test]
    fn test_tablet_registry() {
        let factory = MemoryTablet {
            tablet: Mutex::new(HashMap::default()),
        };
        let registry = TabletRegistry::new(Box::new(factory), "").unwrap();

        let mut ctx = TabletContext::with_infinite_region(1, Some(10));
        let mut tablet_1_10 = registry.load(ctx.clone(), true).unwrap();
        // It's open already, load it twice should report lock error.
        registry.load(ctx.clone(), true).unwrap_err();
        let mut cached = registry.get(1).unwrap();
        assert_eq!(cached.latest(), tablet_1_10.latest());

        let tablet_path = registry.tablet_path(1, 10);
        assert!(registry.tablet_factory().exists(&tablet_path));

        let tablet_path = registry.tablet_path(1, 11);
        assert!(!registry.tablet_factory().exists(&tablet_path));
        // Not exist tablet should report error.
        ctx.suffix = Some(11);
        registry.load(ctx.clone(), false).unwrap_err();
        assert!(registry.get(2).is_none());
        // Though path not exist, but we should be able to create an empty one.
        assert_eq!(registry.get_or_default(2).latest(), None);
        assert!(!registry.tablet_factory().exists(&tablet_path));

        // Load new suffix should update cache.
        registry.load(ctx, true).unwrap();
        assert_ne!(cached.latest(), tablet_1_10.cache());
        let tablet_path = registry.tablet_path(1, 11);
        assert!(registry.tablet_factory().exists(&tablet_path));

        let mut count = 0;
        registry.for_each_opened_tablet(|_, _| {
            count += 1;
            true
        });
        assert_eq!(count, 2);

        registry.remove(2);
        assert!(registry.get(2).is_none());
        count = 0;
        registry.for_each_opened_tablet(|_, _| {
            count += 1;
            true
        });
        assert_eq!(count, 1);

        let name = registry.tablet_name("prefix", 12, 30);
        assert_eq!(name, "prefix_12_30");
        let normal_name = registry.tablet_name("", 20, 15);
        let normal_tablet_path = registry.tablet_path(20, 15);
        assert_eq!(registry.tablet_root().join(normal_name), normal_tablet_path);

        let full_prefix_path = registry.tablet_root().join(name);
        let res = registry.parse_tablet_name(&full_prefix_path);
        assert_eq!(res, Some(("prefix", 12, 30)));
        let res = registry.parse_tablet_name(&normal_tablet_path);
        assert_eq!(res, Some(("", 20, 15)));
        let invalid_path = registry.tablet_root().join("invalid_12");
        let res = registry.parse_tablet_name(&invalid_path);
        assert_eq!(res, None);
    }
}
