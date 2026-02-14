// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    io,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use external_storage::ExternalStorage;
use tikv_util::lru::{GetTailEntry, LruCache};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy)]
pub struct CacheAccessStat {
    pub cache_hit: u64,
    pub cache_miss: u64,
    pub cache_inflight_wait: u64,
    pub cache_evicted_files: u64,
    pub cache_evicted_bytes: u64,
    pub remote_read_calls: u64,
    pub remote_read_bytes: u64,
}

impl CacheAccessStat {
    fn hit() -> Self {
        Self {
            cache_hit: 1,
            ..Default::default()
        }
    }

    fn miss(remote_read_bytes: u64) -> Self {
        Self {
            cache_miss: 1,
            remote_read_calls: 1,
            remote_read_bytes,
            ..Default::default()
        }
    }

    fn inflight_wait() -> Self {
        Self {
            cache_inflight_wait: 1,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
struct CacheEntry {
    path: PathBuf,
    size: u64,
}

struct CacheState {
    lru: LruCache<String, CacheEntry>,
    total_bytes: u64,
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            lru: LruCache::with_capacity(usize::MAX),
            total_bytes: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct InFlightError {
    kind: io::ErrorKind,
    message: String,
}

impl From<InFlightError> for io::Error {
    fn from(value: InFlightError) -> Self {
        io::Error::new(value.kind, value.message)
    }
}

#[derive(Default)]
struct InFlight {
    done: Notify,
    result: Mutex<Option<std::result::Result<PathBuf, InFlightError>>>,
}

impl InFlight {
    async fn wait(&self) -> io::Result<PathBuf> {
        loop {
            if let Some(res) = self.result.lock().await.clone() {
                return res.map_err(Into::into);
            }
            self.done.notified().await;
        }
    }

    async fn finish_ok(&self, path: PathBuf) {
        *self.result.lock().await = Some(Ok(path));
        self.done.notify_waiters();
    }

    async fn finish_err(&self, err: io::Error) {
        let inflight_err = InFlightError {
            kind: err.kind(),
            message: err.to_string(),
        };
        *self.result.lock().await = Some(Err(inflight_err));
        self.done.notify_waiters();
    }
}

/// A per-execution local cache for materializing remote log objects.
///
/// It provides:
/// - Per-object in-flight de-duplication for downloads.
/// - Global capacity control (best-effort LRU eviction, by whole-object files).
/// - Atomic file publishing (temp + rename).
pub struct LocalObjectCache {
    dir: PathBuf,
    capacity_bytes: u64,
    state: Mutex<CacheState>,
    inflight: DashMap<String, Arc<InFlight>>,
    download_limiter: Option<Semaphore>,
}

impl LocalObjectCache {
    pub async fn new(dir: PathBuf, capacity_bytes: u64, max_inflight_downloads: usize) -> io::Result<Self> {
        let pid = std::process::id();
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let dir = dir.join(format!("run_{}_{}", pid, ts_ms));
        tokio::fs::create_dir_all(&dir).await?;

        Ok(Self {
            dir,
            capacity_bytes,
            state: Mutex::new(CacheState::default()),
            inflight: DashMap::default(),
            download_limiter: (max_inflight_downloads > 0).then(|| Semaphore::new(max_inflight_downloads)),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn get_or_fetch(
        &self,
        storage: &dyn ExternalStorage,
        key: &str,
    ) -> io::Result<(PathBuf, CacheAccessStat)> {
        if let Some(path) = self.get_hit(key).await? {
            return Ok((path, CacheAccessStat::hit()));
        }

        use dashmap::mapref::entry::Entry;
        let entry = self.inflight.entry(key.to_owned());
        match entry {
            Entry::Occupied(e) => {
                let inflight = Arc::clone(e.get());
                drop(e);
                let path = inflight.wait().await?;
                Ok((path, CacheAccessStat::inflight_wait()))
            }
            Entry::Vacant(v) => {
                let inflight = Arc::new(InFlight::default());
                v.insert(Arc::clone(&inflight));

                let res = self.fetch_and_insert(storage, key).await;
                match res {
                    Ok((path, stat)) => {
                        inflight.finish_ok(path.clone()).await;
                        self.inflight.remove(key);
                        Ok((path, stat))
                    }
                    Err(err) => {
                        inflight.finish_err(io::Error::new(err.kind(), err.to_string())).await;
                        self.inflight.remove(key);
                        Err(err)
                    }
                }
            }
        }
    }

    async fn get_hit(&self, key: &str) -> io::Result<Option<PathBuf>> {
        let maybe = {
            let mut state = self.state.lock().await;
            state.lru.get(&key.to_owned()).cloned()
        };
        let Some(entry) = maybe else {
            return Ok(None);
        };

        match std::fs::metadata(&entry.path) {
            Ok(meta) if meta.is_file() => Ok(Some(entry.path)),
            _ => {
                let mut state = self.state.lock().await;
                if let Some(removed) = state.lru.remove(&key.to_owned()) {
                    state.total_bytes = state.total_bytes.saturating_sub(removed.size);
                }
                Ok(None)
            }
        }
    }

    async fn fetch_and_insert(
        &self,
        storage: &dyn ExternalStorage,
        key: &str,
    ) -> io::Result<(PathBuf, CacheAccessStat)> {
        let _permit = match &self.download_limiter {
            Some(s) => Some(s.acquire().await.map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "download limiter closed unexpectedly")
            })?),
            None => None,
        };

        let final_path = self.path_for_key(key)?;
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp_path = final_path.with_extension(format!("tmp.{}", Uuid::new_v4()));
        let remote_read_bytes = match self.download_to(storage, key, &tmp_path).await {
            Ok(n) => n,
            Err(err) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return Err(err);
            }
        };

        if let Err(err) = tokio::fs::rename(&tmp_path, &final_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(err);
        }

        let (evicted, evicted_bytes) = self.insert_and_evict(key, final_path.clone(), remote_read_bytes).await;
        let mut stat = CacheAccessStat::miss(remote_read_bytes);
        stat.cache_evicted_files = evicted;
        stat.cache_evicted_bytes = evicted_bytes;
        Ok((final_path, stat))
    }

    async fn download_to(
        &self,
        storage: &dyn ExternalStorage,
        key: &str,
        tmp_path: &Path,
    ) -> io::Result<u64> {
        let mut reader = storage.read(key).compat();
        let mut file = tokio::fs::File::create(tmp_path).await?;
        let n = tokio::io::copy(&mut reader, &mut file).await?;
        file.flush().await?;
        Ok(n)
    }

    async fn insert_and_evict(&self, key: &str, path: PathBuf, size: u64) -> (u64, u64) {
        let mut to_delete = vec![];
        let (mut evicted_files, mut evicted_bytes) = (0u64, 0u64);

        {
            let mut state = self.state.lock().await;
            if let Some(old) = state.lru.remove(&key.to_owned()) {
                state.total_bytes = state.total_bytes.saturating_sub(old.size);
                if old.path != path {
                    to_delete.push(old.path);
                }
            }

            state.lru.insert(
                key.to_owned(),
                CacheEntry {
                    path: path.clone(),
                    size,
                },
            );
            state.total_bytes = state.total_bytes.saturating_add(size);

            while state.total_bytes > self.capacity_bytes && state.lru.len() > 1 {
                let Some((tail_key, _)) = state.lru.get_tail_entry() else {
                    break;
                };
                if tail_key.as_str() == key {
                    break;
                }
                let tail_key = tail_key.clone();
                if let Some(entry) = state.lru.remove(&tail_key) {
                    state.total_bytes = state.total_bytes.saturating_sub(entry.size);
                    evicted_files += 1;
                    evicted_bytes += entry.size;
                    to_delete.push(entry.path);
                } else {
                    break;
                }
            }
        }

        for p in to_delete {
            if let Err(err) = tokio::fs::remove_file(&p).await {
                if err.kind() != io::ErrorKind::NotFound {
                    tikv_util::warn!("failed to remove evicted cache file"; "path" => %p.display(), "err" => %err);
                }
            }
        }

        (evicted_files, evicted_bytes)
    }

    fn path_for_key(&self, key: &str) -> io::Result<PathBuf> {
        let rel = sanitize_relative_path(key)?;
        Ok(self.dir.join(rel))
    }
}

fn sanitize_relative_path(key: &str) -> io::Result<PathBuf> {
    let p = Path::new(key);
    let mut rel = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Normal(s) => rel.push(s),
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) | Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid external object name: {key}"),
                ));
            }
        }
    }
    if rel.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid external object name: {key}"),
        ));
    }
    Ok(rel)
}
