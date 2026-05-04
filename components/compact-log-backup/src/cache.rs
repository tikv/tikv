// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{
        HashMap,
        hash_map::{DefaultHasher, Entry},
    },
    hash::{Hash, Hasher},
    io,
    sync::{
        Arc,
        atomic::{AtomicU64 as StdAtomicU64, Ordering},
    },
};

use bytes::Bytes;
use cloud::blob::read_to_end;
use external_storage::ExternalStorage;
use parking_lot::Mutex;
use protobuf::Chars;
use slog_global::{debug, info};
use tokio::sync::futures::OwnedNotified;

use crate::compaction::Input;

const PHYSICAL_FILE_CACHE_SHARDS: usize = 256;

enum CacheDecision {
    Ready(Bytes),
    Download {
        notify: Arc<tokio::sync::Notify>,
        physical_size: u64,
    },
    Wait(OwnedNotified),
    Bypass,
}

pub enum RegisterPhysicalFileResult {
    Registered,
    Full(OwnedNotified),
    Bypass,
}

struct CacheEntry {
    remaining_refs: usize,
    physical_size: u64,
    content: Option<Bytes>,
    loading: bool,
    notify: Arc<tokio::sync::Notify>,
}

impl CacheEntry {
    fn new(physical_size: u64) -> Self {
        Self {
            remaining_refs: 0,
            physical_size,
            content: None,
            loading: false,
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn add_ref(&mut self, ref_count: usize) {
        assert!(ref_count > 0);
        self.remaining_refs += ref_count;
    }

    fn drop_ref(&mut self) -> bool {
        assert!(self.remaining_refs > 0);
        self.remaining_refs -= 1;
        self.remaining_refs == 0
    }
}

#[derive(Default)]
struct CacheShard {
    entries: HashMap<Chars, CacheEntry>,
}

/// A bounded cache for raw physical log files.
///
/// The cache reserves capacity when a physical file is registered, then stores
/// the physical object as one `Bytes` allocation once it is first loaded.
/// Logical files are returned as zero-copy slices. When the last logical-file
/// reference of a physical file is consumed or dropped, the entry is removed
/// and the reserved capacity is released.
pub struct PhysicalFileCache {
    capacity: u64,
    reserved_bytes: StdAtomicU64,
    capacity_notify: Arc<tokio::sync::Notify>,
    shards: Box<[Mutex<CacheShard>]>,
}

/// Cleans up an in-flight physical-file download if the future is dropped.
///
/// A downloader sets `CacheEntry::loading` and releases the shard lock before
/// awaiting remote storage. If that await is cancelled, waiters would otherwise
/// keep waiting forever.
struct DownloadGuard<'a> {
    cache: &'a PhysicalFileCache,
    name: Chars,
    notify: Arc<tokio::sync::Notify>,
    finished: bool,
}

impl<'a> DownloadGuard<'a> {
    fn new(cache: &'a PhysicalFileCache, name: Chars, notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            cache,
            name,
            notify,
            finished: false,
        }
    }

    fn finish(mut self, content: Option<Bytes>) {
        self.cache
            .finish_download(&self.name, &self.notify, content);
        self.finished = true;
    }
}

impl Drop for DownloadGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.cache.finish_download(&self.name, &self.notify, None);
        }
    }
}

impl PhysicalFileCache {
    pub fn new(capacity: u64) -> Self {
        info!("create physical file cache"; "capacity" => capacity);
        Self {
            capacity,
            reserved_bytes: StdAtomicU64::new(0),
            capacity_notify: Arc::new(tokio::sync::Notify::new()),
            shards: (0..PHYSICAL_FILE_CACHE_SHARDS)
                .map(|_| Mutex::new(CacheShard::default()))
                .collect(),
        }
    }

    fn shard(&self, name: &Chars) -> &Mutex<CacheShard> {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        &self.shards[hasher.finish() as usize % self.shards.len()]
    }

    fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    fn try_reserve(&self, physical_size: u64) -> Option<u64> {
        let mut current = self.reserved_bytes();
        loop {
            let next = current.checked_add(physical_size)?;
            if next > self.capacity {
                return None;
            }
            match self.reserved_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(next),
                Err(actual) => current = actual,
            }
        }
    }

    fn release_reserved(&self, physical_size: u64) {
        let mut current = self.reserved_bytes();
        loop {
            let next = current.saturating_sub(physical_size);
            match self.reserved_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.capacity_notify.notify_one();
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn register_physical_file(
        &self,
        file_name: &Chars,
        file_size: u64,
        ref_count: usize,
    ) -> RegisterPhysicalFileResult {
        if ref_count == 0 {
            return RegisterPhysicalFileResult::Registered;
        }
        if file_size > self.capacity {
            return RegisterPhysicalFileResult::Bypass;
        }
        let mut shard = self.shard(file_name).lock();
        let entry = match shard.entries.entry(file_name.clone()) {
            Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                entry.physical_size = file_size;
                entry.add_ref(ref_count);
                return RegisterPhysicalFileResult::Registered;
            }
            Entry::Vacant(entry) => {
                let wait = Arc::clone(&self.capacity_notify).notified_owned();
                if self.try_reserve(file_size).is_none() {
                    return RegisterPhysicalFileResult::Full(wait);
                }
                entry.insert(CacheEntry::new(file_size))
            }
        };
        entry.physical_size = file_size;
        entry.add_ref(ref_count);
        RegisterPhysicalFileResult::Registered
    }

    pub fn drop_input_ref(&self, input: &Input) {
        self.drop_physical_file_ref(&input.id.name);
    }

    /// Marks one registered logical file in this physical file as consumed.
    ///
    /// For cache hits, call this only after the returned slice has been fully
    /// processed. Otherwise a retry could consume the same ref twice, or the
    /// capacity reservation could be released while the slice still keeps the
    /// physical allocation alive.
    pub(crate) fn drop_physical_file_ref(&self, file_name: &Chars) {
        let mut release_size = None;
        {
            let mut shard = self.shard(file_name).lock();
            if let Entry::Occupied(mut entry) = shard.entries.entry(file_name.clone()) {
                let item = entry.get_mut();
                if item.drop_ref() {
                    release_size = Some(item.physical_size);
                    entry.remove();
                }
            }
        }
        if let Some(physical_size) = release_size {
            self.release_reserved(physical_size);
        }
    }

    pub(crate) fn contains_physical_file(&self, file_name: &Chars) -> bool {
        self.shard(file_name).lock().entries.contains_key(file_name)
    }

    pub async fn load_part(
        &self,
        storage: Arc<dyn ExternalStorage>,
        name: &Chars,
        offset: u64,
        length: u64,
    ) -> io::Result<Option<(Bytes, u64)>> {
        let mut physical_bytes_in = 0;
        loop {
            match self.cache_decision(&name, offset, length)? {
                CacheDecision::Ready(content) => return Ok(Some((content, physical_bytes_in))),
                CacheDecision::Wait(notified) => notified.await,
                CacheDecision::Bypass => return Ok(None),
                CacheDecision::Download {
                    notify,
                    physical_size,
                } => {
                    let guard = DownloadGuard::new(self, name.clone(), notify);
                    let download_res = self
                        .download_physical_file(storage.as_ref(), &name, physical_size)
                        .await;
                    match download_res {
                        Ok((content, read_size)) => {
                            physical_bytes_in += read_size;
                            guard.finish(Some(content));
                        }
                        Err(err) => {
                            guard.finish(None);
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    fn cache_decision(&self, name: &Chars, offset: u64, length: u64) -> io::Result<CacheDecision> {
        let decision = {
            let mut shard = self.shard(name).lock();
            match shard.entries.entry(name.clone()) {
                Entry::Vacant(_) => CacheDecision::Bypass,
                Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    if let Some(content) = entry.content.as_ref() {
                        let part = checked_get_range(name, content, offset, length)?;
                        CacheDecision::Ready(part)
                    } else if entry.loading {
                        CacheDecision::Wait(Arc::clone(&entry.notify).notified_owned())
                    } else {
                        entry.loading = true;
                        CacheDecision::Download {
                            notify: Arc::clone(&entry.notify),
                            physical_size: entry.physical_size,
                        }
                    }
                }
            }
        };
        Ok(decision)
    }

    async fn download_physical_file(
        &self,
        storage: &dyn ExternalStorage,
        name: &Chars,
        physical_size: u64,
    ) -> io::Result<(Bytes, u64)> {
        let mut content = Vec::with_capacity(physical_size as usize);
        let read_size = read_to_end(storage.read(name), &mut content).await?;
        debug!("loaded physical file into cache"; "name" => name.to_string(), "size" => read_size);
        Ok((Bytes::from(content), read_size))
    }

    fn finish_download(&self, name: &Chars, notify: &tokio::sync::Notify, content: Option<Bytes>) {
        let mut shard = self.shard(name).lock();
        if let Some(entry) = shard.entries.get_mut(name) {
            if let Some(content) = content {
                entry.content = Some(content);
            }
            entry.loading = false;
        }
        notify.notify_waiters();
    }
}

fn checked_get_range(name: &Chars, content: &Bytes, offset: u64, length: u64) -> io::Result<Bytes> {
    let start = usize::try_from(offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("log file {name} offset {offset} is too large"),
        )
    })?;
    let end = usize::try_from(offset.checked_add(length).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("log file {name} length {offset} + {length} is too large"),
        )
    })?)
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("log file {name} length {offset} + {length} is too large"),
        )
    })?;
    if end > content.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "log file {name} range [{start}, {end}) exceeds cached physical file {} length {}",
                name,
                content.len()
            ),
        ));
    }
    Ok(content.slice(start..end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_part(decision: CacheDecision) -> Bytes {
        match decision {
            CacheDecision::Ready(part) => part,
            _ => panic!("expected cached part to be ready"),
        }
    }

    #[test]
    fn test_download_guard_clears_loading_on_drop() {
        let cache = PhysicalFileCache::new(8);
        let name = Chars::from("physical.log");
        cache.register_physical_file(&name, 8, 1);

        let notify = match cache.cache_decision(&name, 0, 4).unwrap() {
            CacheDecision::Download { notify, .. } => notify,
            _ => panic!("expected cache download"),
        };
        assert_eq!(cache.reserved_bytes(), 8);
        {
            let shard = cache.shard(&name).lock();
            assert!(shard.entries.get(&name).unwrap().loading);
        }

        drop(DownloadGuard::new(&cache, name.clone(), notify));

        assert_eq!(cache.reserved_bytes(), 8);
        let shard = cache.shard(&name).lock();
        let entry = shard.entries.get(&name).unwrap();
        assert!(!entry.loading);
        assert!(entry.content.is_none());
    }

    #[test]
    fn test_reserved_bytes_released_after_cached_parts_are_consumed() {
        let cache = PhysicalFileCache::new(10);
        let name = Chars::from("physical.log");
        cache.register_physical_file(&name, 10, 2);
        assert_eq!(cache.reserved_bytes(), 10);
        let notify = tokio::sync::Notify::new();
        cache.finish_download(&name, &notify, Some(Bytes::from_static(b"0123456789")));

        let part1 = ready_part(cache.cache_decision(&name, 0, 5).unwrap());
        assert_eq!(part1, Bytes::from_static(b"01234"));
        assert_eq!(cache.reserved_bytes(), 10);
        cache.drop_physical_file_ref(&name);
        assert_eq!(cache.reserved_bytes(), 10);

        let part2 = ready_part(cache.cache_decision(&name, 5, 5).unwrap());
        assert_eq!(part2, Bytes::from_static(b"56789"));
        assert_eq!(cache.reserved_bytes(), 10);
        cache.drop_physical_file_ref(&name);
        assert_eq!(cache.reserved_bytes(), 0);
        {
            let shard = cache.shard(&name).lock();
            assert!(!shard.entries.contains_key(&name));
        }

        drop(part2);
        assert_eq!(cache.reserved_bytes(), 0);
        drop(part1);
        assert_eq!(cache.reserved_bytes(), 0);
    }
}
