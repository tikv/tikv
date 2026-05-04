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
use slog_global::{debug, info, warn};
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
    round: u64,
    physical_size: u64,
    content: Option<Bytes>,
    loading: bool,
    notify: Arc<tokio::sync::Notify>,
}

impl CacheEntry {
    fn new(physical_size: u64, round: u64) -> Self {
        Self {
            remaining_refs: 0,
            round,
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

#[derive(Default)]
struct CacheRoundState {
    current_round: u64,
    last_round_left_files: usize,
    current_round_left_files: usize,
}

#[derive(Default)]
struct CacheStatsSnapshot {
    entries: usize,
    loaded_entries: usize,
    loading_entries: usize,
    cached_content_bytes: u64,
    remaining_refs: usize,
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
    /// Tracks cache windows split by forced compaction. A new forced
    /// compaction is allowed only after every physical file from the previous
    /// forced window has released its cache ref.
    round_state: Mutex<CacheRoundState>,
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
            warn!("physical file cache download future was dropped";
                "name" => %self.name,
            );
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
            round_state: Mutex::new(CacheRoundState::default()),
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

    fn round_state_snapshot(&self) -> (u64, usize, usize) {
        let state = self.round_state.lock();
        (
            state.current_round,
            state.last_round_left_files,
            state.current_round_left_files,
        )
    }

    fn cache_stats_snapshot(&self) -> CacheStatsSnapshot {
        let mut snapshot = CacheStatsSnapshot::default();
        for shard in self.shards.iter() {
            let shard = shard.lock();
            for entry in shard.entries.values() {
                snapshot.entries += 1;
                snapshot.remaining_refs += entry.remaining_refs;
                if entry.loading {
                    snapshot.loading_entries += 1;
                }
                if let Some(content) = entry.content.as_ref() {
                    snapshot.loaded_entries += 1;
                    snapshot.cached_content_bytes += content.len() as u64;
                }
            }
        }
        snapshot
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

    fn register_current_round_file(&self) -> u64 {
        let mut state = self.round_state.lock();
        let round = state.current_round;
        state.current_round_left_files += 1;
        round
    }

    fn release_round_file(&self, round: u64) {
        let mut state = self.round_state.lock();
        if round == state.current_round {
            assert!(state.current_round_left_files > 0);
            state.current_round_left_files -= 1;
        } else {
            assert!(state.last_round_left_files > 0);
            state.last_round_left_files -= 1;
        }
    }

    pub(crate) fn can_force_compaction(&self) -> bool {
        self.round_state.lock().last_round_left_files == 0
    }

    pub(crate) fn advance_round_after_force_compaction(&self) {
        let mut state = self.round_state.lock();
        assert_eq!(state.last_round_left_files, 0);
        let previous_round = state.current_round;
        let moved_files = state.current_round_left_files;
        state.current_round += 1;
        state.last_round_left_files = state.current_round_left_files;
        state.current_round_left_files = 0;
        info!("physical file cache forced compaction round advanced";
            "previous_round" => previous_round,
            "current_round" => state.current_round,
            "last_round_left_files" => state.last_round_left_files,
            "moved_files" => moved_files,
            "reserved_bytes" => self.reserved_bytes(),
            "capacity" => self.capacity,
        );
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
            debug!("physical file cache bypasses oversized physical file";
                "name" => %file_name,
                "physical_size" => file_size,
                "capacity" => self.capacity,
                "ref_count" => ref_count,
            );
            return RegisterPhysicalFileResult::Bypass;
        }
        let mut registered_round = None;
        let mut total_refs = 0;
        let full_wait = {
            let mut shard = self.shard(file_name).lock();
            match shard.entries.entry(file_name.clone()) {
                Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    if entry.physical_size != file_size {
                        warn!("physical file cache duplicate registration has different size";
                            "name" => %file_name,
                            "old_physical_size" => entry.physical_size,
                            "new_physical_size" => file_size,
                            "reserved_bytes" => self.reserved_bytes(),
                        );
                    }
                    entry.physical_size = file_size;
                    entry.add_ref(ref_count);
                    registered_round = Some(entry.round);
                    total_refs = entry.remaining_refs;
                    None
                }
                Entry::Vacant(entry) => {
                    let wait = Arc::clone(&self.capacity_notify).notified_owned();
                    if self.try_reserve(file_size).is_none() {
                        Some(wait)
                    } else {
                        let round = self.register_current_round_file();
                        let entry = entry.insert(CacheEntry::new(file_size, round));
                        entry.physical_size = file_size;
                        entry.add_ref(ref_count);
                        registered_round = Some(round);
                        total_refs = entry.remaining_refs;
                        None
                    }
                }
            }
        };
        if let Some(wait) = full_wait {
            let (current_round, last_round_left_files, current_round_left_files) =
                self.round_state_snapshot();
            let stats = self.cache_stats_snapshot();
            info!("physical file cache is full when registering physical file";
                "name" => %file_name,
                "physical_size" => file_size,
                "ref_count" => ref_count,
                "reserved_bytes" => self.reserved_bytes(),
                "capacity" => self.capacity,
                "current_round" => current_round,
                "last_round_left_files" => last_round_left_files,
                "current_round_left_files" => current_round_left_files,
                "cache_entries" => stats.entries,
                "loaded_entries" => stats.loaded_entries,
                "loading_entries" => stats.loading_entries,
                "cached_content_bytes" => stats.cached_content_bytes,
                "remaining_refs" => stats.remaining_refs,
            );
            return RegisterPhysicalFileResult::Full(wait);
        }
        let (current_round, last_round_left_files, current_round_left_files) =
            self.round_state_snapshot();
        debug!("registered physical file into cache";
            "name" => %file_name,
            "physical_size" => file_size,
            "ref_count" => ref_count,
            "total_refs" => total_refs,
            "entry_round" => registered_round.unwrap_or_default(),
            "reserved_bytes" => self.reserved_bytes(),
            "capacity" => self.capacity,
            "current_round" => current_round,
            "last_round_left_files" => last_round_left_files,
            "current_round_left_files" => current_round_left_files,
        );
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
        let mut release_round = None;
        let mut remaining_refs = None;
        {
            let mut shard = self.shard(file_name).lock();
            if let Entry::Occupied(mut entry) = shard.entries.entry(file_name.clone()) {
                let item = entry.get_mut();
                if item.drop_ref() {
                    release_size = Some(item.physical_size);
                    release_round = Some(item.round);
                    entry.remove();
                } else {
                    remaining_refs = Some(item.remaining_refs);
                }
            }
        }
        if let (Some(physical_size), Some(round)) = (release_size, release_round) {
            self.release_round_file(round);
            self.release_reserved(physical_size);
            let (current_round, last_round_left_files, current_round_left_files) =
                self.round_state_snapshot();
            info!("released physical file from cache";
                "name" => %file_name,
                "physical_size" => physical_size,
                "entry_round" => round,
                "reserved_bytes" => self.reserved_bytes(),
                "capacity" => self.capacity,
                "current_round" => current_round,
                "last_round_left_files" => last_round_left_files,
                "current_round_left_files" => current_round_left_files,
            );
        } else if let Some(remaining_refs) = remaining_refs {
            debug!("dropped one physical file cache ref";
                "name" => %file_name,
                "remaining_refs" => remaining_refs,
            );
        } else {
            debug!("physical file cache ref drop ignored because entry is absent";
                "name" => %file_name,
            );
        }
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
                CacheDecision::Ready(content) => {
                    debug!("physical file cache hit";
                        "name" => %name,
                        "offset" => offset,
                        "length" => length,
                        "downloaded_bytes_for_wait" => physical_bytes_in,
                    );
                    return Ok(Some((content, physical_bytes_in)));
                }
                CacheDecision::Wait(notified) => {
                    debug!("physical file cache waits for loading physical file";
                        "name" => %name,
                        "offset" => offset,
                        "length" => length,
                    );
                    notified.await
                }
                CacheDecision::Bypass => {
                    debug!("physical file cache load bypassed";
                        "name" => %name,
                        "offset" => offset,
                        "length" => length,
                    );
                    return Ok(None);
                }
                CacheDecision::Download {
                    notify,
                    physical_size,
                } => {
                    debug!("physical file cache starts downloading physical file";
                        "name" => %name,
                        "physical_size" => physical_size,
                        "reserved_bytes" => self.reserved_bytes(),
                        "capacity" => self.capacity,
                    );
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
        let content_len = content.as_ref().map(|content| content.len() as u64);
        let mut shard = self.shard(name).lock();
        if let Some(entry) = shard.entries.get_mut(name) {
            if let Some(content) = content {
                entry.content = Some(content);
            }
            entry.loading = false;
        }
        notify.notify_waiters();
        debug!("physical file cache finished download state update";
            "name" => %name,
            "content_bytes" => content_len.unwrap_or(0),
            "has_content" => content_len.is_some(),
            "reserved_bytes" => self.reserved_bytes(),
            "capacity" => self.capacity,
        );
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
