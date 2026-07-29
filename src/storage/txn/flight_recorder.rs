// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! A bounded in-memory flight recorder for transaction commands.
//!
//! It captures transaction command arrivals, cache observations, and successful
//! lock/write modifications. It stores neither raw keys nor user values.

use std::{
    collections::{VecDeque, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    mem::size_of,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crossbeam::utils::CachePadded;
use engine_traits::{CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::PrewriteRequestPessimisticAction;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use tikv_kv::Modify;
use tikv_util::{Either, config::ReadableSize};
use txn_types::{Key, LockType, TimeStamp, WriteRef, WriteType, parse_lock};

use crate::storage::{Context, metrics::CommandKind, txn::commands::Command};

const SHARD_COUNT: usize = 256;
pub(crate) const DEFAULT_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY: ReadableSize = ReadableSize::mb(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnCommandEventKind {
    Received,
    PutLock,
    PutInMemoryPessimisticLock,
    DeleteLock,
    PutWrite,
    DeleteWrite,
    TxnStatusCacheHit,
    TxnStatusCacheMiss,
}

pub(crate) fn hash_key(key: &Key) -> u64 {
    hash_encoded_key(key.as_encoded())
}

fn hash_encoded_key(key: &[u8]) -> u64 {
    // Hash the encoded key so diagnostics cannot panic on a malformed
    // memcomparable key.
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone, Copy)]
struct CommandKey {
    key_hash: u64,
    pessimistic_action: Option<PrewriteRequestPessimisticAction>,
    txn_start_ts: u64,
}

/// Metadata captured before a command is moved into its MVCC command handler.
pub(crate) struct TxnCommandEventMetadata {
    event: TxnCommandEvent,
    keys: Vec<CommandKey>,
}

impl TxnCommandEventMetadata {
    fn from_command(cmd: &Command, cid: u64, snapshot_data_version: Option<u64>) -> Option<Self> {
        let ctx = cmd.ctx();
        let mut metadata = Self {
            event: TxnCommandEvent::new(cmd.tag(), cid, ctx, snapshot_data_version),
            keys: Vec::new(),
        };
        metadata.event.txn_start_ts = cmd.ts().into_inner();

        match cmd {
            Command::Prewrite(c) => {
                metadata.event.lock_ttl = c.lock_ttl;
                metadata.event.min_commit_ts = c.min_commit_ts.into_inner();
                metadata.event.skip_constraint_check = c.skip_constraint_check;
                metadata.event.try_one_pc = c.try_one_pc;
                let primary_key = Key::from_raw(&c.primary);
                if let Some(mutation) = c
                    .mutations
                    .iter()
                    .find(|mutation| mutation.key() == &primary_key)
                {
                    metadata.keys.push(CommandKey {
                        key_hash: hash_key(mutation.key()),
                        pessimistic_action: Some(
                            PrewriteRequestPessimisticAction::SkipPessimisticCheck,
                        ),
                        txn_start_ts: 0,
                    });
                }
            }
            Command::PrewritePessimistic(c) => {
                metadata.event.for_update_ts = c.for_update_ts.into_inner();
                metadata.event.lock_ttl = c.lock_ttl;
                metadata.event.min_commit_ts = c.min_commit_ts.into_inner();
                metadata.event.try_one_pc = c.try_one_pc;
                let primary_key = Key::from_raw(&c.primary);
                if let Some((mutation, action)) = c
                    .mutations
                    .iter()
                    .find(|(mutation, _)| mutation.key() == &primary_key)
                {
                    metadata.keys.push(CommandKey {
                        key_hash: hash_key(mutation.key()),
                        pessimistic_action: Some(*action),
                        txn_start_ts: 0,
                    });
                }
            }
            Command::AcquirePessimisticLock(c) => {
                metadata.event.for_update_ts = c.for_update_ts.into_inner();
                metadata.event.lock_ttl = c.lock_ttl;
                metadata.event.min_commit_ts = c.min_commit_ts.into_inner();
                let primary_key = Key::from_raw(&c.primary);
                if let Some((key, ..)) = c.keys.iter().find(|(key, ..)| key == &primary_key) {
                    metadata.keys.push(command_key(key));
                }
            }
            Command::AcquirePessimisticLockResumed(c) => {
                metadata.keys.extend(
                    c.items
                        .iter()
                        .filter(|item| item.key.is_encoded_from(&item.params.primary))
                        .map(|item| CommandKey {
                            key_hash: hash_key(&item.key),
                            pessimistic_action: None,
                            txn_start_ts: item.params.start_ts.into_inner(),
                        }),
                );
            }
            Command::Commit(c) => {
                metadata.event.txn_start_ts = c.lock_ts.into_inner();
                metadata.event.commit_ts = c.commit_ts.into_inner();
                // A Commit request does not identify its primary key. It is
                // populated after the command reads the lock, so only its
                // successfully applied primary-key modifies are recorded.
            }
            Command::TxnHeartBeat(c) => {
                metadata.event.min_commit_ts = c.min_commit_ts;
                metadata.keys.push(command_key(&c.primary_key));
            }
            Command::CheckTxnStatus(c) => {
                metadata.event.caller_start_ts = c.caller_start_ts.into_inner();
                metadata.event.current_ts = c.current_ts.into_inner();
                metadata.event.rollback_if_not_exist = c.rollback_if_not_exist;
                metadata.event.force_sync_commit = c.force_sync_commit;
                metadata.event.resolving_pessimistic_lock = c.resolving_pessimistic_lock;
                metadata.event.verify_is_primary = c.verify_is_primary;
                metadata.keys.push(command_key(&c.primary_key));
            }
            Command::ResolveLock(c) => {
                metadata.keys.extend(
                    c.key_locks
                        .iter()
                        .filter(|(key, lock)| key.is_encoded_from(&lock.primary))
                        .map(|(key, lock)| CommandKey {
                            key_hash: hash_key(key),
                            pessimistic_action: None,
                            txn_start_ts: lock.ts.into_inner(),
                        }),
                );
            }
            Command::ResolveLockLite(c) => {
                metadata.event.commit_ts = c.commit_ts.into_inner();
                // ResolveLockLite also learns the primary key only after
                // reading the lock, so only its successfully applied
                // primary-key modifies are recorded.
            }
            Command::Flush(c) => {
                metadata.event.lock_ttl = c.lock_ttl;
                let primary_key = Key::from_raw(&c.primary);
                if let Some(mutation) = c
                    .mutations
                    .iter()
                    .find(|mutation| mutation.key() == &primary_key)
                {
                    metadata.keys.push(CommandKey {
                        key_hash: hash_key(mutation.key()),
                        pessimistic_action: None,
                        txn_start_ts: 0,
                    });
                }
            }
            _ => {}
        }

        metadata.keys.sort_unstable_by_key(|key| key.key_hash);
        let identifies_primary_while_processing = matches!(cmd, Command::Commit(_))
            || matches!(cmd, Command::ResolveLockLite(c) if !c.commit_ts.is_zero());
        (!metadata.keys.is_empty() || identifies_primary_while_processing).then_some(metadata)
    }

    pub(crate) fn set_primary_key_hash(&mut self, key_hash: u64) {
        if key_hash == 0 || !self.keys.is_empty() {
            return;
        }
        self.keys.push(CommandKey {
            key_hash,
            pessimistic_action: None,
            txn_start_ts: self.event.txn_start_ts,
        });
    }

    pub(crate) fn events_for_modifies(&self, modifies: &[Modify]) -> Vec<TxnCommandEvent> {
        if self.keys.is_empty() {
            return Vec::new();
        }
        modifies
            .iter()
            .filter_map(|modify| self.event_for_modify(modify))
            .collect()
    }

    fn base_event(&self, command_key: CommandKey, kind: TxnCommandEventKind) -> TxnCommandEvent {
        let mut event = self.event;
        event.kind = kind;
        event.key_hash = command_key.key_hash;
        if command_key.txn_start_ts != 0 {
            event.txn_start_ts = command_key.txn_start_ts;
        }
        event.pessimistic_action = command_key.pessimistic_action;
        event
    }

    fn command_key(&self, key_hash: u64) -> Option<CommandKey> {
        self.keys
            .binary_search_by_key(&key_hash, |key| key.key_hash)
            .ok()
            .map(|index| self.keys[index])
    }

    fn event_for_modify(&self, modify: &Modify) -> Option<TxnCommandEvent> {
        match modify {
            Modify::Put(cf, key, value) if *cf == CF_LOCK => {
                let key_hash = hash_key(key);
                let command_key = self.command_key(key_hash)?;
                let mut event = self.base_event(command_key, TxnCommandEventKind::PutLock);
                match parse_lock(value).ok()? {
                    Either::Left(lock) => {
                        event.lock_type = Some(lock.lock_type);
                        event.txn_start_ts = lock.ts.into_inner();
                        event.for_update_ts = lock.for_update_ts.into_inner();
                        event.min_commit_ts = lock.min_commit_ts.into_inner();
                        event.lock_ttl = lock.ttl;
                        event.generation = lock.generation;
                    }
                    Either::Right(_) => event.lock_type = Some(LockType::Shared),
                }
                Some(event)
            }
            Modify::Delete(cf, key) if *cf == CF_LOCK => {
                let key_hash = hash_key(key);
                let command_key = self.command_key(key_hash)?;
                Some(self.base_event(command_key, TxnCommandEventKind::DeleteLock))
            }
            Modify::Put(cf, key, value) if *cf == CF_WRITE => {
                let (user_key, commit_ts) = Key::split_on_ts_for(key.as_encoded()).ok()?;
                let key_hash = hash_encoded_key(user_key);
                let command_key = self.command_key(key_hash)?;
                let write = WriteRef::parse(value).ok()?;
                let mut event = self.base_event(command_key, TxnCommandEventKind::PutWrite);
                event.commit_ts = commit_ts.into_inner();
                event.write_type = Some(write.write_type);
                event.txn_start_ts = write.start_ts.into_inner();
                // An overlapped rollback reuses the older transaction's write
                // record; without this flag the event looks like a normal
                // commit of that older transaction at `commit_ts`.
                event.has_overlapped_rollback = write.has_overlapped_rollback;
                Some(event)
            }
            Modify::Delete(cf, key) if *cf == CF_WRITE => {
                let (user_key, commit_ts) = Key::split_on_ts_for(key.as_encoded()).ok()?;
                let key_hash = hash_encoded_key(user_key);
                let command_key = self.command_key(key_hash)?;
                let mut event = self.base_event(command_key, TxnCommandEventKind::DeleteWrite);
                event.commit_ts = commit_ts.into_inner();
                Some(event)
            }
            Modify::PessimisticLock(key, lock) => {
                let key_hash = hash_key(key);
                let command_key = self.command_key(key_hash)?;
                let mut event = self.base_event(command_key, TxnCommandEventKind::PutLock);
                event.lock_type = Some(LockType::Pessimistic);
                event.txn_start_ts = lock.start_ts.into_inner();
                event.for_update_ts = lock.for_update_ts.into_inner();
                event.min_commit_ts = lock.min_commit_ts.into_inner();
                event.lock_ttl = lock.ttl;
                Some(event)
            }
            _ => None,
        }
    }
}

fn command_key(key: &Key) -> CommandKey {
    CommandKey {
        key_hash: hash_key(key),
        pessimistic_action: None,
        txn_start_ts: 0,
    }
}

/// A single immutable record. It deliberately contains no raw key or user
/// value.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // Fields are consumed by the derived Debug output in panic diagnostics.
pub(crate) struct TxnCommandEvent {
    unix_time_ms: u64,
    kind: TxnCommandEventKind,
    command: CommandKind,
    cid: u64,
    key_hash: u64,
    txn_start_ts: u64,
    commit_ts: u64,
    caller_start_ts: u64,
    current_ts: u64,
    for_update_ts: u64,
    min_commit_ts: u64,
    lock_ttl: u64,
    region_id: u64,
    term: u64,
    snapshot_data_version: u64,
    is_retry_request: bool,
    skip_constraint_check: bool,
    try_one_pc: bool,
    rollback_if_not_exist: bool,
    force_sync_commit: bool,
    resolving_pessimistic_lock: bool,
    verify_is_primary: bool,
    pessimistic_action: Option<PrewriteRequestPessimisticAction>,
    lock_type: Option<LockType>,
    write_type: Option<WriteType>,
    has_overlapped_rollback: bool,
    generation: u64,
}

pub(crate) const MIN_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY: usize =
    SHARD_COUNT * size_of::<TxnCommandEvent>();

impl TxnCommandEvent {
    fn new(
        command: CommandKind,
        cid: u64,
        ctx: &Context,
        snapshot_data_version: Option<u64>,
    ) -> Self {
        Self {
            unix_time_ms: 0,
            kind: TxnCommandEventKind::Received,
            command,
            cid,
            key_hash: 0,
            txn_start_ts: 0,
            commit_ts: 0,
            caller_start_ts: 0,
            current_ts: 0,
            for_update_ts: 0,
            min_commit_ts: 0,
            lock_ttl: 0,
            region_id: ctx.get_region_id(),
            term: ctx.get_term(),
            snapshot_data_version: snapshot_data_version.unwrap_or(0),
            is_retry_request: ctx.get_is_retry_request(),
            skip_constraint_check: false,
            try_one_pc: false,
            rollback_if_not_exist: false,
            force_sync_commit: false,
            resolving_pessimistic_lock: false,
            verify_is_primary: false,
            pessimistic_action: None,
            lock_type: None,
            write_type: None,
            has_overlapped_rollback: false,
            generation: 0,
        }
    }
}

struct RecorderShard {
    events: VecDeque<TxnCommandEvent>,
    max_evicted_start_ts: u64,
}

impl RecorderShard {
    fn new(capacity: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(capacity),
            max_evicted_start_ts: 0,
        }
    }

    fn evict_oldest(&mut self) {
        if let Some(event) = self.events.pop_front() {
            let max_start_ts = if event.has_overlapped_rollback {
                // The write key's commit_ts is also the start_ts of the
                // rollback that overlapped this write record.
                event.txn_start_ts.max(event.commit_ts)
            } else {
                event.txn_start_ts
            };
            self.max_evicted_start_ts = self.max_evicted_start_ts.max(max_start_ts);
        }
    }
}

pub(crate) struct TxnCommandHistory {
    pub(crate) events: Vec<TxnCommandEvent>,
    pub(crate) max_evicted_start_ts: u64,
    pub(crate) recorder_enabled: bool,
}

impl TxnCommandHistory {
    /// Returns whether this recorder session has not evicted an event whose
    /// transaction start_ts is at least `start_ts`.
    pub(crate) fn is_complete_by_eviction(&self, start_ts: TimeStamp) -> bool {
        self.recorder_enabled && self.max_evicted_start_ts < start_ts.into_inner()
    }
}

pub(crate) struct TxnCommandFlightRecorder {
    enabled: AtomicBool,
    shards: Vec<CachePadded<Mutex<RecorderShard>>>,
    shard_mask: usize,
    configured_events_per_shard: AtomicUsize,
}

impl TxnCommandFlightRecorder {
    fn new(shard_count: usize, events_per_shard: usize, enabled: bool) -> Self {
        assert!(shard_count.is_power_of_two() && events_per_shard > 0);
        let capacity = if enabled { events_per_shard } else { 0 };
        let shards = (0..shard_count)
            .map(|_| CachePadded::new(Mutex::new(RecorderShard::new(capacity))))
            .collect();
        Self {
            enabled: AtomicBool::new(enabled),
            shards,
            shard_mask: shard_count - 1,
            configured_events_per_shard: AtomicUsize::new(events_per_shard),
        }
    }

    fn with_capacity(shard_count: usize, capacity: usize, enabled: bool) -> Self {
        Self::new(
            shard_count,
            events_per_shard(shard_count, capacity),
            enabled,
        )
    }

    #[inline]
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Disabling clears the current history session and releases its memory.
    pub(crate) fn set_enabled(&self, enabled: bool) {
        if !enabled {
            if !self.enabled.swap(false, Ordering::AcqRel) {
                return;
            }
            for shard in &self.shards {
                *shard.lock() = RecorderShard::new(0);
            }
            return;
        }
        if self.is_enabled() {
            return;
        }
        let events_per_shard = self.configured_events_per_shard.load(Ordering::Relaxed);
        for shard in &self.shards {
            *shard.lock() = RecorderShard::new(events_per_shard);
        }
        self.enabled.store(true, Ordering::Release);
    }

    pub(crate) fn set_capacity(&self, capacity: usize) {
        let events_per_shard = events_per_shard(self.shards.len(), capacity);
        if self
            .configured_events_per_shard
            .swap(events_per_shard, Ordering::Relaxed)
            == events_per_shard
        {
            return;
        }
        if !self.is_enabled() {
            return;
        }

        for shard in &self.shards {
            let mut shard = shard.lock();
            let mut resized = VecDeque::with_capacity(events_per_shard);
            while shard.events.len() > resized.capacity() {
                shard.evict_oldest();
            }
            resized.extend(shard.events.drain(..));
            shard.events = resized;
        }
    }

    #[inline]
    pub(crate) fn command_metadata(
        &self,
        cmd: &Command,
        cid: u64,
        snapshot_data_version: Option<u64>,
    ) -> Option<TxnCommandEventMetadata> {
        if !self.is_enabled() {
            return None;
        }
        TxnCommandEventMetadata::from_command(cmd, cid, snapshot_data_version)
    }

    #[inline]
    pub(crate) fn record_received(&self, cmd: &Command, cid: u64) {
        if let Some(metadata) = self.command_metadata(cmd, cid, None) {
            self.record(
                metadata
                    .keys
                    .iter()
                    .map(|key| metadata.base_event(*key, TxnCommandEventKind::Received)),
            );
        }
    }

    fn record(&self, events: impl IntoIterator<Item = TxnCommandEvent>) {
        if !self.is_enabled() {
            return;
        }
        let mut events = events.into_iter().peekable();
        if events.peek().is_none() {
            return;
        }
        let unix_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for mut event in events {
            debug_assert_ne!(event.txn_start_ts, 0);
            let shard_index = event.key_hash as usize & self.shard_mask;
            let mut shard = self.shards[shard_index].lock();
            // Pair with set_enabled(false): a recorder that passed the first
            // check before disabling must not repopulate a cleared shard.
            if !self.is_enabled() {
                continue;
            }
            event.unix_time_ms = unix_time_ms;
            if shard.events.len() == shard.events.capacity() {
                shard.evict_oldest();
            }
            shard.events.push_back(event);
        }
    }

    pub(crate) fn record_persistent_modifies(&self, events: &[TxnCommandEvent]) {
        self.record(events.iter().copied())
    }

    pub(crate) fn record_in_memory_pessimistic_locks(&self, events: &[TxnCommandEvent]) {
        self.record(events.iter().copied().map(|mut event| {
            event.kind = TxnCommandEventKind::PutInMemoryPessimisticLock;
            event
        }))
    }

    /// Records a txn-status-cache lookup observed while processing a prewrite.
    ///
    /// The command cid is not plumbed into command processing, so the event
    /// keeps `cid == 0` as an auxiliary record; correlate it with the owning
    /// command through its key hash, `txn_start_ts`, `is_retry_request`, and
    /// timestamp.
    pub(crate) fn record_txn_status_cache_lookup<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a Key>,
        primary_key: &[u8],
        start_ts: TimeStamp,
        committed_ts: Option<TimeStamp>,
        ctx: &Context,
        snapshot_data_version: Option<u64>,
    ) {
        if !self.is_enabled() {
            return;
        }
        let primary_key = Key::from_raw(primary_key);
        let kind = if committed_ts.is_some() {
            TxnCommandEventKind::TxnStatusCacheHit
        } else {
            TxnCommandEventKind::TxnStatusCacheMiss
        };
        if let Some(key) = keys.into_iter().find(|key| *key == &primary_key) {
            let key_hash = hash_key(key);
            let mut event =
                TxnCommandEvent::new(CommandKind::prewrite, 0, ctx, snapshot_data_version);
            event.kind = kind;
            event.key_hash = key_hash;
            event.txn_start_ts = start_ts.into_inner();
            event.commit_ts = committed_ts.unwrap_or_default().into_inner();
            self.record([event]);
        }
    }

    pub(crate) fn history_for_key(&self, key: &Key) -> TxnCommandHistory {
        let key_hash = hash_key(key);
        let shard_index = key_hash as usize & self.shard_mask;
        let shard = self.shards[shard_index].lock();
        let events = shard
            .events
            .iter()
            .filter(|event| event.key_hash == key_hash)
            .copied()
            .collect();
        TxnCommandHistory {
            events,
            max_evicted_start_ts: shard.max_evicted_start_ts,
            recorder_enabled: self.is_enabled(),
        }
    }
}

fn events_per_shard(shard_count: usize, capacity: usize) -> usize {
    (capacity / shard_count / size_of::<TxnCommandEvent>()).max(1)
}

lazy_static! {
    pub(crate) static ref TXN_FLIGHT_RECORDER: TxnCommandFlightRecorder =
        TxnCommandFlightRecorder::with_capacity(
            SHARD_COUNT,
            DEFAULT_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY.0 as usize,
            false,
        );
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use txn_types::{Lock, Write};

    use super::*;

    fn event_for_key(key: &Key, cid: u64) -> TxnCommandEvent {
        let mut event = TxnCommandEvent::new(CommandKind::prewrite, cid, &Context::default(), None);
        event.key_hash = hash_key(key);
        event.txn_start_ts = cid;
        event
    }

    #[test]
    fn test_bounded_history_for_key() {
        let recorder = TxnCommandFlightRecorder::new(2, 4, true);
        let key = Key::from_raw(b"key");
        let other_key = Key::from_raw(b"other-key");

        for cid in 1..=5 {
            recorder.record([event_for_key(&key, cid)]);
        }
        recorder.record([event_for_key(&other_key, 100)]);

        let history = recorder.history_for_key(&key);
        assert!(history.events.len() <= 4);
        assert_eq!(history.events.last().unwrap().cid, 5);
        assert!(history.events.iter().all(|event| event.cid != 100));
        assert!(!history.is_complete_by_eviction(1.into()));
        assert!(history.is_complete_by_eviction(3.into()));
    }

    #[test]
    fn test_modify_events_are_normalized_to_user_key() {
        let key = Key::from_raw(b"key");
        let secondary_key = Key::from_raw(b"secondary-key");
        let key_hash = hash_key(&key);
        let mut event =
            TxnCommandEvent::new(CommandKind::prewrite, 42, &Context::default(), Some(4));
        event.txn_start_ts = 10;
        event.for_update_ts = 11;
        event.min_commit_ts = 12;
        event.lock_ttl = 20_000;
        let mut metadata = TxnCommandEventMetadata {
            event,
            keys: vec![CommandKey {
                key_hash,
                pessimistic_action: Some(PrewriteRequestPessimisticAction::DoPessimisticCheck),
                txn_start_ts: 10,
            }],
        };
        let lock = Lock::new(
            LockType::Put,
            b"key".to_vec(),
            10.into(),
            20_000,
            None,
            11.into(),
            1,
            12.into(),
            false,
        );
        let secondary_lock = Lock::new(
            LockType::Put,
            b"key".to_vec(),
            10.into(),
            20_000,
            None,
            11.into(),
            1,
            12.into(),
            false,
        );
        let write = Write::new(WriteType::Put, 10.into(), None);
        let modifies = vec![
            Modify::Put(CF_LOCK, key.clone(), lock.to_bytes()),
            Modify::Put(CF_LOCK, secondary_key.clone(), secondary_lock.to_bytes()),
            Modify::Put(
                CF_WRITE,
                key.clone().append_ts(20.into()),
                write.as_ref().to_bytes(),
            ),
            Modify::Put(
                CF_WRITE,
                secondary_key.clone().append_ts(20.into()),
                write.as_ref().to_bytes(),
            ),
        ];

        let events = metadata.events_for_modifies(&modifies);
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.key_hash == key_hash));
        assert_eq!(events[0].kind, TxnCommandEventKind::PutLock);
        assert_eq!(events[0].txn_start_ts, 10);
        assert_eq!(
            events[0].pessimistic_action,
            Some(PrewriteRequestPessimisticAction::DoPessimisticCheck)
        );
        assert_eq!(events[1].kind, TxnCommandEventKind::PutWrite);
        assert_eq!(events[1].commit_ts, 20);
        assert_eq!(events[1].write_type, Some(WriteType::Put));

        metadata.keys.clear();
        metadata.set_primary_key_hash(key_hash);
        let events = metadata.events_for_modifies(&modifies);
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.key_hash == key_hash));
        assert!(events.iter().all(|event| !event.has_overlapped_rollback));

        // An overlapped rollback record keeps the older transaction's write
        // type/start_ts. Its commit_ts is also the overlapped rollback's
        // start_ts, and both timestamps contribute to the eviction watermark.
        let overlapped =
            Write::new(WriteType::Put, 10.into(), None).set_overlapped_rollback(true, None);
        let events = metadata.events_for_modifies(&[Modify::Put(
            CF_WRITE,
            key.clone().append_ts(30.into()),
            overlapped.as_ref().to_bytes(),
        )]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, TxnCommandEventKind::PutWrite);
        assert_eq!(events[0].commit_ts, 30);
        assert_eq!(events[0].txn_start_ts, 10);
        assert!(events[0].has_overlapped_rollback);

        let recorder = TxnCommandFlightRecorder::new(1, 1, true);
        recorder.record([events[0]]);
        recorder.record([event_for_key(&key, 31)]);
        let history = recorder.history_for_key(&key);
        assert_eq!(history.max_evicted_start_ts, 30);
        assert!(!history.is_complete_by_eviction(30.into()));
        assert!(history.is_complete_by_eviction(31.into()));
    }

    #[test]
    fn test_event_size_is_bounded() {
        let size = size_of::<TxnCommandEvent>();
        assert!(size <= 160, "event size is {size}");

        let events_per_shard = events_per_shard(
            SHARD_COUNT,
            DEFAULT_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY.0 as usize,
        );
        let retained_event_bytes = SHARD_COUNT * events_per_shard * size;
        assert!(retained_event_bytes <= DEFAULT_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY.0 as usize);
        assert!(
            DEFAULT_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY.0 as usize - retained_event_bytes
                < MIN_TXN_COMMAND_FLIGHT_RECORDER_CAPACITY
        );
    }

    #[test]
    fn test_resize_capacity() {
        let recorder = TxnCommandFlightRecorder::new(2, 4, true);
        let key = Key::from_raw(b"key");

        for cid in 1..=4 {
            recorder.record([event_for_key(&key, cid)]);
        }

        recorder.set_capacity(2 * 2 * size_of::<TxnCommandEvent>());
        assert!(
            recorder
                .shards
                .iter()
                .all(|shard| shard.lock().events.capacity() >= 2)
        );
        let history = recorder.history_for_key(&key);
        assert_eq!(history.events.len(), 2);
        assert_eq!(history.events[0].cid, 3);
        assert_eq!(history.events[1].cid, 4);
        assert_eq!(history.max_evicted_start_ts, 2);
        assert!(!history.is_complete_by_eviction(2.into()));
        assert!(history.is_complete_by_eviction(3.into()));

        recorder.set_capacity(2 * 4 * size_of::<TxnCommandEvent>());
        assert!(
            recorder
                .shards
                .iter()
                .all(|shard| shard.lock().events.capacity() >= 4)
        );
        for cid in 5..=6 {
            recorder.record([event_for_key(&key, cid)]);
        }
        let history = recorder.history_for_key(&key);
        assert_eq!(history.events.len(), 4);
        assert_eq!(history.events[0].cid, 3);
        assert_eq!(history.events[3].cid, 6);
        assert_eq!(history.max_evicted_start_ts, 2);
    }

    #[test]
    fn test_txn_status_cache_lookup_event() {
        let recorder = TxnCommandFlightRecorder::new(2, 4, true);
        let key = Key::from_raw(b"key");
        let secondary_key = Key::from_raw(b"secondary-key");
        let mut ctx = Context::default();
        ctx.set_region_id(7);
        ctx.set_is_retry_request(true);

        recorder.record_txn_status_cache_lookup(
            [&secondary_key, &key],
            b"key",
            10.into(),
            Some(20.into()),
            &ctx,
            Some(30),
        );

        let history = recorder.history_for_key(&key);
        assert_eq!(history.events.len(), 1);
        assert_eq!(
            history.events[0].kind,
            TxnCommandEventKind::TxnStatusCacheHit
        );
        assert_eq!(history.events[0].txn_start_ts, 10);
        assert_eq!(history.events[0].commit_ts, 20);
        assert_eq!(history.events[0].snapshot_data_version, 30);
        assert!(history.events[0].is_retry_request);
        assert!(recorder.history_for_key(&secondary_key).events.is_empty());
    }

    #[test]
    fn test_disabled_recorder_does_not_record_and_enable_starts_fresh() {
        let recorder = TxnCommandFlightRecorder::new(2, 4, false);
        let key = Key::from_raw(b"key");
        assert!(
            recorder
                .shards
                .iter()
                .all(|shard| shard.lock().events.capacity() == 0)
        );

        recorder.record([event_for_key(&key, 1)]);
        let history = recorder.history_for_key(&key);
        assert!(history.events.is_empty());
        assert!(!history.is_complete_by_eviction(1.into()));

        recorder.set_enabled(true);
        assert!(
            recorder
                .shards
                .iter()
                .all(|shard| shard.lock().events.capacity() >= 4)
        );
        for cid in 2..=6 {
            recorder.record([event_for_key(&key, cid)]);
        }
        let history = recorder.history_for_key(&key);
        assert_eq!(history.events.len(), 4);
        assert_eq!(history.max_evicted_start_ts, 2);

        recorder.set_enabled(false);
        let history = recorder.history_for_key(&key);
        assert!(history.events.is_empty());
        assert!(!history.recorder_enabled);
        assert!(
            recorder
                .shards
                .iter()
                .all(|shard| shard.lock().events.capacity() == 0)
        );
        recorder.set_capacity(2 * 2 * size_of::<TxnCommandEvent>());
        recorder.set_enabled(true);
        let history = recorder.history_for_key(&key);
        assert!(history.events.is_empty());
        assert_eq!(history.max_evicted_start_ts, 0);
        assert!(
            recorder
                .shards
                .iter()
                .all(|shard| shard.lock().events.capacity() >= 2)
        );
    }
}
