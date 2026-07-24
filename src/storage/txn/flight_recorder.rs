// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! A bounded in-memory flight recorder for transaction commands.
//!
//! It captures transaction command arrivals, cache observations, and successful
//! lock/write modifications. It stores neither raw keys nor user values.

use std::{
    collections::{VecDeque, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crossbeam::utils::CachePadded;
use engine_traits::{CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::PrewriteRequestPessimisticAction;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use tikv_kv::Modify;
use tikv_util::Either;
use txn_types::{Key, LockType, TimeStamp, WriteRef, WriteType, parse_lock};

use crate::storage::{Context, metrics::CommandKind, txn::commands::Command};

const TXN_COMMAND_FLIGHT_RECORDER_SHARDS: usize = 128;
const TXN_COMMAND_FLIGHT_RECORDER_EVENTS_PER_SHARD: usize = 4096;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordedPessimisticAction {
    Unknown,
    SkipPessimisticCheck,
    DoPessimisticCheck,
    DoConstraintCheck,
}

impl From<PrewriteRequestPessimisticAction> for RecordedPessimisticAction {
    fn from(action: PrewriteRequestPessimisticAction) -> Self {
        use PrewriteRequestPessimisticAction::*;
        match action {
            SkipPessimisticCheck => Self::SkipPessimisticCheck,
            DoPessimisticCheck => Self::DoPessimisticCheck,
            DoConstraintCheck => Self::DoConstraintCheck,
        }
    }
}

fn hash_key(key: &Key) -> u64 {
    // Hash the encoded key so diagnostics cannot panic on a malformed
    // memcomparable key.
    let mut hasher = DefaultHasher::new();
    key.as_encoded().hash(&mut hasher);
    hasher.finish()
}

fn hash_raw_key(raw_key: &[u8]) -> u64 {
    hash_key(&Key::from_raw(raw_key))
}

#[derive(Clone, Copy)]
struct CommandKey {
    key_hash: u64,
    pessimistic_action: RecordedPessimisticAction,
    txn_start_ts: u64,
}

/// Metadata captured before a command is moved into its MVCC command handler.
pub(crate) struct TxnCommandEventMetadata {
    cid: u64,
    command: CommandKind,
    txn_start_ts: u64,
    commit_ts: u64,
    caller_start_ts: u64,
    current_ts: u64,
    for_update_ts: u64,
    min_commit_ts: u64,
    lock_ttl: u64,
    primary_key_hash: u64,
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
    keys: Vec<CommandKey>,
}

impl TxnCommandEventMetadata {
    fn from_command(cmd: &Command, cid: u64, snapshot_data_version: Option<u64>) -> Self {
        let ctx = cmd.ctx();
        let mut metadata = Self {
            cid,
            command: cmd.tag(),
            txn_start_ts: cmd.ts().into_inner(),
            commit_ts: 0,
            caller_start_ts: 0,
            current_ts: 0,
            for_update_ts: 0,
            min_commit_ts: 0,
            lock_ttl: 0,
            primary_key_hash: 0,
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
            keys: Vec::new(),
        };

        match cmd {
            Command::Prewrite(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.lock_ttl = c.lock_ttl;
                metadata.min_commit_ts = c.min_commit_ts.into_inner();
                metadata.primary_key_hash = hash_raw_key(&c.primary);
                metadata.skip_constraint_check = c.skip_constraint_check;
                metadata.try_one_pc = c.try_one_pc;
                metadata
                    .keys
                    .extend(c.mutations.iter().map(|mutation| CommandKey {
                        key_hash: hash_key(mutation.key()),
                        pessimistic_action: RecordedPessimisticAction::SkipPessimisticCheck,
                        txn_start_ts: c.start_ts.into_inner(),
                    }));
            }
            Command::PrewritePessimistic(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.for_update_ts = c.for_update_ts.into_inner();
                metadata.lock_ttl = c.lock_ttl;
                metadata.min_commit_ts = c.min_commit_ts.into_inner();
                metadata.primary_key_hash = hash_raw_key(&c.primary);
                metadata.try_one_pc = c.try_one_pc;
                metadata
                    .keys
                    .extend(c.mutations.iter().map(|(mutation, action)| CommandKey {
                        key_hash: hash_key(mutation.key()),
                        pessimistic_action: (*action).into(),
                        txn_start_ts: c.start_ts.into_inner(),
                    }));
            }
            Command::AcquirePessimisticLock(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.for_update_ts = c.for_update_ts.into_inner();
                metadata.lock_ttl = c.lock_ttl;
                metadata.min_commit_ts = c.min_commit_ts.into_inner();
                metadata.primary_key_hash = hash_raw_key(&c.primary);
                metadata
                    .keys
                    .extend(c.keys.iter().map(|(key, ..)| command_key(key, c.start_ts)));
            }
            Command::AcquirePessimisticLockResumed(c) => {
                metadata.keys.extend(c.items.iter().map(|item| CommandKey {
                    key_hash: hash_key(&item.key),
                    pessimistic_action: RecordedPessimisticAction::Unknown,
                    txn_start_ts: item.params.start_ts.into_inner(),
                }));
            }
            Command::Commit(c) => {
                metadata.txn_start_ts = c.lock_ts.into_inner();
                metadata.commit_ts = c.commit_ts.into_inner();
                metadata
                    .keys
                    .extend(c.keys.iter().map(|key| command_key(key, c.lock_ts)));
            }
            Command::Cleanup(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.keys.push(command_key(&c.key, c.start_ts));
            }
            Command::Rollback(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata
                    .keys
                    .extend(c.keys.iter().map(|key| command_key(key, c.start_ts)));
            }
            Command::PessimisticRollback(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.for_update_ts = c.for_update_ts.into_inner();
                metadata
                    .keys
                    .extend(c.keys.iter().map(|key| command_key(key, c.start_ts)));
            }
            Command::TxnHeartBeat(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.min_commit_ts = c.min_commit_ts;
                metadata.keys.push(command_key(&c.primary_key, c.start_ts));
                metadata.primary_key_hash = hash_key(&c.primary_key);
            }
            Command::CheckTxnStatus(c) => {
                metadata.txn_start_ts = c.lock_ts.into_inner();
                metadata.caller_start_ts = c.caller_start_ts.into_inner();
                metadata.current_ts = c.current_ts.into_inner();
                metadata.rollback_if_not_exist = c.rollback_if_not_exist;
                metadata.force_sync_commit = c.force_sync_commit;
                metadata.resolving_pessimistic_lock = c.resolving_pessimistic_lock;
                metadata.verify_is_primary = c.verify_is_primary;
                metadata.keys.push(command_key(&c.primary_key, c.lock_ts));
                metadata.primary_key_hash = hash_key(&c.primary_key);
            }
            Command::CheckSecondaryLocks(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata
                    .keys
                    .extend(c.keys.iter().map(|key| command_key(key, c.start_ts)));
            }
            Command::ResolveLock(c) => {
                metadata
                    .keys
                    .extend(c.key_locks.iter().map(|(key, lock)| CommandKey {
                        key_hash: hash_key(key),
                        pessimistic_action: RecordedPessimisticAction::Unknown,
                        txn_start_ts: lock.ts.into_inner(),
                    }));
            }
            Command::ResolveLockLite(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.commit_ts = c.commit_ts.into_inner();
                metadata.keys.extend(
                    c.resolve_keys
                        .iter()
                        .map(|key| command_key(key, c.start_ts)),
                );
            }
            Command::Flush(c) => {
                metadata.txn_start_ts = c.start_ts.into_inner();
                metadata.lock_ttl = c.lock_ttl;
                metadata.primary_key_hash = hash_raw_key(&c.primary);
                metadata
                    .keys
                    .extend(c.mutations.iter().map(|mutation| CommandKey {
                        key_hash: hash_key(mutation.key()),
                        pessimistic_action: RecordedPessimisticAction::Unknown,
                        txn_start_ts: c.start_ts.into_inner(),
                    }));
            }
            _ => {}
        }

        metadata.keys.sort_unstable_by_key(|key| key.key_hash);
        metadata
    }

    pub(crate) fn events_for_modifies(&self, modifies: &[Modify]) -> Vec<TxnCommandEvent> {
        modifies
            .iter()
            .filter_map(|modify| self.event_for_modify(modify))
            .collect()
    }

    fn event_for_command_key(&self, key: CommandKey) -> TxnCommandEvent {
        self.base_event(key.key_hash, TxnCommandEventKind::Received)
    }

    fn base_event(&self, key_hash: u64, kind: TxnCommandEventKind) -> TxnCommandEvent {
        let command_key = self.command_key(key_hash);
        TxnCommandEvent {
            unix_time_ms: 0,
            kind,
            command: self.command,
            cid: self.cid,
            key_hash,
            primary_key_hash: self.primary_key_hash,
            txn_start_ts: command_key
                .map(|key| key.txn_start_ts)
                .filter(|ts| *ts != 0)
                .unwrap_or(self.txn_start_ts),
            commit_ts: self.commit_ts,
            caller_start_ts: self.caller_start_ts,
            current_ts: self.current_ts,
            for_update_ts: self.for_update_ts,
            min_commit_ts: self.min_commit_ts,
            lock_ttl: self.lock_ttl,
            region_id: self.region_id,
            term: self.term,
            snapshot_data_version: self.snapshot_data_version,
            is_retry_request: self.is_retry_request,
            skip_constraint_check: self.skip_constraint_check,
            try_one_pc: self.try_one_pc,
            rollback_if_not_exist: self.rollback_if_not_exist,
            force_sync_commit: self.force_sync_commit,
            resolving_pessimistic_lock: self.resolving_pessimistic_lock,
            verify_is_primary: self.verify_is_primary,
            pessimistic_action: command_key
                .map(|key| key.pessimistic_action)
                .unwrap_or(RecordedPessimisticAction::Unknown),
            lock_type: None,
            write_type: None,
            generation: 0,
        }
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
                let mut event = self.base_event(hash_key(key), TxnCommandEventKind::PutLock);
                match parse_lock(value).ok()? {
                    Either::Left(lock) => {
                        event.lock_type = Some(lock.lock_type);
                        event.txn_start_ts = lock.ts.into_inner();
                        event.for_update_ts = lock.for_update_ts.into_inner();
                        event.min_commit_ts = lock.min_commit_ts.into_inner();
                        event.lock_ttl = lock.ttl;
                        event.primary_key_hash = hash_raw_key(&lock.primary);
                        event.generation = lock.generation;
                    }
                    Either::Right(_) => event.lock_type = Some(LockType::Shared),
                }
                Some(event)
            }
            Modify::Delete(cf, key) if *cf == CF_LOCK => {
                Some(self.base_event(hash_key(key), TxnCommandEventKind::DeleteLock))
            }
            Modify::Put(cf, key, value) if *cf == CF_WRITE => {
                let commit_ts = key.decode_ts().ok()?;
                let user_key = key.clone().truncate_ts().ok()?;
                let write = WriteRef::parse(value).ok()?;
                let mut event = self.base_event(hash_key(&user_key), TxnCommandEventKind::PutWrite);
                event.commit_ts = commit_ts.into_inner();
                event.write_type = Some(write.write_type);
                event.txn_start_ts = write.start_ts.into_inner();
                Some(event)
            }
            Modify::Delete(cf, key) if *cf == CF_WRITE => {
                let commit_ts = key.decode_ts().ok()?;
                let user_key = key.clone().truncate_ts().ok()?;
                let mut event =
                    self.base_event(hash_key(&user_key), TxnCommandEventKind::DeleteWrite);
                event.commit_ts = commit_ts.into_inner();
                Some(event)
            }
            Modify::PessimisticLock(key, lock) => {
                let mut event = self.base_event(hash_key(key), TxnCommandEventKind::PutLock);
                event.lock_type = Some(LockType::Pessimistic);
                event.txn_start_ts = lock.start_ts.into_inner();
                event.for_update_ts = lock.for_update_ts.into_inner();
                event.min_commit_ts = lock.min_commit_ts.into_inner();
                event.lock_ttl = lock.ttl;
                event.primary_key_hash = hash_raw_key(&lock.primary);
                Some(event)
            }
            _ => None,
        }
    }
}

fn command_key(key: &Key, start_ts: TimeStamp) -> CommandKey {
    CommandKey {
        key_hash: hash_key(key),
        pessimistic_action: RecordedPessimisticAction::Unknown,
        txn_start_ts: start_ts.into_inner(),
    }
}

/// A single immutable record. It deliberately contains no raw key or user
/// value.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Fields are consumed by the derived Debug output in panic diagnostics.
pub(crate) struct TxnCommandEvent {
    unix_time_ms: u64,
    kind: TxnCommandEventKind,
    command: CommandKind,
    cid: u64,
    key_hash: u64,
    primary_key_hash: u64,
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
    pessimistic_action: RecordedPessimisticAction,
    lock_type: Option<LockType>,
    write_type: Option<WriteType>,
    generation: u64,
}

pub(crate) struct TxnCommandFlightRecorder {
    enabled: AtomicBool,
    shards: Vec<CachePadded<Mutex<VecDeque<TxnCommandEvent>>>>,
    shard_mask: usize,
    events_per_shard: usize,
    overwritten_events: AtomicU64,
}

impl TxnCommandFlightRecorder {
    fn new(shard_count: usize, events_per_shard: usize, enabled: bool) -> Self {
        assert!(shard_count.is_power_of_two() && events_per_shard > 0);
        let shards = (0..shard_count)
            .map(|_| CachePadded::new(Mutex::new(VecDeque::new())))
            .collect();
        Self {
            enabled: AtomicBool::new(enabled),
            shards,
            shard_mask: shard_count - 1,
            events_per_shard,
            overwritten_events: AtomicU64::new(0),
        }
    }

    #[inline]
    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Disabling clears retained events and releases their ring-buffer memory.
    pub(crate) fn set_enabled(&self, enabled: bool) {
        if !enabled {
            if !self.enabled.swap(false, Ordering::AcqRel) {
                return;
            }
            for shard in &self.shards {
                let mut shard = shard.lock();
                shard.clear();
                shard.shrink_to_fit();
            }
            self.overwritten_events.store(0, Ordering::Relaxed);
            return;
        }
        if self.enabled.load(Ordering::Acquire) {
            return;
        }
        self.enabled.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn command_metadata(
        &self,
        cmd: &Command,
        cid: u64,
        snapshot_data_version: Option<u64>,
    ) -> Option<TxnCommandEventMetadata> {
        self.is_enabled()
            .then(|| TxnCommandEventMetadata::from_command(cmd, cid, snapshot_data_version))
    }

    #[inline]
    pub(crate) fn record_received(&self, cmd: &Command) {
        if let Some(metadata) = self.command_metadata(cmd, 0, None) {
            self.record(
                metadata
                    .keys
                    .iter()
                    .map(|key| metadata.event_for_command_key(*key)),
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
            let shard_index = event.key_hash as usize & self.shard_mask;
            let mut shard = self.shards[shard_index].lock();
            // Pair with set_enabled(false): a recorder that passed the first
            // check before disabling must not repopulate a cleared shard.
            if !self.is_enabled() {
                continue;
            }
            event.unix_time_ms = unix_time_ms;
            if shard.len() == self.events_per_shard {
                shard.pop_front();
                self.overwritten_events.fetch_add(1, Ordering::Relaxed);
            }
            shard.push_back(event);
        }
    }

    pub(crate) fn record_persistent_modifies(&self, events: &[TxnCommandEvent]) {
        if events.is_empty() {
            return;
        }
        self.record(events.iter().cloned())
    }

    pub(crate) fn record_in_memory_pessimistic_locks(&self, events: &[TxnCommandEvent]) {
        if events.is_empty() {
            return;
        }
        self.record(events.iter().cloned().map(|mut event| {
            event.kind = TxnCommandEventKind::PutInMemoryPessimisticLock;
            event
        }))
    }

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
        let metadata = TxnCommandEventMetadata {
            cid: 0,
            command: CommandKind::prewrite,
            txn_start_ts: start_ts.into_inner(),
            commit_ts: committed_ts.unwrap_or_default().into_inner(),
            caller_start_ts: 0,
            current_ts: 0,
            for_update_ts: 0,
            min_commit_ts: 0,
            lock_ttl: 0,
            primary_key_hash: hash_raw_key(primary_key),
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
            keys: Vec::new(),
        };
        let kind = if committed_ts.is_some() {
            TxnCommandEventKind::TxnStatusCacheHit
        } else {
            TxnCommandEventKind::TxnStatusCacheMiss
        };
        self.record(
            keys.into_iter()
                .map(|key| metadata.base_event(hash_key(key), kind)),
        );
    }

    pub(crate) fn events_for_key(&self, key: &Key) -> Vec<TxnCommandEvent> {
        let key_hash = hash_key(key);
        let shard_index = key_hash as usize & self.shard_mask;
        self.shards[shard_index]
            .lock()
            .iter()
            .filter(|event| event.key_hash == key_hash)
            .cloned()
            .collect()
    }

    pub(crate) fn overwritten_events(&self) -> u64 {
        self.overwritten_events.load(Ordering::Relaxed)
    }
}

lazy_static! {
    pub(crate) static ref TXN_COMMAND_FLIGHT_RECORDER: TxnCommandFlightRecorder =
        TxnCommandFlightRecorder::new(
            TXN_COMMAND_FLIGHT_RECORDER_SHARDS,
            TXN_COMMAND_FLIGHT_RECORDER_EVENTS_PER_SHARD,
            false,
        );
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use txn_types::{Lock, Write};

    use super::*;

    fn event_for_key(key: &Key, cid: u64) -> TxnCommandEvent {
        TxnCommandEvent {
            unix_time_ms: 0,
            kind: TxnCommandEventKind::Received,
            command: CommandKind::prewrite,
            cid,
            key_hash: hash_key(key),
            primary_key_hash: 0,
            txn_start_ts: 0,
            commit_ts: 0,
            caller_start_ts: 0,
            current_ts: 0,
            for_update_ts: 0,
            min_commit_ts: 0,
            lock_ttl: 0,
            region_id: 0,
            term: 0,
            snapshot_data_version: 0,
            is_retry_request: false,
            skip_constraint_check: false,
            try_one_pc: false,
            rollback_if_not_exist: false,
            force_sync_commit: false,
            resolving_pessimistic_lock: false,
            verify_is_primary: false,
            pessimistic_action: RecordedPessimisticAction::Unknown,
            lock_type: None,
            write_type: None,
            generation: 0,
        }
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

        let events = recorder.events_for_key(&key);
        assert!(events.len() <= 4);
        assert_eq!(events.last().unwrap().cid, 5);
        assert!(events.iter().all(|event| event.cid != 100));
        assert!(recorder.overwritten_events() >= 1);
    }

    #[test]
    fn test_modify_events_are_normalized_to_user_key() {
        let key = Key::from_raw(b"key");
        let key_hash = hash_key(&key);
        let metadata = TxnCommandEventMetadata {
            cid: 42,
            command: CommandKind::prewrite,
            txn_start_ts: 10,
            commit_ts: 0,
            caller_start_ts: 0,
            current_ts: 0,
            for_update_ts: 11,
            min_commit_ts: 12,
            lock_ttl: 20_000,
            primary_key_hash: key_hash,
            region_id: 1,
            term: 2,
            snapshot_data_version: 4,
            is_retry_request: true,
            skip_constraint_check: false,
            try_one_pc: false,
            rollback_if_not_exist: false,
            force_sync_commit: false,
            resolving_pessimistic_lock: false,
            verify_is_primary: false,
            keys: vec![CommandKey {
                key_hash,
                pessimistic_action: RecordedPessimisticAction::DoPessimisticCheck,
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
        let write = Write::new(WriteType::Put, 10.into(), None);
        let modifies = vec![
            Modify::Put(CF_LOCK, key.clone(), lock.to_bytes()),
            Modify::Put(
                CF_WRITE,
                key.clone().append_ts(20.into()),
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
            RecordedPessimisticAction::DoPessimisticCheck
        );
        assert_eq!(events[1].kind, TxnCommandEventKind::PutWrite);
        assert_eq!(events[1].commit_ts, 20);
        assert_eq!(events[1].write_type, Some(WriteType::Put));
    }

    #[test]
    fn test_event_size_is_bounded() {
        // 128 * 4096 records should remain below roughly 80 MiB, excluding the small
        // per-shard VecDeque bookkeeping.
        let size = size_of::<TxnCommandEvent>();
        assert!(size <= 160, "event size is {size}");
    }

    #[test]
    fn test_txn_status_cache_lookup_event() {
        let recorder = TxnCommandFlightRecorder::new(2, 4, true);
        let key = Key::from_raw(b"key");
        let mut ctx = Context::default();
        ctx.set_region_id(7);
        ctx.set_is_retry_request(true);

        recorder.record_txn_status_cache_lookup(
            [&key],
            b"key",
            10.into(),
            Some(20.into()),
            &ctx,
            Some(30),
        );

        let events = recorder.events_for_key(&key);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, TxnCommandEventKind::TxnStatusCacheHit);
        assert_eq!(events[0].txn_start_ts, 10);
        assert_eq!(events[0].commit_ts, 20);
        assert_eq!(events[0].snapshot_data_version, 30);
        assert!(events[0].is_retry_request);
    }

    #[test]
    fn test_disabled_recorder_does_not_record_and_enable_starts_fresh() {
        let recorder = TxnCommandFlightRecorder::new(2, 4, false);
        let key = Key::from_raw(b"key");

        recorder.record([event_for_key(&key, 1)]);
        assert!(recorder.events_for_key(&key).is_empty());

        recorder.set_enabled(true);
        recorder.record([event_for_key(&key, 2)]);
        assert_eq!(recorder.events_for_key(&key).len(), 1);

        recorder.set_enabled(false);
        assert!(recorder.events_for_key(&key).is_empty());
        recorder.set_enabled(true);
        assert!(recorder.events_for_key(&key).is_empty());
    }
}
