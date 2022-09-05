// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BinaryHeap, convert::TryFrom, num::NonZeroU64, result::Result, sync::Arc};

use dashmap;
use kvproto::kvrpcpb;
use smallvec::SmallVec;
use thiserror::Error;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    lock_manager::{lock_wait_context::LockWaitContextSharedState, LockManager, LockWaitToken},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
    txn::Error as TxnError,
    types::{PessimisticLockParameters, PessimisticLockRes},
    Error as StorageError, ErrorInner as StorageErrorInner,
};

#[derive(Debug, Error)]
#[error(transparent)]
pub struct SharedError(Arc<StorageErrorInner>);

impl From<StorageErrorInner> for SharedError {
    fn from(e: StorageErrorInner) -> Self {
        Self(Arc::new(e))
    }
}

impl From<StorageError> for SharedError {
    fn from(e: StorageError) -> Self {
        Self(Arc::from(e.0))
    }
}

impl TryFrom<SharedError> for StorageError {
    type Error = ();

    fn try_from(e: SharedError) -> Result<Self, Self::Error> {
        Arc::try_unwrap(e.0).map(Into::into).map_err(|_| ())
    }
}

pub type CallbackWithSharedError<T> = Box<dyn FnOnce(Result<T, SharedError>) + Send>;
pub type PessimisticLockKeyCallback = CallbackWithSharedError<PessimisticLockRes>;

pub struct LockWaitEntry {
    pub key: Key,
    pub lock_hash: u64,
    pub hash_for_latch: u64,
    pub term: Option<NonZeroU64>,
    pub parameters: PessimisticLockParameters,
    pub lock_wait_token: LockWaitToken,
    pub req_states: Option<Arc<LockWaitContextSharedState>>,
    pub current_legacy_wakeup_cnt: Option<usize>,
    pub key_cb: Option<PessimisticLockKeyCallback>,
}

#[repr(transparent)]
pub struct LockWaitEntryComparableWrapper(pub Box<LockWaitEntry>);

impl From<Box<LockWaitEntry>> for LockWaitEntryComparableWrapper {
    fn from(x: Box<LockWaitEntry>) -> Self {
        LockWaitEntryComparableWrapper(x)
    }
}

impl LockWaitEntryComparableWrapper {
    pub fn unwrap(self) -> Box<LockWaitEntry> {
        self.0
    }
}

impl PartialEq<Self> for LockWaitEntryComparableWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.parameters.start_ts == other.0.parameters.start_ts
            && self.0.current_legacy_wakeup_cnt == other.0.current_legacy_wakeup_cnt
    }
}

impl Eq for LockWaitEntryComparableWrapper {}

impl PartialOrd<Self> for LockWaitEntryComparableWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse it since the std BinaryHeap is max heap and we want to pop the
        // minimal.
        (
            other.0.current_legacy_wakeup_cnt,
            other.0.parameters.start_ts,
        )
            .partial_cmp(&(self.0.current_legacy_wakeup_cnt, self.0.parameters.start_ts))
    }
}

impl Ord for LockWaitEntryComparableWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse it since the std BinaryHeap is max heap and we want to pop the
        // minimal.
        (
            other.0.current_legacy_wakeup_cnt,
            other.0.parameters.start_ts,
        )
            .cmp(&(self.0.current_legacy_wakeup_cnt, self.0.parameters.start_ts))
    }
}

pub struct KeyLockWaitState {
    current_lock: kvrpcpb::LockInfo,
    legacy_wakeup_cnt: usize,
    queue: BinaryHeap<LockWaitEntryComparableWrapper>,
}

impl KeyLockWaitState {
    fn new(current_lock: kvrpcpb::LockInfo) -> Self {
        Self {
            current_lock,
            legacy_wakeup_cnt: 0,
            queue: BinaryHeap::new(),
        }
    }
}

pub struct LockWaitQueues<L: LockManager> {
    queue_map: Arc<dashmap::DashMap<Key, KeyLockWaitState>>,
    lock_mgr: L,
}

impl<L: LockManager> LockWaitQueues<L> {
    fn new(lock_mgr: L) -> Self {
        Self {
            queue_map: Arc::new(dashmap::DashMap::new()),
            lock_mgr,
        }
    }

    fn enqueue_lock_wait(
        &self,
        mut lock_wait_entry: Box<LockWaitEntry>,
        current_lock: kvrpcpb::LockInfo,
    ) {
        let mut entry = self
            .queue_map
            .entry(lock_wait_entry.key.clone())
            .or_insert_with(|| KeyLockWaitState::new(current_lock));
        if lock_wait_entry.current_legacy_wakeup_cnt.is_none() {
            lock_wait_entry.current_legacy_wakeup_cnt = Some(entry.value().legacy_wakeup_cnt);
        }
        entry.value_mut().queue.push(lock_wait_entry.into());
    }

    fn dequeue_lock_wait(&self, key: &Key) -> Option<Box<LockWaitEntry>> {
        let mut entry = self.queue_map.get_mut(key)?;

        loop {
            let result = entry.queue.pop();
            if entry.queue.is_empty() {
                self.queue_map.remove(key);
            }
            let result = result?.unwrap();
            if result.req_states.as_ref().unwrap().is_finished() {
                // Skip already cancelled entries.
                continue;
            }

            if !result.parameters.allow_lock_with_conflict {
                // If a pessimistic lock request in legacy mode is woken up, increase the
                // counter.
                entry.legacy_wakeup_cnt += 1
            }
            return Some(result);
        }
    }

    fn update_current_lock(&self, _key: &Key, _current_lock: kvrpcpb::LockInfo) {
        // Implementation of this function is required for supporting acquiring lock
        // after woken up.
        unimplemented!()
    }

    fn delayed_notify_all(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        legacy_wake_up_index: usize,
    ) -> Option<Box<LockWaitEntry>> {
        let mut popped_lock_wait_entries = SmallVec::<[_; 4]>::new();
        let mut entry = self.queue_map.get_mut(key)?;

        let mut woken_up_resumeable_entry = None;
        while let Some(front) = entry.queue.peek() {
            if front.0.req_states.as_ref().unwrap().is_finished() {
                // Skip already cancelled entries.
                entry.queue.pop();
                continue;
            }
            if front
                .0
                .current_legacy_wakeup_cnt
                .map_or(false, |cnt| cnt > legacy_wake_up_index)
            {
                break;
            }
            let lock_wait_entry = entry.queue.pop().unwrap().unwrap();
            if lock_wait_entry.parameters.allow_lock_with_conflict {
                woken_up_resumeable_entry = Some(lock_wait_entry);
                break;
            }
            popped_lock_wait_entries.push(lock_wait_entry);
        }

        // Release lock.

        drop(entry);

        // Call callbacks to cancel these entries here.
        // TODO: Perhaps we'better make it concurrent with scheduling the new command
        // (if `woken_up_resumeable_entry` is some) if there are too many.
        for lock_wait_entry in popped_lock_wait_entries {
            let lock_wait_entry = *lock_wait_entry;
            let cb = lock_wait_entry.key_cb.unwrap();
            let e = StorageError::from(TxnError::from(MvccError::from(
                MvccErrorInner::WriteConflict {
                    start_ts: lock_wait_entry.parameters.start_ts,
                    conflict_start_ts: conflicting_start_ts,
                    conflict_commit_ts: conflicting_commit_ts,
                    key: lock_wait_entry.key.into_raw().unwrap(),
                    primary: lock_wait_entry.parameters.primary,
                },
            )));
            cb(Err(e.into()));
        }

        // Return the item to be woken up in resumeable way.
        woken_up_resumeable_entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {}
}
