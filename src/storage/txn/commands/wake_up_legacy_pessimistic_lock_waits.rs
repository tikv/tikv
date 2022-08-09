// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, ReleasedLock},
    txn::{
        commands::{CommandExt, ReleasedLocks, SyncCommand, SyncCommandContext},
        Error as TxnError,
    },
    Error as StorageError,
};

command! {
    /// Wake up as many legacy pessimistic lock that's waiting on the specified key as possible.
    WakeUpLegacyPessimisticLockWaits:
        cmd_ty => (),
        display => "kv::command::wake_up_legacy_pessimistic_lock_wait key({}) {:?}", (key, ctx),
        content => {
            key: Key,
            conflicting_start_ts: TimeStamp,
            conflicting_commit_ts: TimeStamp,
            wake_up_before: Instant,
        }
}

impl CommandExt for WakeUpLegacyPessimisticLockWaits {
    ctx!();
    tag!(wake_up_legacy_pessimistic_lock_wait);
    gen_lock!(key);
    property!(is_sys_cmd);
    property!(is_sync_cmd);

    fn write_bytes(&self) -> usize {
        0
    }
}

impl SyncCommand for WakeUpLegacyPessimisticLockWaits {
    fn process_sync(self, sync_cmd_ctx: SyncCommandContext<'_>) -> Option<ReleasedLocks> {
        assert_eq!(sync_cmd_ctx.latch.required_hashes.len(), 1);
        let hash = sync_cmd_ctx.latch.required_hashes[0];
        let queue_map = sync_cmd_ctx.latch.lock_wait_queues.get_mut(&hash)?;
        let queue = queue_map.get_mut(&self.key)?;

        let mut popped_entries = vec![];
        let mut released_locks = ReleasedLocks::new();

        while let Some(entry) = queue.peek() {
            if entry
                .0
                .req_states
                .as_ref()
                .unwrap()
                .finished
                .load(Ordering::Acquire)
            {
                info!("expired lock wait entry dropped";
                    "start_ts" => entry.0.parameters.start_ts, "for_update_ts" => entry.0.parameters.for_update_ts, "key" => %entry.0.key,
                    "is_new_mode" => entry.0.allow_lock_with_conflict);
                queue.pop();
                continue;
            }

            if !entry.0.allow_lock_with_conflict
                && entry
                    .0
                    .wait_start_time
                    .map_or(true, |t| t <= self.wake_up_before)
            {
                popped_entries.push(queue.pop().unwrap());
                continue;
            }

            info!("trying to wake up lock in normal way for late arrived or new mode request";
                    "start_ts" => entry.0.parameters.start_ts, "for_update_ts" => entry.0.parameters.for_update_ts, "key" => %entry.0.key,
                    "is_new_mode" => entry.0.allow_lock_with_conflict, "conflict_start_ts" => self.conflicting_start_ts, "conflict_commit_ts" => self.conflicting_commit_ts);
            // If we found an waiting request in new mode, or in old mode but inserted later
            // than registering the waking up, wake it up in normal way and stop.
            released_locks.push(Some(ReleasedLock::new(
                self.conflicting_start_ts,
                if self.conflicting_commit_ts.is_zero() {
                    None
                } else {
                    Some(self.conflicting_commit_ts)
                },
                self.key,
                false,
            )));
            break;
        }

        // Make borrow checker happy.
        let conflicting_start_ts = self.conflicting_start_ts;
        let conflicting_commit_ts = self.conflicting_commit_ts;

        if !popped_entries.is_empty() {
            *sync_cmd_ctx.on_finished = Some(Box::new(move || {
                for entry in popped_entries {
                    let entry = entry.unwrap();
                    let cb = entry.key_cb.unwrap();
                    let e = StorageError::from(TxnError::from(MvccError::from(
                        MvccErrorInner::WriteConflict {
                            start_ts: entry.parameters.start_ts,
                            conflict_start_ts: conflicting_start_ts,
                            conflict_commit_ts: conflicting_commit_ts,
                            key: entry.key.to_raw().unwrap(),
                            primary: entry.parameters.primary,
                        },
                    )));
                    info!("reporting write conflict after wake-up-delay-duration";
                        "start_ts" => entry.parameters.start_ts, "for_update_ts" => entry.parameters.for_update_ts, "key" => %entry.key,
                        "is_new_mode" => entry.allow_lock_with_conflict, "err" => ?e);
                    cb(Err(Arc::new(e)));
                }
            }));
        }

        if released_locks.is_empty() {
            None
        } else {
            Some(released_locks)
        }
    }
}
