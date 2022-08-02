// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::ReleasedLock;
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::storage::txn::commands::{CommandExt, ReleasedLocks, SyncCommand, SyncCommandContext};
use crate::storage::txn::Error as TxnError;
use crate::storage::Error as StorageError;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use txn_types::Key;

command! {
    /// Wake up as many legacy pessimistic lock that's waiting on the specified key as possible.
    WakeUpLegacyPessimisticLockWaits:
        cmd_ty => (),
        display => "kv::command::wake_up_legacy_pessimistic_lock_wait key({}) {:?}", (key, ctx),
        content => {
            key: Key,
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
                queue.pop();
                continue;
            }

            if !entry.0.allow_lock_with_conflict {
                popped_entries.push(queue.pop().unwrap());
                continue;
            }

            // If we found an waiting request in new mode, wake it up and stop.
            released_locks.push(Some(ReleasedLock::new(0.into(), None, self.key, false)));
            break;
        }

        if !popped_entries.is_empty() {
            *sync_cmd_ctx.on_finished = Some(Box::new(move || {
                for entry in popped_entries {
                    let entry = entry.unwrap();
                    let cb = entry.key_cb.unwrap();
                    let e = StorageError::from(TxnError::from(MvccError::from(
                        MvccErrorInner::KeyIsLocked(entry.last_found_lock),
                    )));
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
