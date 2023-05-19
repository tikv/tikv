// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_kv::{Snapshot, SEEK_BOUND};
use txn_types::{Key, TimeStamp, Write, WriteType};

use crate::storage::mvcc::{Result, SnapshotReader};

/// Returns the new `last_change_ts` and `versions_to_last_change` according
/// to this write record. If it is unknown from the given write, try iterate to
/// the last change and find the answer.
pub fn next_last_change_info<S: Snapshot>(
    key: &Key,
    write: &Write,
    start_ts: TimeStamp,
    reader: &SnapshotReader<S>,
    commit_ts: TimeStamp,
) -> Result<(TimeStamp, u64)> {
    match write.write_type {
        WriteType::Put | WriteType::Delete => Ok((commit_ts, 1)),
        WriteType::Lock | WriteType::Rollback => {
            if !write.last_change_ts.is_zero() || write.versions_to_last_change != 0 {
                Ok((write.last_change_ts, write.versions_to_last_change + 1))
            } else {
                // If neither `last_change_ts` nor `versions_to_last_change` exists, it means we
                // do not know the last change info, probably because it comes from an older
                // version TiKV. To support data from old TiKV, we iterate to the last change to
                // find it.

                // TODO: can we reuse the reader?
                let snapshot = reader.reader.snapshot().clone();
                let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                match reader.get_write_with_commit_ts(key, commit_ts)? {
                    // last_change_ts == 0 && versions_to_last_change > 0 means the key does not
                    // exist.
                    None => Ok((TimeStamp::zero(), 1)),
                    Some((w, last_change_ts)) => {
                        assert!(matches!(w.write_type, WriteType::Put | WriteType::Delete));
                        // we don't know how many versions there are, make `versions_to_last_change`
                        // big enough so that later reads won't try to `next` to it.
                        Ok((last_change_ts, SEEK_BOUND + 1))
                    }
                }
            }
        }
    }
}
