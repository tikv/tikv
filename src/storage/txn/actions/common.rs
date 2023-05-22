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
    original_reader: &mut SnapshotReader<S>,
    commit_ts: TimeStamp,
) -> Result<(TimeStamp, u64)> {
    match write.write_type {
        WriteType::Put | WriteType::Delete => Ok((commit_ts, 1)),
        WriteType::Lock | WriteType::Rollback => {
            assert!(write.last_change_ts.is_zero() || write.versions_to_last_change > 0);
            if !write.last_change_ts.is_zero() || write.versions_to_last_change != 0 {
                Ok((write.last_change_ts, write.versions_to_last_change + 1))
            } else {
                // If neither `last_change_ts` nor `versions_to_last_change` exists, it means we
                // do not know the last change info, probably because it comes from an older
                // version TiKV. To support data from old TiKV, we iterate to the last change to
                // find it.

                // TODO: can we reuse the reader?
                let snapshot = original_reader.reader.snapshot().clone();
                let mut reader = SnapshotReader::new(start_ts, snapshot, true);

                // Note that the scan can also utilize `last_change`. So once it finds a LOCK
                // version with useful `last_change` pointer, it just needs one more `seek` or
                // several `next`s to get to the final result.
                let res = reader.get_write_with_commit_ts(key, commit_ts);
                let stat = reader.take_statistics();
                original_reader.reader.statistics.add(&stat);
                match res? {
                    // last_change_ts == 0 && versions_to_last_change > 0 means the key does not
                    // exist.
                    None => Ok((TimeStamp::zero(), 1)),
                    Some((w, last_change_ts)) => {
                        assert!(matches!(w.write_type, WriteType::Put));
                        // We don't know how many versions there are. Make `versions_to_last_change`
                        // big enough so that later reads won't try to `next` to it.
                        Ok((last_change_ts, SEEK_BOUND + 1))
                    }
                }
            }
        }
    }
}
