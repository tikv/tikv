// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_kv::Snapshot;
use txn_types::{Key, LastChange, OldValue, TimeStamp, Write, WriteType};

use crate::storage::mvcc::{MvccTxn, Result, SnapshotReader, TxnCommitRecord};

/// Returns the new `LastChange` according to this write record. If it is
/// unknown from the given write, try iterate to the last change and find the
/// answer.
pub fn next_last_change_info<S: Snapshot>(
    key: &Key,
    write: &Write,
    start_ts: TimeStamp,
    original_reader: &mut SnapshotReader<S>,
    commit_ts: TimeStamp,
) -> Result<LastChange> {
    match write.write_type {
        WriteType::Put | WriteType::Delete => Ok(LastChange::make_exist(commit_ts, 1)),
        WriteType::Lock | WriteType::Rollback => {
            match &write.last_change {
                LastChange::Exist {
                    last_change_ts,
                    estimated_versions_to_last_change,
                } => Ok(LastChange::make_exist(
                    *last_change_ts,
                    estimated_versions_to_last_change + 1,
                )),
                LastChange::NotExist => Ok(LastChange::NotExist),
                LastChange::Unknown => {
                    fail_point!("before_get_write_in_next_last_change_info");
                    // We do not know the last change info, probably
                    // because it comes from an older version TiKV. To support data
                    // from old TiKV, we iterate to the last change to find it.

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
                        // last_change_ts == 0 && estimated_versions_to_last_change > 0 means the
                        // key does not exist.
                        None => Ok(LastChange::NotExist),
                        Some((w, last_change_ts)) => {
                            assert!(matches!(w.write_type, WriteType::Put));
                            Ok(LastChange::make_exist(
                                last_change_ts,
                                stat.write.next as u64 + 1,
                            ))
                        }
                    }
                }
            }
        }
    }
}

// Further check whether the prewritten transaction has been committed
// when encountering a WriteConflict or PessimisticLockNotFound error.
// This extra check manages to make prewrite idempotent after the transaction
// was committed.
// Note that this check cannot fully guarantee idempotence because an MVCC
// GC can remove the old committed records, then we cannot determine
// whether the transaction has been committed, so the error is still returned.
pub fn check_committed_record_on_err(
    prewrite_result: crate::storage::mvcc::Result<(TimeStamp, OldValue)>,
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key: &Key,
) -> crate::storage::txn::Result<(
    Vec<std::result::Result<(), crate::storage::errors::Error>>,
    TimeStamp,
)> {
    match reader.get_txn_commit_record(key)? {
        TxnCommitRecord::SingleRecord { commit_ts, write }
            if write.write_type != WriteType::Rollback =>
        {
            info!("prewritten transaction has been committed";
                        "start_ts" => reader.start_ts, "commit_ts" => commit_ts,
                        "key" => ?key, "write_type" => ?write.write_type);
            txn.clear();
            Ok((vec![], commit_ts))
        }
        _ => Err(prewrite_result.unwrap_err().into()),
    }
}
