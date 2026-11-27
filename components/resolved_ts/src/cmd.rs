// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{Debug, Formatter};

use collections::HashMap;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::{
    errorpb,
    raft_cmdpb::{AdminCmdType, CmdType, Request},
};
use raftstore::coprocessor::{Cmd, CmdBatch, ObserveLevel};
use tikv_util::Either;
use txn_types::{
    Key, Lock, LockType, TimeStamp, Value, Write, WriteBatchFlags, WriteRef, WriteType,
};

#[derive(Debug, PartialEq)]
pub enum ChangeRow {
    Prewrite {
        key: Key,
        start_ts: TimeStamp,
        lock_type: LockType,
        value: Option<Value>,
        generation: u64,
    },
    Commit {
        key: Key,
        write_type: WriteType,
        // In some cases a rollback will be done by just deleting the lock, need to lookup Resolver
        // to untrack lock and get start ts from it.
        start_ts: Option<TimeStamp>,
        commit_ts: Option<TimeStamp>,
    },
    OnePc {
        key: Key,
        write_type: WriteType,
        commit_ts: TimeStamp,
        value: Option<Value>,
    },
    IngestSsT,
}

#[allow(clippy::large_enum_variant)]
pub enum ChangeLog {
    Error(errorpb::Error),
    Rows { index: u64, rows: Vec<ChangeRow> },
    Admin(AdminCmdType),
}

impl ChangeLog {
    pub fn encode_change_log(region_id: u64, batch: CmdBatch) -> Vec<ChangeLog> {
        batch
            .into_iter(region_id)
            .map(|cmd| {
                let Cmd {
                    index,
                    term: _,
                    mut request,
                    mut response,
                } = cmd;
                if !response.get_header().has_error() {
                    if !request.has_admin_request() {
                        let flags =
                            WriteBatchFlags::from_bits_truncate(request.get_header().get_flags());
                        let is_one_pc = flags.contains(WriteBatchFlags::ONE_PC);
                        let requests: Vec<Request> = request.requests.into();
                        let (changes, has_ingest_sst) = group_row_changes(requests);
                        let mut rows = Self::encode_rows(changes, is_one_pc);
                        if has_ingest_sst {
                            rows.push(ChangeRow::IngestSsT);
                        }
                        ChangeLog::Rows { index, rows }
                    } else {
                        ChangeLog::Admin(request.take_admin_request().get_cmd_type())
                    }
                } else {
                    let err_header = response.mut_header().take_error();
                    ChangeLog::Error(err_header)
                }
            })
            .collect()
    }

    fn encode_rows(changes: HashMap<Key, RowChange>, is_one_pc: bool) -> Vec<ChangeRow> {
        let mut out = Vec::with_capacity(changes.len());
        for (key, row) in changes {
            let RowChange {
                writes,
                lock,
                mut default,
            } = row;
            if !writes.is_empty() {
                let mut emitted_write = false;
                let mut filtered_write = false;
                for write_op in writes {
                    let (mut commit_ts, write) = write_op.into_put();
                    if let Some(write) = decode_write(key.as_encoded(), &write, true) {
                        emitted_write = true;
                        let Write {
                            short_value,
                            start_ts,
                            write_type,
                            ..
                        } = write;
                        let value = default.take().map_or(short_value, |v| Some(v.into_put().1));
                        if is_one_pc {
                            out.push(ChangeRow::OnePc {
                                commit_ts: commit_ts.unwrap_or_default(),
                                write_type,
                                value,
                                key: key.clone(),
                            });
                        } else {
                            if write.write_type == WriteType::Rollback {
                                commit_ts = None;
                            }
                            out.push(ChangeRow::Commit {
                                key: key.clone(),
                                commit_ts,
                                start_ts: Some(start_ts),
                                write_type,
                            });
                        }
                    } else {
                        filtered_write = true;
                    }
                }
                match lock {
                    Some(KeyOp::Put(_, lock_val)) => {
                        if let Some(lock) = decode_lock(key.as_encoded(), &lock_val) {
                            let Lock {
                                short_value,
                                ts,
                                lock_type,
                                generation,
                                ..
                            } = lock;
                            let value =
                                default.take().map_or(short_value, |v| Some(v.into_put().1));
                            out.push(ChangeRow::Prewrite {
                                key,
                                start_ts: ts,
                                lock_type,
                                value,
                                generation,
                            });
                        }
                    }
                    Some(KeyOp::Delete) if !is_one_pc && (!emitted_write || filtered_write) => {
                        // A final CF_LOCK delete guarantees no physical lock remains on this
                        // key. When a write record was filtered out (such as an overlapped
                        // rollback rewriting an older write with gc_fence, or a lock-only
                        // commit), emit an authoritative unlock so the resolver does not
                        // keep a stale lock pinned.
                        out.push(ChangeRow::Commit {
                            key,
                            commit_ts: None,
                            start_ts: None,
                            write_type: WriteType::Rollback,
                        });
                    }
                    _ => {}
                }
                continue;
            }
            match (lock, default) {
                (Some(KeyOp::Put(_, lock)), default) => {
                    if let Some(lock) = decode_lock(key.as_encoded(), &lock) {
                        let Lock {
                            short_value,
                            ts,
                            lock_type,
                            generation,
                            ..
                        } = lock;
                        let value = default.map_or(short_value, |v| Some(v.into_put().1));
                        out.push(ChangeRow::Prewrite {
                            key,
                            start_ts: ts,
                            lock_type,
                            value,
                            generation,
                        });
                    }
                }
                (Some(KeyOp::Delete), _) => out.push(ChangeRow::Commit {
                    key,
                    commit_ts: None,
                    start_ts: None,
                    write_type: WriteType::Rollback,
                }),
                other => panic!("unexpected row pattern {:?}", other),
            }
        }
        out
    }
}

pub(crate) fn decode_write(key: &[u8], value: &[u8], is_apply: bool) -> Option<Write> {
    let write = WriteRef::parse(value).ok()?.to_owned();
    // Drop the record it self but keep only the overlapped rollback information if
    // gc_fence exists.
    if is_apply && write.gc_fence.is_some() {
        // `gc_fence` is set means the write record has been rewritten.
        // Currently the only case is writing overlapped_rollback. And in this case we
        // can safely ignore the writing operation.
        assert!(write.has_overlapped_rollback);
        assert_ne!(write.write_type, WriteType::Rollback);
        return None;
    }
    match write.write_type {
        WriteType::Put | WriteType::Delete | WriteType::Rollback => Some(write),
        other => {
            debug!("skip write record"; "write" => ?other, "key" => log_wrappers::Value(key));
            None
        }
    }
}

#[cfg_attr(not(debug_assertions), allow(unused_variables))]
pub(crate) fn decode_lock(key: &[u8], value: &[u8]) -> Option<Lock> {
    match txn_types::decode_lock_type(value).ok()? {
        LockType::Put | LockType::Delete => match txn_types::parse_lock(value).ok()? {
            Either::Left(lock) => Some(lock),
            Either::Right(..) => unreachable!("lock type Put/Delete cannot be SharedLocks"),
        },
        _ => {
            #[cfg(debug_assertions)]
            {
                match txn_types::parse_lock(value).ok()? {
                    Either::Left(lock) => {
                        debug!("skip lock record";
                            "type" => ?lock.lock_type,
                            "start_ts" => ?lock.ts,
                            "key" => log_wrappers::Value(key),
                            "for_update_ts" => ?lock.for_update_ts);
                    }
                    Either::Right(shared_locks) => {
                        debug!("skip shared locks record";
                            "key" => log_wrappers::Value(key),
                            "len" => shared_locks.len());
                    }
                };
            }
            None
        }
    }
}

enum KeyOp {
    Put(Option<TimeStamp>, Vec<u8>),
    Delete,
}

impl Debug for KeyOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyOp::Put(ts, value) => {
                write!(
                    f,
                    "Put(ts:{:?}, value:{:?})",
                    ts,
                    log_wrappers::Value(value)
                )
            }
            KeyOp::Delete => write!(f, "Delete"),
        }
    }
}

impl KeyOp {
    fn into_put(self) -> (Option<TimeStamp>, Vec<u8>) {
        match self {
            KeyOp::Put(ts, value) => (ts, value),
            KeyOp::Delete => unreachable!(),
        }
    }
}

#[derive(Default, Debug)]
struct RowChange {
    writes: Vec<KeyOp>,
    lock: Option<KeyOp>,
    default: Option<KeyOp>,
}

fn group_row_changes(requests: Vec<Request>) -> (HashMap<Key, RowChange>, bool) {
    let mut changes: HashMap<Key, RowChange> = HashMap::default();
    // The changes about default cf was recorded here and need to be matched with a
    // `write` or a `lock`.
    let mut unmatched_default = HashMap::default();
    let mut has_ingest_sst = false;
    for mut req in requests {
        match req.get_cmd_type() {
            CmdType::IngestSst => {
                has_ingest_sst = true;
            }
            CmdType::Put => {
                let mut put = req.take_put();
                let key = Key::from_encoded(put.take_key());
                let value = put.take_value();
                match put.cf.as_str() {
                    CF_WRITE => {
                        if let Ok(ts) = key.decode_ts() {
                            let key = key.truncate_ts().unwrap();
                            let row = changes.entry(key).or_default();
                            row.writes.push(KeyOp::Put(Some(ts), value));
                        }
                    }
                    CF_LOCK => {
                        // A single Raft batch (such as batched ResolveLock over a shared
                        // wrapper) can update the same CF_LOCK key multiple times; keep the
                        // final physical lock state.
                        let row = changes.entry(key).or_default();
                        row.lock = Some(KeyOp::Put(None, value));
                    }
                    "" | CF_DEFAULT => {
                        if let Ok(ts) = key.decode_ts() {
                            let key = key.truncate_ts().unwrap();
                            unmatched_default.insert(key, KeyOp::Put(Some(ts), value));
                        }
                    }
                    other => {
                        debug!("resolved ts invalid cf {}", other);
                    }
                }
            }
            CmdType::Delete => {
                let mut delete = req.take_delete();
                match delete.cf.as_str() {
                    CF_LOCK => {
                        let key = Key::from_encoded(delete.take_key());
                        let row = changes.entry(key).or_default();
                        row.lock = Some(KeyOp::Delete);
                    }
                    "" | CF_WRITE | CF_DEFAULT => {}
                    other => {
                        debug!("resolved ts invalid cf {}", other);
                    }
                }
            }
            _ => {
                debug!(
                    "skip other command";
                    "command" => ?req,
                );
            }
        }
    }
    for (key, default) in unmatched_default {
        if let Some(row) = changes.get_mut(&key) {
            row.default = Some(default);
        }
    }
    (changes, has_ingest_sst)
}

/// Filter non-lock related data (i.e `default_cf` data), the implement is
/// subject to how `group_row_changes` and `encode_rows` encode `ChangeRow`
pub fn lock_only_filter(mut cmd_batch: CmdBatch) -> Option<CmdBatch> {
    if cmd_batch.is_empty() {
        return None;
    }
    match cmd_batch.level {
        ObserveLevel::None => None,
        ObserveLevel::All => Some(cmd_batch),
        ObserveLevel::LockRelated => {
            for cmd in &mut cmd_batch.cmds {
                let mut requests = cmd.request.take_requests().into_vec();
                requests.retain(|req| {
                    let cf = match req.get_cmd_type() {
                        CmdType::Put => req.get_put().cf.as_str(),
                        CmdType::Delete => req.get_delete().cf.as_str(),
                        _ => "",
                    };
                    cf == CF_LOCK || cf == CF_WRITE || req.get_cmd_type() == CmdType::IngestSst
                });
                cmd.request.set_requests(requests.into());
            }
            Some(cmd_batch)
        }
    }
}

#[cfg(test)]
mod tests {
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::{CF_LOCK, CF_WRITE};
    use kvproto::{
        kvrpcpb::{AssertionLevel, PrewriteRequestPessimisticAction::*},
        raft_cmdpb::{CmdType, Request},
    };
    use tikv::storage::{
        Engine,
        kv::{MockEngineBuilder, TestEngineBuilder},
        mvcc::{Mutation, MvccTxn, SnapshotReader, tests::write},
        txn::{
            CommitKind, TransactionKind, TransactionProperties, commands::one_pc_commit, prewrite,
            tests::*,
        },
    };
    use tikv_kv::Modify;
    use txn_types::{Key, LockType, WriteType};

    use super::{ChangeLog, ChangeRow, group_row_changes};

    #[test]
    fn test_cmd_encode() {
        let rocks_engine = TestEngineBuilder::new().build().unwrap();
        let mut engine = MockEngineBuilder::from_rocks_engine(rocks_engine).build();

        let mut reqs = vec![Modify::Put("default", Key::from_raw(b"k1"), b"v1".to_vec()).into()];
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        reqs.push(req);
        let (changes, has_ingest_sst) = group_row_changes(reqs);
        assert_eq!(has_ingest_sst, true);
        assert!(ChangeLog::encode_rows(changes, false).is_empty());

        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 1);
        must_commit(&mut engine, b"k1", 1, 2);

        must_prewrite_put(&mut engine, b"k1", b"v2", b"k1", 3);
        must_rollback(&mut engine, b"k1", 3, false);

        must_prewrite_put(&mut engine, b"k1", &[b'v'; 512], b"k1", 4);
        must_commit(&mut engine, b"k1", 4, 5);

        must_prewrite_put(&mut engine, b"k1", b"v3", b"pk", 5);
        must_rollback(&mut engine, b"k1", 5, false);

        must_prewrite_put(&mut engine, b"k1", b"v4", b"k1", 6);
        must_commit(&mut engine, b"k1", 6, 7);

        must_prewrite_put(&mut engine, b"k1", b"v5", b"k1", 7);
        must_rollback(&mut engine, b"k1", 7, true);

        let k1 = Key::from_raw(b"k1");
        let rows: Vec<_> = engine
            .take_last_modifies()
            .into_iter()
            .flat_map(|m| {
                let reqs: Vec<Request> = m.into_iter().map(Into::into).collect();
                let (changes, has_ingest_sst) = group_row_changes(reqs);
                assert_eq!(has_ingest_sst, false);
                ChangeLog::encode_rows(changes, false)
            })
            .collect();

        let expected = vec![
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 1.into(),
                value: Some(b"v1".to_vec()),
                lock_type: LockType::Put,
                generation: 0,
            },
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: Some(1.into()),
                commit_ts: Some(2.into()),
                write_type: WriteType::Put,
            },
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 3.into(),
                value: Some(b"v2".to_vec()),
                lock_type: LockType::Put,
                generation: 0,
            },
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: Some(3.into()),
                commit_ts: None,
                write_type: WriteType::Rollback,
            },
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 4.into(),
                value: Some(vec![b'v'; 512]),
                lock_type: LockType::Put,
                generation: 0,
            },
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: Some(4.into()),
                commit_ts: Some(5.into()),
                write_type: WriteType::Put,
            },
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 5.into(),
                value: Some(b"v3".to_vec()),
                lock_type: LockType::Put,
                generation: 0,
            },
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: None,
                commit_ts: None,
                write_type: WriteType::Rollback,
            },
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 6.into(),
                value: Some(b"v4".to_vec()),
                lock_type: LockType::Put,
                generation: 0,
            },
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: Some(6.into()),
                commit_ts: Some(7.into()),
                write_type: WriteType::Put,
            },
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 7.into(),
                value: Some(b"v5".to_vec()),
                lock_type: LockType::Put,
                generation: 0,
            },
            // Overlapped rollback deletes CF_LOCK even though the rewritten Write CF
            // record is filtered, so emit an authoritative unlock row.
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: None,
                commit_ts: None,
                write_type: WriteType::Rollback,
            },
        ];
        assert_eq!(rows, expected);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new_for_test(42.into());
        let mut txn = MvccTxn::new(10.into(), cm);
        let mut reader = SnapshotReader::new(10.into(), snapshot, true);
        prewrite(
            &mut txn,
            &mut reader,
            &TransactionProperties {
                start_ts: 10.into(),
                kind: TransactionKind::Optimistic(false),
                commit_kind: CommitKind::OnePc(50.into()),
                primary: b"k1",
                txn_size: 2,
                lock_ttl: 2000,
                min_commit_ts: 10.into(),
                need_old_value: false,
                is_retry_request: false,
                assertion_level: AssertionLevel::Off,
                txn_source: 0,
            },
            Mutation::make_put(k1.clone(), b"v4".to_vec()),
            &None,
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        one_pc_commit(true, &mut txn, 10.into());
        write(&engine, &Default::default(), txn.into_modifies());
        let one_pc_row = engine
            .take_last_modifies()
            .into_iter()
            .flat_map(|m| {
                let reqs = m.into_iter().map(Into::into).collect();
                let (changes, has_ingest_sst) = group_row_changes(reqs);
                assert_eq!(has_ingest_sst, false);
                ChangeLog::encode_rows(changes, true)
            })
            .last()
            .unwrap();
        assert_eq!(
            one_pc_row,
            ChangeRow::OnePc {
                key: k1,
                write_type: WriteType::Put,
                commit_ts: 10.into(),
                value: Some(b"v4".to_vec()),
            }
        )
    }

    #[test]
    fn test_cmd_encode_batched_shared_member_resolutions() {
        let key = Key::from_raw(b"shared-key");
        let w1 = txn_types::Write::new_rollback(10.into(), true);
        let w2 = txn_types::Write::new_rollback(20.into(), true);
        let mut remaining = txn_types::SharedLocks::new();
        remaining
            .insert_lock(txn_types::Lock::new(
                LockType::Pessimistic,
                b"shared-key".to_vec(),
                30.into(),
                1000,
                None,
                30.into(),
                1,
                0.into(),
                false,
            ))
            .unwrap();
        let remaining_bytes = remaining.to_bytes();

        // Case 1: two shared rollbacks + two intermediate CF_LOCK updates with a
        // surviving holder.
        let reqs_surviving: Vec<Request> = vec![
            Modify::Put(
                CF_WRITE,
                key.clone().append_ts(10.into()),
                w1.as_ref().to_bytes(),
            )
            .into(),
            Modify::Put(CF_LOCK, key.clone(), remaining_bytes.clone()).into(),
            Modify::Put(
                CF_WRITE,
                key.clone().append_ts(20.into()),
                w2.as_ref().to_bytes(),
            )
            .into(),
            Modify::Put(CF_LOCK, key.clone(), remaining_bytes).into(),
        ];
        let (changes, has_ingest_sst) = group_row_changes(reqs_surviving);
        assert!(!has_ingest_sst);
        let rows = ChangeLog::encode_rows(changes, false);
        assert_eq!(
            rows,
            vec![
                ChangeRow::Commit {
                    key: key.clone(),
                    write_type: WriteType::Rollback,
                    start_ts: Some(10.into()),
                    commit_ts: None,
                },
                ChangeRow::Commit {
                    key: key.clone(),
                    write_type: WriteType::Rollback,
                    start_ts: Some(20.into()),
                    commit_ts: None,
                },
            ]
        );

        // Case 2: two shared rollbacks + intermediate CF_LOCK put + final CF_LOCK
        // delete.
        let reqs_deleted: Vec<Request> = vec![
            Modify::Put(
                CF_WRITE,
                key.clone().append_ts(10.into()),
                w1.as_ref().to_bytes(),
            )
            .into(),
            Modify::Put(CF_LOCK, key.clone(), remaining.to_bytes()).into(),
            Modify::Put(
                CF_WRITE,
                key.clone().append_ts(20.into()),
                w2.as_ref().to_bytes(),
            )
            .into(),
            Modify::Delete(CF_LOCK, key.clone()).into(),
        ];
        let (changes, _) = group_row_changes(reqs_deleted);
        let rows = ChangeLog::encode_rows(changes, false);
        assert_eq!(
            rows,
            vec![
                ChangeRow::Commit {
                    key: key.clone(),
                    write_type: WriteType::Rollback,
                    start_ts: Some(10.into()),
                    commit_ts: None,
                },
                ChangeRow::Commit {
                    key,
                    write_type: WriteType::Rollback,
                    start_ts: Some(20.into()),
                    commit_ts: None,
                },
            ]
        );
    }

    #[test]
    fn test_overlapped_rollback_untracks_and_unrelated_rollback_pins_resolver() {
        use std::sync::Arc;

        use tikv::storage::txn::txn_status_cache::TxnStatusCache;
        use tikv_util::memory::MemoryQuota;
        use txn_types::TimeStamp;

        use crate::{TsSource, resolver::Resolver};

        let rocks_engine = TestEngineBuilder::new().build().unwrap();
        let mut engine = MockEngineBuilder::from_rocks_engine(rocks_engine).build();
        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let txn_status_cache = Arc::new(TxnStatusCache::new(100));
        let mut resolver = Resolver::new(1, memory_quota, txn_status_cache);
        let mut apply_index = 0u64;

        let apply_batches =
            |batches: Vec<Vec<Modify>>, resolver: &mut Resolver, apply_index: &mut u64| {
                for batch in batches {
                    *apply_index += 1;
                    let reqs: Vec<Request> = batch.into_iter().map(Into::into).collect();
                    let (changes, _) = group_row_changes(reqs);
                    for row in ChangeLog::encode_rows(changes, false) {
                        match row {
                            ChangeRow::Prewrite {
                                key,
                                start_ts,
                                generation,
                                ..
                            } => {
                                resolver
                                    .track_lock(
                                        start_ts,
                                        key.to_raw().unwrap(),
                                        Some(*apply_index),
                                        generation,
                                    )
                                    .unwrap();
                            }
                            ChangeRow::Commit { key, start_ts, .. } => {
                                resolver.untrack_lock_with_start_ts(
                                    &key.to_raw().unwrap(),
                                    start_ts,
                                    Some(*apply_index),
                                );
                            }
                            _ => {}
                        }
                    }
                }
            };

        // 1. Protected overlapped rollback deletes CF_LOCK and must advance
        //    resolved-ts, and subsequent transactions on the same key must not stay
        //    pinned at 7.
        must_prewrite_put(&mut engine, b"k1", b"v4", b"k1", 6);
        must_commit(&mut engine, b"k1", 6, 7);
        apply_batches(engine.take_last_modifies(), &mut resolver, &mut apply_index);
        assert_eq!(resolver.resolve(6.into(), None, TsSource::PdTso), 6.into());

        must_prewrite_put(&mut engine, b"k1", b"v5", b"k1", 7);
        apply_batches(engine.take_last_modifies(), &mut resolver, &mut apply_index);
        assert_eq!(resolver.resolve(50.into(), None, TsSource::PdTso), 7.into());

        must_rollback(&mut engine, b"k1", 7, true);
        apply_batches(engine.take_last_modifies(), &mut resolver, &mut apply_index);
        assert_eq!(resolver.resolve(9.into(), None, TsSource::PdTso), 9.into());

        must_prewrite_put(&mut engine, b"k1", b"v6", b"k1", 10);
        apply_batches(engine.take_last_modifies(), &mut resolver, &mut apply_index);
        assert_eq!(
            resolver.resolve(50.into(), None, TsSource::PdTso),
            10.into()
        );

        must_commit(&mut engine, b"k1", 10, 11);
        apply_batches(engine.take_last_modifies(), &mut resolver, &mut apply_index);
        assert_eq!(
            resolver.resolve(50.into(), None, TsSource::PdTso),
            50.into()
        );

        // 2. Rollback by another transaction that leaves k2's active data lock in place
        //    (no CF_LOCK delete) must keep resolved-ts pinned at the active owner's
        //    start_ts.
        let mut resolver2 = Resolver::new(
            2,
            Arc::new(MemoryQuota::new(usize::MAX)),
            Arc::new(TxnStatusCache::new(100)),
        );
        let mut apply_index2 = 0u64;
        must_prewrite_put(&mut engine, b"k2", b"v1", b"k2", 20);
        apply_batches(
            engine.take_last_modifies(),
            &mut resolver2,
            &mut apply_index2,
        );
        assert_eq!(
            resolver2.resolve(50.into(), None, TsSource::PdTso),
            20.into()
        );

        // Unrelated rollback (start_ts=15) writes CF_WRITE rollback without deleting
        // CF_LOCK.
        must_rollback(&mut engine, b"k2", 15, false);
        apply_batches(
            engine.take_last_modifies(),
            &mut resolver2,
            &mut apply_index2,
        );
        assert_eq!(
            resolver2.resolve(50.into(), None, TsSource::PdTso),
            20.into()
        );

        // Also ensure a filtered overlapped write without CF_LOCK delete keeps k2
        // pinned.
        let k2 = Key::from_raw(b"k2");
        let overlapped =
            txn_types::Write::new(WriteType::Put, TimeStamp::new(12), Some(b"old".to_vec()))
                .set_overlapped_rollback(true, Some(TimeStamp::new(0)));
        let (changes, _) = group_row_changes(vec![
            Modify::Put(
                CF_WRITE,
                k2.clone().append_ts(15.into()),
                overlapped.as_ref().to_bytes(),
            )
            .into(),
        ]);
        assert!(ChangeLog::encode_rows(changes, false).is_empty());
        assert_eq!(
            resolver2.resolve(50.into(), None, TsSource::PdTso),
            20.into()
        );

        // Committing the active owner (start_ts=20) untracks k2 and lets resolved-ts
        // advance.
        must_commit(&mut engine, b"k2", 20, 25);
        apply_batches(
            engine.take_last_modifies(),
            &mut resolver2,
            &mut apply_index2,
        );
        assert_eq!(
            resolver2.resolve(50.into(), None, TsSource::PdTso),
            50.into()
        );
    }
}
