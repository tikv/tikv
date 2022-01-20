// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::errorpb;
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, Request};
use raftstore::coprocessor::{Cmd, CmdBatch, ObserveLevel};
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
                    mut request,
                    mut response,
                } = cmd;
                if !response.get_header().has_error() {
                    if !request.has_admin_request() {
                        let flags =
                            WriteBatchFlags::from_bits_truncate(request.get_header().get_flags());
                        let is_one_pc = flags.contains(WriteBatchFlags::ONE_PC);
                        let changes = group_row_changes(request.requests.into());
                        let rows = Self::encode_rows(changes, is_one_pc);
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
        changes
            .into_iter()
            .filter_map(|(key, row)| match (row.write, row.lock, row.default) {
                (Some(KeyOp::Put(mut commit_ts, write)), _, default) => {
                    decode_write(key.as_encoded(), &write, true).map(|write| {
                        let Write {
                            short_value,
                            start_ts,
                            write_type,
                            ..
                        } = write;
                        let value = default.map_or(short_value, |v| Some(v.into_put().1));
                        if is_one_pc {
                            ChangeRow::OnePc {
                                commit_ts: commit_ts.unwrap_or_default(),
                                write_type,
                                value,
                                key,
                            }
                        } else {
                            if write.write_type == WriteType::Rollback {
                                commit_ts = None;
                            }
                            ChangeRow::Commit {
                                key,
                                commit_ts,
                                start_ts: Some(start_ts),
                                write_type,
                            }
                        }
                    })
                }
                (None, Some(KeyOp::Put(_, lock)), default) => decode_lock(key.as_encoded(), &lock)
                    .map(|lock| {
                        let Lock {
                            short_value,
                            ts,
                            lock_type,
                            ..
                        } = lock;
                        let value = default.map_or(short_value, |v| Some(v.into_put().1));
                        ChangeRow::Prewrite {
                            key,
                            start_ts: ts,
                            lock_type,
                            value,
                        }
                    }),
                (None, Some(KeyOp::Delete), _) => Some(ChangeRow::Commit {
                    key,
                    commit_ts: None,
                    start_ts: None,
                    write_type: WriteType::Rollback,
                }),
                other => panic!("unexpected row pattern {:?}", other),
            })
            .collect()
    }
}

pub(crate) fn decode_write(key: &[u8], value: &[u8], is_apply: bool) -> Option<Write> {
    let write = WriteRef::parse(value).ok()?.to_owned();
    // Drop the record it self but keep only the overlapped rollback information if gc_fence exists.
    if is_apply && write.gc_fence.is_some() {
        // `gc_fence` is set means the write record has been rewritten.
        // Currently the only case is writing overlapped_rollback. And in this case
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

pub(crate) fn decode_lock(key: &[u8], value: &[u8]) -> Option<Lock> {
    let lock = Lock::parse(value).ok()?;
    match lock.lock_type {
        LockType::Put | LockType::Delete => Some(lock),
        other => {
            debug!("skip lock record";
                "type" => ?other,
                "start_ts" => ?lock.ts,
                "key" => log_wrappers::Value(key),
                "for_update_ts" => ?lock.for_update_ts);
            None
        }
    }
}

#[derive(Debug)]
enum KeyOp {
    Put(Option<TimeStamp>, Vec<u8>),
    Delete,
}

impl KeyOp {
    fn into_put(self) -> (Option<TimeStamp>, Vec<u8>) {
        match self {
            KeyOp::Put(ts, value) => (ts, value),
            KeyOp::Delete => unreachable!(),
        }
    }
}

#[derive(Default)]
struct RowChange {
    write: Option<KeyOp>,
    lock: Option<KeyOp>,
    default: Option<KeyOp>,
}

fn group_row_changes(requests: Vec<Request>) -> HashMap<Key, RowChange> {
    let mut changes: HashMap<Key, RowChange> = HashMap::default();
    // The changes about default cf was recorded here and need to be matched with a `write` or a `lock`.
    let mut unmatched_default = HashMap::default();
    for mut req in requests {
        match req.get_cmd_type() {
            CmdType::Put => {
                let mut put = req.take_put();
                let key = Key::from_encoded(put.take_key());
                let value = put.take_value();
                match put.cf.as_str() {
                    CF_WRITE => {
                        if let Ok(ts) = key.decode_ts() {
                            let key = key.truncate_ts().unwrap();
                            let mut row = changes.entry(key).or_default();
                            assert!(row.write.is_none());
                            row.write = Some(KeyOp::Put(Some(ts), value));
                        }
                    }
                    CF_LOCK => {
                        let mut row = changes.entry(key).or_default();
                        assert!(row.lock.is_none());
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
                        let mut row = changes.entry(key).or_default();
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
    changes
}

/// Filter non-lock related data (i.e `default_cf` data), the implement is subject to
/// how `group_row_changes` and `encode_rows` encode `ChangeRow`
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
                    cf == CF_LOCK || cf == CF_WRITE
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
    use kvproto::kvrpcpb::AssertionLevel;
    use tikv::server::raftkv::modifies_to_requests;
    use tikv::storage::kv::{MockEngineBuilder, TestEngineBuilder};
    use tikv::storage::lock_manager::DummyLockManager;
    use tikv::storage::mvcc::{tests::write, Mutation, MvccTxn, SnapshotReader};
    use tikv::storage::txn::commands::one_pc_commit_ts;
    use tikv::storage::txn::tests::*;
    use tikv::storage::txn::{prewrite, CommitKind, TransactionKind, TransactionProperties};
    use tikv::storage::Engine;
    use tikv_kv::Modify;
    use txn_types::{Key, LockType, WriteType};

    use super::{group_row_changes, ChangeLog, ChangeRow};

    #[test]
    fn test_cmd_encode() {
        let rocks_engine = TestEngineBuilder::new().build().unwrap();
        let engine = MockEngineBuilder::from_rocks_engine(rocks_engine).build();

        let reqs = modifies_to_requests(vec![Modify::Put(
            "default",
            Key::from_raw(b"k1"),
            b"v1".to_vec(),
        )]);
        assert!(ChangeLog::encode_rows(group_row_changes(reqs), false).is_empty());

        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 1);
        must_commit(&engine, b"k1", 1, 2);

        must_prewrite_put(&engine, b"k1", b"v2", b"k1", 3);
        must_rollback(&engine, b"k1", 3, false);

        must_prewrite_put(&engine, b"k1", &[b'v'; 512], b"k1", 4);
        must_commit(&engine, b"k1", 4, 5);

        must_prewrite_put(&engine, b"k1", b"v3", b"k1", 5);
        must_rollback(&engine, b"k1", 5, false);

        let k1 = Key::from_raw(b"k1");
        let rows: Vec<_> = engine
            .take_last_modifies()
            .into_iter()
            .flat_map(|m| {
                let reqs = modifies_to_requests(m);
                ChangeLog::encode_rows(group_row_changes(reqs), false)
            })
            .collect();

        let expected = vec![
            ChangeRow::Prewrite {
                key: k1.clone(),
                start_ts: 1.into(),
                value: Some(b"v1".to_vec()),
                lock_type: LockType::Put,
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
            },
            ChangeRow::Commit {
                key: k1.clone(),
                start_ts: None,
                commit_ts: None,
                write_type: WriteType::Rollback,
            },
        ];
        assert_eq!(rows, expected);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(42.into());
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
            },
            Mutation::make_put(k1.clone(), b"v4".to_vec()),
            &None,
            false,
        )
        .unwrap();
        one_pc_commit_ts(true, &mut txn, 10.into(), &DummyLockManager);
        write(&engine, &Default::default(), txn.into_modifies());
        let one_pc_row = engine
            .take_last_modifies()
            .into_iter()
            .flat_map(|m| {
                let reqs = modifies_to_requests(m);
                ChangeLog::encode_rows(group_row_changes(reqs), true)
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
}
