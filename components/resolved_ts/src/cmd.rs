// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use kvproto::errorpb;
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, Request};
use raftstore::coprocessor::{Cmd, CmdBatch};
use raftstore::errors::Error as RaftStoreError;
use tikv::server::raftkv::WriteBatchFlags;
use txn_types::{Key, Lock, LockType, TimeStamp, Value, Write, WriteRef, WriteType};

#[derive(Debug)]
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
        start_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
        // In some cases a rollback will be done by just deleting the lock, need to lookup Resolver
        // to untrack lock and get start ts from it.
        lack_start_ts: bool,
    },
    OnePc {
        key: Key,
        write_type: WriteType,
        commit_ts: TimeStamp,
        value: Option<Value>,
    },
}

pub enum ChangeLog {
    Error(errorpb::Error),
    Rows { index: u64, rows: Vec<ChangeRow> },
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
                        Some(ChangeLog::Rows { index, rows })
                    } else {
                        let mut response = response.take_admin_response();
                        let error = match request.take_admin_request().get_cmd_type() {
                            AdminCmdType::Split => Some(RaftStoreError::EpochNotMatch(
                                "split".to_owned(),
                                vec![
                                    response.mut_split().take_left(),
                                    response.mut_split().take_right(),
                                ],
                            )),
                            AdminCmdType::BatchSplit => Some(RaftStoreError::EpochNotMatch(
                                "batchsplit".to_owned(),
                                response.mut_splits().take_regions().into(),
                            )),
                            AdminCmdType::PrepareMerge
                            | AdminCmdType::CommitMerge
                            | AdminCmdType::RollbackMerge => {
                                Some(RaftStoreError::EpochNotMatch("merge".to_owned(), vec![]))
                            }
                            _ => None,
                        };
                        error.map(|e| ChangeLog::Error(e.into()))
                    }
                } else {
                    let err_header = response.mut_header().take_error();
                    Some(ChangeLog::Error(err_header))
                }
            })
            .filter_map(|v| v)
            .collect()
    }

    fn encode_rows(changes: HashMap<Key, RowChange>, is_one_pc: bool) -> Vec<ChangeRow> {
        changes
            .into_iter()
            .map(|(key, row)| match (row.write, row.lock, row.default) {
                (Some(KeyOp::Put(mut commit_ts, write)), None, default) => {
                    decode_write(key.as_encoded(), &write, true).map(|write| {
                        let Write {
                            short_value,
                            start_ts,
                            write_type,
                            ..
                        } = write;
                        let value = default.map_or_else(|| short_value, |v| Some(v.into_put().1));
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
                                start_ts,
                                write_type,
                                lack_start_ts: false,
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
                        let value = default.map_or_else(|| short_value, |v| Some(v.into_put().1));
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
                    start_ts: TimeStamp::zero(),
                    write_type: WriteType::Rollback,
                    lack_start_ts: true,
                }),
                other => panic!("unexpected row pattern {:?}", other),
            })
            .filter_map(|v| v)
            .collect()
    }
}

pub(crate) fn decode_write(key: &[u8], value: &[u8], is_apply: bool) -> Option<Write> {
    let write = WriteRef::parse(value).unwrap().to_owned();
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
    let lock = Lock::parse(value).unwrap();
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
    for mut req in requests {
        match req.get_cmd_type() {
            CmdType::Put => {
                let mut put = req.take_put();
                let key = Key::from_encoded(put.take_key());
                let value = put.take_value();
                match put.cf.as_str() {
                    "write" => {
                        let ts = key.decode_ts().unwrap();
                        let key = key.truncate_ts().unwrap();
                        let mut row = changes.entry(key).or_default();
                        assert!(row.write.is_none());
                        row.write = Some(KeyOp::Put(Some(ts), value));
                    }
                    "lock" => {
                        let mut row = changes.entry(key).or_default();
                        assert!(row.lock.is_none());
                        row.lock = Some(KeyOp::Put(None, value));
                    }
                    "" | "default" => {
                        let ts = key.decode_ts().unwrap();
                        let key = key.truncate_ts().unwrap();
                        let mut row = changes.entry(key).or_default();
                        assert!(row.default.is_none());
                        row.default = Some(KeyOp::Put(Some(ts), value));
                    }
                    other => {
                        panic!("invalid cf {}", other);
                    }
                }
            }
            CmdType::Delete => {
                let mut delete = req.take_delete();
                match delete.cf.as_str() {
                    "lock" => {
                        let key = Key::from_encoded(delete.take_key());
                        let mut row = changes.entry(key).or_default();
                        row.lock = Some(KeyOp::Delete);
                    }
                    "" | "default" | "write" => {}
                    other => {
                        panic!("invalid cf {}", other);
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
    changes
}

#[cfg(test)]
mod tests {
    use tikv::server::raftkv::modifies_to_requests;
    use tikv::storage::kv::{MockEngineBuilder, TestEngineBuilder};
    use tikv::storage::txn::tests::*;
    use txn_types::{Key, WriteType};

    use super::{group_row_changes, ChangeLog, ChangeRow};

    #[test]
    fn test_cmd_encode() {
        let rocks_engine = TestEngineBuilder::new().build().unwrap();
        let engine = MockEngineBuilder::from_rocks_engine(rocks_engine).build();

        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 1);
        must_commit(&engine, b"k1", 1, 2);

        must_prewrite_put(&engine, b"k1", b"v2", b"k1", 3);
        must_rollback(&engine, b"k1", 3);

        must_prewrite_put(&engine, b"k1", &[b'v'; 512], b"k1", 4);

        let modifies = engine.take_last_modifies();
        for (i, m) in modifies.into_iter().enumerate() {
            let reqs = modifies_to_requests(m);
            let mut rows = ChangeLog::encode_rows(group_row_changes(reqs), false);
            assert_eq!(rows.len(), 1);
            match i {
                0 => match rows.pop().unwrap() {
                    ChangeRow::Prewrite {
                        key,
                        start_ts,
                        value,
                        ..
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(start_ts.into_inner(), 1);
                        assert_eq!(b"v1".to_vec(), value.unwrap());
                    }
                    _ => unreachable!(),
                },
                1 => match rows.pop().unwrap() {
                    ChangeRow::Commit {
                        key,
                        start_ts,
                        write_type,
                        commit_ts,
                        lack_start_ts,
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(start_ts.into_inner(), 1);
                        assert_eq!(commit_ts.unwrap().into_inner(), 2);
                        assert_eq!(write_type, WriteType::Put);
                        assert!(!lack_start_ts);
                    }
                    _ => unreachable!(),
                },
                2 => match rows.pop().unwrap() {
                    ChangeRow::Prewrite {
                        key,
                        start_ts,
                        value,
                        ..
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(start_ts.into_inner(), 3);
                        assert_eq!(b"v2".to_vec(), value.unwrap());
                    }
                    _ => unreachable!(),
                },
                3 => match rows.pop().unwrap() {
                    ChangeRow::Commit {
                        key,
                        write_type,
                        start_ts,
                        commit_ts,
                        lack_start_ts,
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(write_type, WriteType::Rollback);
                        assert_eq!(start_ts.into_inner(), 3);
                        assert!(commit_ts.is_none());
                        assert!(!lack_start_ts);
                    }
                    _ => unreachable!(),
                },
                4 => match rows.pop().unwrap() {
                    ChangeRow::Prewrite {
                        key,
                        start_ts,
                        value,
                        ..
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(start_ts.into_inner(), 4);
                        assert!(value.is_none());
                        assert_eq!(value, Some(vec![b'v'; 512]));
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }
    }
}
