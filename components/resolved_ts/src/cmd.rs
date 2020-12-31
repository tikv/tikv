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
        lock: Lock,
        value: Option<Value>,
    },
    Commit {
        key: Key,
        write: Write,
        commit_ts: Option<TimeStamp>,
    },
    OnePc {
        key: Key,
        write: Write,
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
                        let rows = Self::encode_rows(request.requests.into(), is_one_pc);
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
                    Some(ChangeLog::Error(err_header.into()))
                }
            })
            .filter_map(|v| v)
            .collect()
    }

    pub fn encode_rows(requests: Vec<Request>, is_one_pc: bool) -> Vec<ChangeRow> {
        let mut change_logs: HashMap<Key, ChangeRow> = HashMap::default();
        let mut pending_default: HashMap<Key, Vec<u8>> = HashMap::default();

        for mut req in requests {
            if req.get_cmd_type() != CmdType::Put {
                // Do not log delete requests because they are issued by GC
                // frequently.
                if req.get_cmd_type() != CmdType::Delete {
                    debug!(
                        "skip other command";
                        "command" => ?req,
                    );
                }
                continue;
            }
            let mut put = req.take_put();
            match put.cf.as_str() {
                "write" => match decode_write(put.get_key(), put.get_value()) {
                    Some(write) => {
                        // TODO: handle gc_fence here.
                        let key = Key::from_encoded(put.take_key());
                        let commit_ts = key.decode_ts().unwrap();
                        let key = key.truncate_ts().unwrap();
                        if is_one_pc {
                            change_logs.insert(
                                key.clone(),
                                ChangeRow::OnePc {
                                    commit_ts,
                                    write,
                                    value: pending_default.remove(&key),
                                    key,
                                },
                            );
                        } else {
                            let commit_ts = if write.write_type == WriteType::Rollback {
                                None
                            } else {
                                Some(commit_ts)
                            };
                            change_logs.insert(
                                key.clone(),
                                ChangeRow::Commit {
                                    key,
                                    commit_ts,
                                    write,
                                },
                            );
                        }
                    }
                    None => continue,
                },
                "lock" => match decode_lock(put.get_key(), put.get_value()) {
                    Some(lock) => {
                        let key = Key::from_encoded(put.take_key());
                        let value = pending_default.remove(&key);
                        let l = change_logs
                            .insert(key.clone(), ChangeRow::Prewrite { key, lock, value });
                        assert!(l.is_none());
                    }
                    None => continue,
                },
                "" | "default" => {
                    let key = Key::from_encoded(put.take_key());
                    let start_ts = key.decode_ts().unwrap();
                    let key = key.truncate_ts().unwrap();
                    match change_logs.get_mut(&key) {
                        Some(ChangeRow::Prewrite { value, lock, .. }) => {
                            assert!(value.is_none());
                            assert_eq!(start_ts, lock.ts);
                            *value = Some(put.take_value());
                        }
                        Some(ChangeRow::Commit { key, .. }) => {
                            unreachable!("commit {:?} should take a value", key)
                        }
                        Some(ChangeRow::OnePc { write, value, .. }) => {
                            assert!(value.is_none());
                            assert_eq!(start_ts, write.start_ts);
                            *value = Some(put.take_value());
                        }
                        None => {
                            pending_default.insert(key, put.take_value());
                        }
                    }
                }
                other => {
                    panic!("invalid cf {}", other);
                }
            }
        }
        change_logs.into_iter().map(|(_, v)| v).collect()
    }
}

pub(crate) fn decode_write(key: &[u8], value: &[u8]) -> Option<Write> {
    let write = WriteRef::parse(value).unwrap().to_owned();
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

#[cfg(test)]
mod tests {
    use tikv::server::raftkv::modifies_to_requests;
    use tikv::storage::kv::{MockEngineBuilder, TestEngineBuilder};
    use tikv::storage::txn::tests::*;
    use txn_types::{Key, WriteType};

    use super::{ChangeLog, ChangeRow};

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
            let mut rows = ChangeLog::encode_rows(reqs, false);
            assert_eq!(rows.len(), 1);
            match i {
                0 => match rows.pop().unwrap() {
                    ChangeRow::Prewrite { key, lock, .. } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(lock.ts.into_inner(), 1);
                        assert_eq!(b"v1".to_vec(), lock.short_value.unwrap());
                    }
                    _ => unreachable!(),
                },
                1 => match rows.pop().unwrap() {
                    ChangeRow::Commit {
                        key,
                        write,
                        commit_ts,
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(write.start_ts.into_inner(), 1);
                        assert_eq!(commit_ts.unwrap().into_inner(), 2);
                        assert_eq!(b"v1".to_vec(), write.short_value.unwrap());
                    }
                    _ => unreachable!(),
                },
                2 => match rows.pop().unwrap() {
                    ChangeRow::Prewrite { key, lock, .. } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(lock.ts.into_inner(), 3);
                        assert_eq!(b"v2".to_vec(), lock.short_value.unwrap());
                    }
                    _ => unreachable!(),
                },
                3 => match rows.pop().unwrap() {
                    ChangeRow::Commit {
                        key,
                        write,
                        commit_ts,
                    } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(write.write_type, WriteType::Rollback);
                        assert_eq!(write.start_ts.into_inner(), 3);
                        assert!(commit_ts.is_none());
                    }
                    _ => unreachable!(),
                },
                4 => match rows.pop().unwrap() {
                    ChangeRow::Prewrite { key, lock, value } => {
                        assert_eq!(key, Key::from_raw(b"k1"));
                        assert_eq!(lock.ts.into_inner(), 4);
                        assert!(lock.short_value.is_none());
                        assert_eq!(value, Some(vec![b'v'; 512]));
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }
    }
}
