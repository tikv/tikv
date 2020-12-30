use collections::HashMap;
use kvproto::errorpb;
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, Request};
use raftstore::coprocessor::{Cmd, CmdBatch};
use raftstore::errors::Error as RaftStoreError;
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
}

pub enum ChangeLog {
    Error(errorpb::Error),
    Rows {
        index: u64,
        // (index, change rows)
        rows: Vec<ChangeRow>,
    },
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
                        let rows = Self::encode_rows(request.requests.into());
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

    fn encode_rows(requests: Vec<Request>) -> Vec<ChangeRow> {
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
                        let key = Key::from_encoded(put.take_key());
                        let commit_ts = if write.write_type == WriteType::Rollback {
                            None
                        } else {
                            Some(key.decode_ts().unwrap())
                        };
                        let key = key.truncate_ts().unwrap();
                        let l = change_logs.insert(
                            key.clone(),
                            ChangeRow::Commit {
                                key,
                                commit_ts,
                                write,
                            },
                        );
                        assert!(l.is_none());
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
                        Some(ChangeRow::Commit { write, .. }) => {
                            assert_eq!(start_ts, write.start_ts);
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
