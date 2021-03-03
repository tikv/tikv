use collections::HashMap;
use kvproto::raft_cmdpb::{CmdType, Request};
use raftstore::coprocessor::Cmd;
use tikv_util::WriteBatchFlags;
use txn_types::{Key, Lock, LockType, TimeStamp, Value, Write, WriteRef, WriteType};

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
        // Define it for one PC.
        value: Option<Value>,
    },
}

pub struct ChangeLog {
    pub index: u64,
    pub rows: Vec<ChangeRow>,
}

impl ChangeLog {
    pub fn encode_change_log(cmd: Cmd) -> ChangeLog {
        let Cmd { index, request, .. } = cmd;
        let flags = WriteBatchFlags::from_bits_truncate(request.get_header().get_flags());
        if flags.contains(WriteBatchFlags::ONE_PC) {
            debug!("skip encode one pc change log");
            return ChangeLog {
                index,
                rows: vec![],
            };
        }
        ChangeLog {
            index,
            rows: Self::encode_rows(request.requests.into()),
        }
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
                                value: None,
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
                        Some(ChangeRow::Commit { value, write, .. }) => {
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

fn decode_write(key: &[u8], value: &[u8]) -> Option<Write> {
    let write = WriteRef::parse(value).unwrap().to_owned();
    match write.write_type {
        WriteType::Put | WriteType::Delete | WriteType::Rollback => Some(write),
        other => {
            debug!("skip write record"; "write" => ?other, "key" => log_wrappers::Value(key));
            None
        }
    }
}

fn decode_lock(key: &[u8], value: &[u8]) -> Option<Lock> {
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
