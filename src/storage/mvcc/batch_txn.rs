// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock::{Lock, LockType};
use super::metrics::*;
use super::reader::MvccReader;
use super::write::{Write, WriteType};
use super::{Error, Result};
use crate::storage::kv::{Cursor, Modify, Snapshot};
use crate::storage::txn;
use crate::storage::{
    is_short_value, Command, Error as StorageError, Key, Options, Result as StorageResult,
    Statistics, Value, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use kvproto::kvrpcpb::IsolationLevel;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use tidb_query::util::convert_to_prefix_next;

pub struct BatchMvccTxn<S: Snapshot> {
    reader: MvccReader<S>,
    writes: Vec<Modify>,
    write_size: usize,
}

impl<S: Snapshot> fmt::Debug for BatchMvccTxn<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "batch-txn")
    }
}

pub type BatchResults<T> = Vec<(u64, Result<T>)>;

impl<S: Snapshot> BatchMvccTxn<S> {
    pub fn new(snapshot: S) -> Result<Self> {
        Ok(BatchMvccTxn {
            reader: MvccReader::new(
                snapshot.clone(),
                None,
                // TODO(tabokie)
                false, /*fill_cache*/
                None,
                None,
                IsolationLevel::Si,
            ),
            writes: vec![],
            write_size: 0,
        })
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.writes
    }

    pub fn take_statistics(&mut self) -> Statistics {
        let mut statistics = Statistics::default();
        self.reader.collect_statistics_into(&mut statistics);
        statistics
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    fn checkpoint(&mut self) -> (usize, usize) {
        (self.write_size, self.writes.len())
    }

    fn reset(&mut self, checkpoint: (usize, usize)) {
        self.write_size = checkpoint.0;
        self.writes.truncate(checkpoint.1);
    }

    fn put_lock(&mut self, key: Key, lock: &Lock) {
        let lock = lock.to_bytes();
        self.write_size += CF_LOCK.len() + key.as_encoded().len() + lock.len();
        self.writes.push(Modify::Put(CF_LOCK, key, lock));
    }

    fn lock_key(
        &mut self,
        key: Key,
        start_ts: u64,
        lock_type: LockType,
        primary: Vec<u8>,
        short_value: Option<Value>,
        options: &Options,
    ) {
        let lock = Lock::new(
            lock_type,
            primary,
            start_ts,
            options.lock_ttl,
            short_value,
            options.for_update_ts,
            options.txn_size,
            options.min_commit_ts,
        );
        self.put_lock(key, &lock);
    }

    fn unlock_key(&mut self, key: Key) {
        self.write_size += CF_LOCK.len() + key.as_encoded().len();
        self.writes.push(Modify::Delete(CF_LOCK, key));
    }

    fn put_value(&mut self, key: Key, ts: u64, value: Value) {
        let key = key.append_ts(ts);
        self.write_size += key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_DEFAULT, key, value));
    }

    fn put_write(&mut self, key: Key, ts: u64, value: Value) {
        let key = key.append_ts(ts);
        self.write_size += CF_WRITE.len() + key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_WRITE, key, value));
    }

    fn prewrite_key_value(
        &mut self,
        key: Key,
        start_ts: u64,
        lock_type: LockType,
        primary: Vec<u8>,
        value: Option<Value>,
        options: &Options,
    ) {
        if let Some(value) = value {
            if is_short_value(&value) {
                // If the value is short, embed it in Lock.
                self.lock_key(key, start_ts, lock_type, primary, Some(value), options);
            } else {
                // value is long
                self.put_value(key.clone(), start_ts, value);

                self.lock_key(key, start_ts, lock_type, primary, None, options);
            }
        } else {
            self.lock_key(key, start_ts, lock_type, primary, None, options);
        }
    }

    fn check_write(
        &mut self,
        key: Key,
        ts: u64,
        is_insert: bool,
        primary: &[u8],
        iter: &mut Cursor<S::Iter>,
        key_equal: &mut bool,
    ) -> Result<()> {
        let statistics = self.reader.mut_write_statistics();
        let write_key = iter.key(statistics);
        let commit_ts = Key::decode_ts_from(write_key)?;
        if Key::is_user_key_eq(write_key, key.as_encoded()) {
            *key_equal = true;
            let mut write = Write::parse(iter.value(statistics))?;
            if commit_ts >= ts {
                return Err(Error::WriteConflict {
                    start_ts: ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_owned(),
                });
            } else if is_insert {
                // check data constraint using existing cursor
                loop {
                    if write.write_type == WriteType::Delete {
                        break;
                    } else if write.write_type == WriteType::Put {
                        return Err(Error::AlreadyExist { key: key.to_raw()? });
                    }
                    write = if iter.next(statistics)
                        && Key::is_user_key_eq(iter.key(statistics), key.as_encoded())
                    {
                        Write::parse(iter.value(statistics))?
                    } else {
                        *key_equal = false;
                        break;
                    }
                }
            }
        } else {
            *key_equal = false;
        }
        Ok(())
    }

    pub fn batch_prewrite(
        &mut self,
        ids: &mut Vec<Option<u64>>,
        commands: &[Command],
    ) -> Result<BatchResults<Vec<StorageResult<()>>>> {
        let mut results = Vec::new();
        let mut map = BTreeMap::new();
        let mut locks = HashMap::<_, Vec<StorageResult<()>>>::new();
        let len = commands.len();
        let mut _rows = 0;
        // initialize mutation to txn mapping
        for i in 0..len {
            if let Command::Prewrite { mutations, .. } = &commands[i] {
                let len = mutations.len();
                for j in 0..len {
                    map.insert(mutations[j].key().clone().into_encoded(), (i, j));
                }
            }
        }

        let mut idx;
        // peek mutation boundaries
        let (lower, _) = map.iter().next().unwrap();
        let min_key = Key::from_encoded(lower.to_vec());
        let (upper, _) = map.iter().next_back().unwrap();
        let mut upper = upper.to_vec();
        convert_to_prefix_next(&mut upper);
        let max_key = Key::from_encoded(upper);

        // initialize iterator over write/lock cf
        let mut write_cursor = self
            .reader
            .new_write_cursor(&min_key, u64::max_value(), &max_key, 0, false)
            .unwrap();
        let mut key_equal = true;

        let mut range = map.range::<Vec<u8>, _>(..);
        'write_loop: loop {
            // find first active mutation
            let (min_key, min_ts) = loop {
                if let Some(tuple) = range.next() {
                    idx = tuple.1;
                    if ids[idx.0].is_some() {
                        if if let Command::Prewrite { options, .. } = &commands[idx.0] {
                            !options.skip_constraint_check
                        } else {
                            unreachable!()
                        } {
                            break (
                                Key::from_encoded(tuple.0.to_vec()),
                                if let Command::Prewrite { start_ts, .. } = commands[idx.0] {
                                    start_ts
                                } else {
                                    unreachable!()
                                },
                            );
                        }
                    }
                } else {
                    break 'write_loop;
                }
            };
            write_cursor.seek(
                &min_key.clone().append_ts(u64::max_value()),
                self.reader.mut_write_statistics(),
            )?;
            if write_cursor.valid()? {
                if let Err(err) = self.check_write(
                    min_key,
                    min_ts,
                    if let Command::Prewrite { mutations, .. } = &commands[idx.0] {
                        mutations[idx.1].is_insert()
                    } else {
                        unreachable!()
                    },
                    if let Command::Prewrite { primary, .. } = &commands[idx.0] {
                        primary
                    } else {
                        unreachable!()
                    },
                    &mut write_cursor,
                    &mut key_equal,
                ) {
                    results.push((ids[idx.0].take().unwrap(), Err(err)))
                }
            }
            if !write_cursor.valid()? {
                break;
            } else if !key_equal {
                range = map.range::<Vec<u8>, _>(
                    &Key::truncate_ts_for(write_cursor.key(self.reader.mut_write_statistics()))?
                        .to_vec()..,
                );
            }
        }

        range = map.range::<Vec<u8>, _>(..);
        'lock_loop: loop {
            let (min_key, min_ts) = loop {
                if let Some(tuple) = range.next() {
                    idx = tuple.1;
                    if ids[idx.0].is_some() {
                        break (
                            Key::from_encoded(tuple.0.to_vec()),
                            if let Command::Prewrite { start_ts, .. } = commands[idx.0] {
                                start_ts
                            } else {
                                unreachable!()
                            },
                        );
                    }
                } else {
                    break 'lock_loop;
                }
            };
            if let Some(lock) = self.reader.load_lock(&min_key)? {
                if lock.ts != min_ts {
                    let mut info = kvproto::kvrpcpb::LockInfo::default();
                    info.set_primary_lock(lock.primary);
                    info.set_lock_version(lock.ts);
                    info.set_key(min_key.into_raw()?);
                    info.set_lock_ttl(lock.ttl);
                    info.set_txn_size(lock.txn_size);
                    let result = StorageResult::Err(StorageError::from(txn::Error::from(
                        Error::KeyIsLocked(info),
                    )));
                    if let Some(locks) = locks.get_mut(&idx.0) {
                        locks.push(result);
                    } else {
                        locks.insert(idx.0, vec![result]);
                    }
                }
            }
        }

        for i in 0..len {
            if ids[i].is_some() {
                if let Command::Prewrite {
                    mutations,
                    primary,
                    options,
                    start_ts,
                    ..
                } = &commands[i]
                {
                    _rows += mutations.len();
                    if let Some(locks) = locks.remove(&i) {
                        results.push((ids[i].take().unwrap(), Ok(locks)));
                    } else {
                        for m in mutations {
                            let lock_type = LockType::from_mutation(&m);
                            let (key, value) = m.clone().into_key_value();
                            self.prewrite_key_value(
                                key,
                                *start_ts,
                                lock_type,
                                primary.to_vec(),
                                value,
                                &options,
                            );
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn commit(&mut self, key: Key, start_ts: u64, commit_ts: u64) -> Result<bool> {
        let (lock_type, short_value, is_pessimistic_txn) = match self.reader.load_lock(&key)? {
            Some(ref mut lock) if lock.ts == start_ts => {
                // A pessimistic lock cannot be committed.
                if lock.lock_type == LockType::Pessimistic {
                    error!(
                        "trying to commit a pessimistic lock";
                        "key" => %key,
                        "start_ts" => start_ts,
                        "commit_ts" => commit_ts,
                    );
                    return Err(Error::LockTypeNotMatch {
                        start_ts,
                        key: key.into_raw()?,
                        pessimistic: true,
                    });
                }
                (
                    lock.lock_type,
                    lock.short_value.take(),
                    lock.for_update_ts != 0,
                )
            }
            _ => {
                return match self.reader.get_txn_commit_info(&key, start_ts)? {
                    Some((_, WriteType::Rollback)) | None => {
                        MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                        // None: related Rollback has been collapsed.
                        // Rollback: rollback by concurrent transaction.
                        info!(
                            "txn conflict (lock not found)";
                            "key" => %key,
                            "start_ts" => start_ts,
                            "commit_ts" => commit_ts,
                        );
                        Err(Error::TxnLockNotFound {
                            start_ts,
                            commit_ts,
                            key: key.into_raw()?,
                        })
                    }
                    // Committed by concurrent transaction.
                    Some((_, WriteType::Put))
                    | Some((_, WriteType::Delete))
                    | Some((_, WriteType::Lock)) => {
                        MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                        Ok(false)
                    }
                };
            }
        };
        let write = Write::new(
            WriteType::from_lock_type(lock_type).unwrap(),
            start_ts,
            short_value,
        );
        self.put_write(key.clone(), commit_ts, write.to_bytes());
        self.unlock_key(key);
        Ok(is_pessimistic_txn)
    }

    pub fn batch_commit(
        &mut self,
        ids: &mut Vec<Option<u64>>,
        commands: &[Command],
    ) -> Result<BatchResults<Result<()>>> {
        let mut results = Vec::new();
        let mut _rows = 0;
        let len = commands.len();
        for i in 0..len {
            if let Command::Commit {
                keys,
                commit_ts,
                lock_ts,
                ..
            } = &commands[i]
            {
                let checkpoint = (self.checkpoint(), _rows);
                for k in keys {
                    if let Err(e) = self.commit(k.clone(), *lock_ts, *commit_ts) {
                        results.push((ids[i].take().unwrap(), Err(e)));
                        self.reset(checkpoint.0);
                        _rows = checkpoint.1;
                        break;
                    }
                    _rows += 1;
                }
            }
        }
        Ok(results)
    }
}
