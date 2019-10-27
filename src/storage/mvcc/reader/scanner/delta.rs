// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A scanner that outputs content in CF between (start_ts,end_ts]
use std::cmp::Ordering;

use engine::CF_DEFAULT;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::lock::{Lock, LockType};
use crate::storage::mvcc::write::{Write, WriteType};
use crate::storage::mvcc::Result;
use crate::storage::txn::{Result as TxnResult, TxnEntry, TxnEntryScanner};
use crate::storage::{Cursor, Key, KvPair, Snapshot, Statistics};

use super::ScannerConfig;
struct LockEntry {
    user_key: Key,
    entry: TxnEntry,
}

struct LockItem {
    lock: Lock,
    key: Vec<u8>,
    value: Vec<u8>,
}

struct WriteEntry {
    user_key: Key,
    entry: TxnEntry,
}

pub struct DeltaScanner<S: Snapshot> {
    cfg: ScannerConfig<S>,
    // if is none, we needn't scan it
    lock_cursor: Option<Cursor<S::Iter>>,
    write_cursor: Cursor<S::Iter>,
    default_cursor: Option<Cursor<S::Iter>>,
    /// Is iteration started
    is_started: bool,
    statistics: Statistics,
    // the end ts is cfg.ts
    begin_ts: u64,
    // check and return error if a lock exist
    err_lock_exist: bool,
    cache_lock: Option<LockEntry>,
    cache_write: Option<WriteEntry>,
}

impl<S: Snapshot> TxnEntryScanner for DeltaScanner<S> {
    fn next_entry(&mut self) -> TxnResult<Option<TxnEntry>> {
        Ok(self.read_next()?)
    }
    fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }
}

impl<S: Snapshot> DeltaScanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Option<Cursor<S::Iter>>,
        write_cursor: Cursor<S::Iter>,
        begin_ts: u64,
        err_lock_exist: bool,
    ) -> DeltaScanner<S> {
        DeltaScanner {
            cfg,
            lock_cursor,
            write_cursor,
            statistics: Statistics::default(),
            default_cursor: None,
            is_started: false,
            begin_ts,
            err_lock_exist,
            cache_lock: None,
            cache_write: None,
        }
    }

    pub fn read_next(&mut self) -> Result<Option<TxnEntry>> {
        self.check_and_start()?;
        if self.lock_cursor.is_none() {
            let data = self.read_next_commit_item()?;
            return Ok(data.map(|e| e.entry));
        }
        if self.err_lock_exist {
            return self.read_and_check_lock();
        }
        self.read_next_item()
    }

    fn read_and_check_lock(&mut self) -> Result<Option<TxnEntry>> {
        let lock_item = self.read_next_lock_item()?;
        if let Some(lock) = lock_item {
            let current_user_key = Key::from_encoded_slice(&lock.key);
            super::super::util::check_lock(&current_user_key, self.cfg.ts, &lock.lock)?;
        }
        let data = self.read_next_commit_item()?;
        Ok(data.map(|e| e.entry))
    }

    fn read_next_item(&mut self) -> Result<Option<TxnEntry>> {
        let lock = self.get_next_prewrite_item()?;
        let commit = self.get_next_commit_entry()?;
        if lock.is_none() {
            return Ok(commit.map(|entry| entry.entry));
        }
        let lock_entry = lock.unwrap();
        if commit.is_none() {
            return Ok(Some(lock_entry.entry));
        }
        let commit_entry = commit.unwrap();
        match commit_entry.user_key.cmp(&lock_entry.user_key) {
            Ordering::Less => {
                self.cache_lock = Some(lock_entry);
                Ok(Some(commit_entry.entry))
            }
            _ => {
                self.cache_write = Some(commit_entry);
                Ok(Some(lock_entry.entry))
            }
        }
    }

    fn get_next_prewrite_item(&mut self) -> Result<Option<LockEntry>> {
        let lock = self.cache_lock.take();
        if lock.is_none() {
            self.read_next_prewrite_item()
        } else {
            Ok(lock)
        }
    }

    fn get_next_commit_entry(&mut self) -> Result<Option<WriteEntry>> {
        let entry = self.cache_write.take();
        if entry.is_none() {
            self.read_next_commit_item()
        } else {
            Ok(entry)
        }
    }

    fn read_next_prewrite_item(&mut self) -> Result<Option<LockEntry>> {
        let locks = self.read_next_lock_item()?;
        if locks.is_none() {
            return Ok(None);
        }
        let lock_detail = locks.unwrap();
        let current_user_key = Key::from_encoded_slice(&lock_detail.key);
        let default_kv = if lock_detail.lock.lock_type != LockType::Put
            || lock_detail.lock.short_value.is_some()
        {
            (Vec::new(), Vec::new())
        } else {
            let write = Write::new(
                WriteType::from_lock_type(lock_detail.lock.lock_type).unwrap(),
                lock_detail.lock.ts,
                None,
            );
            self.load_from_default_cf(&current_user_key, write)?
        };

        let entry = LockEntry {
            entry: TxnEntry::Prewrite {
                default: default_kv,
                lock: (lock_detail.key, lock_detail.value),
            },
            user_key: current_user_key,
        };
        Ok(Some(entry))
    }

    fn read_next_lock_item(&mut self) -> Result<Option<LockItem>> {
        if self.lock_cursor.is_none() {
            return Ok(None);
        }
        let lock_cursor = self.lock_cursor.as_mut().unwrap();
        while lock_cursor.valid()? {
            let lock_value = lock_cursor.value(&mut self.statistics.lock);
            let lock = Lock::parse(lock_value)?;
            if lock.ts > self.cfg.ts {
                lock_cursor.next(&mut self.statistics.lock);
                continue;
            }
            let lock_value = lock_value.to_vec();
            let lock_key = lock_cursor.key(&mut self.statistics.lock).to_vec();
            lock_cursor.next(&mut self.statistics.lock);
            let item = LockItem {
                lock,
                key: lock_key,
                value: lock_value,
            };
            return Ok(Some(item));
        }
        Ok(None)
    }

    fn read_next_commit_item(&mut self) -> Result<Option<WriteEntry>> {
        loop {
            if !self.write_cursor.valid()? {
                return Ok(None);
            }
            let w_key = self.write_cursor.key(&mut self.statistics.write);
            let (user_key, commit_ts) = Key::split_on_ts_for(w_key)?;
            // Use `from_encoded_slice` to reserve space for ts, so later we can append ts to
            // the key or its clones without reallocation.
            let current_user_key = Key::from_encoded_slice(user_key);
            // goto next key
            if commit_ts <= self.begin_ts {
                self.move_write_cursor_to_next_user_key(&current_user_key)?;
                continue;
            }
            if commit_ts <= self.cfg.ts {
                let v = self.load_data_and_write(&current_user_key)?;
                self.write_cursor.next(&mut self.statistics.write);
                let entry = WriteEntry {
                    user_key: current_user_key,
                    entry: v,
                };
                return Ok(Some(entry));
            }
            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    ///
    /// The implementation is the same as `PointGetter::load_data_and_write`.
    #[inline]
    fn load_data_and_write(&mut self, user_key: &Key) -> Result<(TxnEntry)> {
        let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
        self.statistics.write.processed += 1;
        let w = (
            self.write_cursor.key(&mut self.statistics.write).to_vec(),
            self.write_cursor.value(&mut self.statistics.write).to_vec(),
        );
        // Not PUT or Value is carried in `write`.
        if write.write_type != WriteType::Put || write.short_value.is_some() {
            return Ok(TxnEntry::Commit {
                default: (Vec::new(), Vec::new()),
                write: w,
            });
        };
        let default_kv = self.load_from_default_cf(user_key, write)?;
        Ok(TxnEntry::Commit {
            default: default_kv,
            write: w,
        })
    }

    fn load_from_default_cf(&mut self, user_key: &Key, write: Write) -> Result<KvPair> {
        // Value is in the default CF.
        self.ensure_default_cursor()?;
        let mut default_cf = self.default_cursor.as_mut().unwrap();
        let value = super::super::util::near_load_data_by_write(
            &mut default_cf,
            user_key,
            write,
            &mut self.statistics,
        )?;
        Ok((default_cf.key(&mut self.statistics.data).to_vec(), value))
    }

    /// Create the default cursor if it doesn't exist.
    #[inline]
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        self.default_cursor = Some(self.cfg.create_cf_cursor(CF_DEFAULT)?);
        Ok(())
    }

    fn check_and_start(&mut self) -> Result<()> {
        if self.is_started {
            return Ok(());
        }
        if self.cfg.lower_bound.is_some() {
            // TODO: `seek_to_first` is better, however it has performance issues currently.
            self.write_cursor.seek(
                self.cfg.lower_bound.as_ref().unwrap(),
                &mut self.statistics.write,
            )?;
            if let Some(lock_cursor) = self.lock_cursor.as_mut() {
                lock_cursor.seek(
                    self.cfg.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.lock,
                )?;
            }
        } else {
            self.write_cursor.seek_to_first(&mut self.statistics.write);
            if let Some(lock_cursor) = self.lock_cursor.as_mut() {
                lock_cursor.seek_to_first(&mut self.statistics.lock);
            }
        }
        self.is_started = true;
        Ok(())
    }

    fn move_write_cursor_to_next_user_key(&mut self, current_user_key: &Key) -> Result<()> {
        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write_cursor.next(&mut self.statistics.write);
            }
            if !self.write_cursor.valid()? {
                // Key space ended. We are done here.
                return Ok(());
            }
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, current_user_key.as_encoded().as_slice()) {
                    // Found another user key. We are done here.
                    return Ok(());
                }
            }
        }
        // We have not found another user key for now, so we directly `seek()`.
        // After that, we must pointing to another key, or out of bound.
        // `current_user_key` must have reserved space here, so its clone has reserved space too.
        // So no reallocation happens in `append_ts`.
        self.write_cursor.internal_seek(
            &current_user_key.clone().append_ts(0),
            &mut self.statistics.write,
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::txn_entry::tests::EntryBuilder;
    use super::super::ScannerBuilder;
    use super::*;
    use crate::storage::mvcc::tests::*;
    use crate::storage::{Engine, TestEngineBuilder, SHORT_VALUE_MAX_LEN};

    use kvproto::kvrpcpb::{Context, IsolationLevel};

    #[test]
    fn test_with_rc() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);
        // Generate a Lock for [a] with start ts=8.
        must_prewrite_put(&engine, b"a", b"value", b"a", 8);

        // Generate 5 rollback for [b].
        for ts in 0..10 {
            must_rollback(&engine, b"b", ts);
        }

        must_rollback(&engine, b"c", 1);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let begin_ts = 2;
        let end_ts = 9;
        let mut scanner = ScannerBuilder::new(snapshot, end_ts, false)
            .range(None, None)
            .isolation_level(IsolationLevel::Rc)
            .build_delta(begin_ts, false)
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 b_0
        //       ^cursor
        let mut builder: EntryBuilder = EntryBuilder::default();
        let entry = builder
            .key(b"a")
            .value(b"value")
            .start_ts(7)
            .commit_ts(7)
            .build_commit(WriteType::Put, true);
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        let mut rollback_ts = end_ts;
        while rollback_ts > begin_ts {
            let entry = EntryBuilder::default()
                .key(b"b")
                .start_ts(rollback_ts)
                .commit_ts(rollback_ts)
                .build_commit(WriteType::Rollback, true);
            assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
            let statistics = scanner.take_statistics();
            assert_eq!(statistics.write.seek, 0);
            rollback_ts -= 1;
        }
        assert!(scanner.next_entry().unwrap().is_none());
    }

    #[test]
    fn test_with_rr_get_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);
        // Generate a Lock for [a] with start ts=8.
        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        must_prewrite_put(&engine, b"a", &long_value, b"a", 8);

        // Generate 5 rollback for [b].
        for ts in 0..10 {
            must_rollback(&engine, b"b", ts);
        }

        // Generate a Lock for [a] with start ts=8.
        must_prewrite_put(&engine, b"b", b"value", b"a", 11);

        must_rollback(&engine, b"c", 1);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let begin_ts = 2;
        let end_ts = 9;
        let mut scanner = ScannerBuilder::new(snapshot, end_ts, false)
            .range(None, None)
            .isolation_level(IsolationLevel::Si)
            .build_delta(begin_ts, false)
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 b_0
        //       ^cursor
        let mut builder: EntryBuilder = EntryBuilder::default();
        let key = Key::from_raw(b"a").into_encoded();
        let lock_entry = scanner.next_entry().unwrap().unwrap();
        if let TxnEntry::Prewrite { default, lock } = lock_entry {
            assert_eq!(lock.0, key);
            assert_eq!(default.1, long_value);
        } else {
            panic!("unexpect entry:{:?}", lock_entry);
        }
        let entry = builder
            .key(b"a")
            .value(b"value")
            .start_ts(7)
            .commit_ts(7)
            .build_commit(WriteType::Put, true);
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        let mut rollback_ts = end_ts;
        while rollback_ts > begin_ts {
            let entry = EntryBuilder::default()
                .key(b"b")
                .start_ts(rollback_ts)
                .commit_ts(rollback_ts)
                .build_commit(WriteType::Rollback, true);
            assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
            let statistics = scanner.take_statistics();
            assert_eq!(statistics.write.seek, 0);
            rollback_ts -= 1;
        }
        assert!(scanner.next_entry().unwrap().is_none());
    }

    #[test]
    fn test_with_rr_list_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let txn_keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];

        // Generate a Lock for [a,b,c] with start ts=8.
        for key in txn_keys.iter() {
            must_prewrite_put(&engine, key, b"value", b"a", 8);
        }
        // Generate a Lock for [b1] with start ts=100.
        must_prewrite_put(&engine, b"b1", b"value", b"b1", 100);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let begin_ts = 2;
        let end_ts = 9;
        let mut scanner = ScannerBuilder::new(snapshot, end_ts, false)
            .range(None, None)
            .isolation_level(IsolationLevel::Si)
            .build_delta(begin_ts, false)
            .unwrap();
        for key in txn_keys.iter() {
            let key = Key::from_raw(key).into_encoded();
            let lock_entry = scanner.next_entry().unwrap().unwrap();
            if let TxnEntry::Prewrite { default, lock } = lock_entry {
                assert_eq!(lock.0, key);
                assert!(default.0.is_empty());
            } else {
                panic!("unexpect entry:{:?}", lock_entry);
            }
        }
    }

    #[test]
    fn test_with_rr_err_lock_contains_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let txn_keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        for key in txn_keys.iter() {
            must_prewrite_put(&engine, key, b"value", b"a", 4);
            must_commit(&engine, key, 4, 6);
        }
        // Generate a Lock for [b1] with start ts=7.
        must_prewrite_put(&engine, b"b1", b"value", b"b1", 7);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let begin_ts = 2;
        let end_ts = 9;
        let mut scanner = ScannerBuilder::new(snapshot, end_ts, false)
            .range(None, None)
            .isolation_level(IsolationLevel::Si)
            .build_delta(begin_ts, true)
            .unwrap();
        assert!(scanner.next_entry().is_err());
    }

    #[test]
    fn test_with_rr_err_lock_with_err_no_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let txn_keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        for key in txn_keys.iter() {
            must_prewrite_put(&engine, key, b"value", b"a", 4);
            must_commit(&engine, key, 4, 6);
        }
        // Generate a Lock for [b1] with start ts=100.
        must_prewrite_put(&engine, b"b1", b"value", b"b1", 100);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let begin_ts = 2;
        let end_ts = 9;
        let mut scanner = ScannerBuilder::new(snapshot, end_ts, false)
            .range(None, None)
            .isolation_level(IsolationLevel::Si)
            .build_delta(begin_ts, true)
            .unwrap();
        for key in txn_keys.iter() {
            let entry = EntryBuilder::default()
                .key(key)
                .start_ts(4)
                .commit_ts(6)
                .value(b"value")
                .build_commit(WriteType::Put, true);
            assert_eq!(scanner.next_entry().unwrap(), Some(entry));
        }
    }

    #[test]
    fn test_with_rr_err_lock_with_long_value() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let txn_keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        for key in txn_keys.iter() {
            must_prewrite_put(&engine, key, &long_value, b"a", 4);
            must_commit(&engine, key, 4, 6);
        }
        // Generate a Lock for [b1] with start ts=100.
        must_prewrite_put(&engine, b"b1", b"value", b"b1", 100);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let begin_ts = 2;
        let end_ts = 9;
        let mut scanner = ScannerBuilder::new(snapshot, end_ts, false)
            .range(None, None)
            .isolation_level(IsolationLevel::Si)
            .build_delta(begin_ts, true)
            .unwrap();
        for key in &txn_keys {
            let entry = EntryBuilder::default()
                .key(key)
                .start_ts(4)
                .commit_ts(6)
                .value(&long_value)
                .build_commit(WriteType::Put, false);
            assert_eq!(scanner.next_entry().unwrap(), Some(entry));
        }
    }
}
