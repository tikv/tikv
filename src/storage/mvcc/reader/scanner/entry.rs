// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A dedicate scanner that outputs content in each CF.

use std::cmp::Ordering;

use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::write::{Write, WriteType};
use crate::storage::mvcc::Result;
use crate::storage::txn::{Result as TxnResult, TxnEntry, TxnEntryScanner};
use crate::storage::{Cursor, Key, Lock, Snapshot, Statistics};

use super::util::CheckLockResult;
use super::ScannerConfig;

/// A dedicate scanner that outputs content in each CF.
///
/// Use `ScannerBuilder` to build `EntryScanner`.
///
/// Note: The implementation is almost the same as `ForwardScanner`, made a few
///       adjustments to output content in each cf.
pub struct Scanner<S: Snapshot> {
    cfg: ScannerConfig<S>,
    lock_cursor: Cursor<S::Iter>,
    write_cursor: Cursor<S::Iter>,
    default_cursor: Cursor<S::Iter>,
    lower_bound: Option<Key>,
    /// Is iteration started
    is_started: bool,
    statistics: Statistics,
}

impl<S: Snapshot> TxnEntryScanner for Scanner<S> {
    fn next_entry(&mut self) -> TxnResult<Option<TxnEntry>> {
        Ok(self.read_next()?)
    }
    fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }
}

impl<S: Snapshot> Scanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
        write_cursor: Cursor<S::Iter>,
        default_cursor: Cursor<S::Iter>,
        lower_bound: Option<Key>,
    ) -> Result<Scanner<S>> {
        Ok(Scanner {
            cfg,
            lock_cursor,
            write_cursor,
            default_cursor,
            lower_bound,
            statistics: Statistics::default(),
            is_started: false,
        })
    }

    /// Get the next txn entry, in forward order.
    pub fn read_next(&mut self) -> Result<Option<TxnEntry>> {
        if !self.is_started {
            if self.lower_bound.is_some() {
                // TODO: `seek_to_first` is better, however it has performance issues currently.
                self.write_cursor.seek(
                    self.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.write,
                )?;
                self.lock_cursor.seek(
                    self.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.lock,
                )?;
            } else {
                self.write_cursor.seek_to_first(&mut self.statistics.write);
                self.lock_cursor.seek_to_first(&mut self.statistics.lock);
            }
            self.is_started = true;
        }

        // The general idea is to simultaneously step write cursor and lock cursor.

        // TODO: We don't need to seek lock CF if isolation level is RC.

        loop {
            // The overall implementation is the same as forward scanner.
            let (current_user_key, has_write, has_lock) = {
                let w_key = if self.write_cursor.valid()? {
                    Some(self.write_cursor.key(&mut self.statistics.write))
                } else {
                    None
                };
                let l_key = if self.lock_cursor.valid()? {
                    Some(self.lock_cursor.key(&mut self.statistics.lock))
                } else {
                    None
                };

                // `res` is `(current_user_key_slice, has_write, has_lock)`
                let res = match (w_key, l_key) {
                    (None, None) => {
                        // Both cursors yield `None`: we know that there is nothing remaining.
                        return Ok(None);
                    }
                    (None, Some(k)) => {
                        // Write cursor yields `None` but lock cursor yields something:
                        // In RC, it means we got nothing.
                        // In SI, we need to check if the lock will cause conflict.
                        (k, false, true)
                    }
                    (Some(k), None) => {
                        // Write cursor yields something but lock cursor yields `None`:
                        // We need to further step write cursor to our desired version
                        (Key::truncate_ts_for(k)?, true, false)
                    }
                    (Some(wk), Some(lk)) => {
                        let write_user_key = Key::truncate_ts_for(wk)?;
                        match write_user_key.cmp(lk) {
                            Ordering::Less => {
                                // Write cursor user key < lock cursor, it means the lock of the
                                // current key that write cursor is pointing to does not exist.
                                (write_user_key, true, false)
                            }
                            Ordering::Greater => {
                                // Write cursor user key > lock cursor, it means we got a lock of a
                                // key that does not have a write. In SI, we need to check if the
                                // lock will cause conflict.
                                (lk, false, true)
                            }
                            Ordering::Equal => {
                                // Write cursor user key == lock cursor, it means the lock of the
                                // current key that write cursor is pointing to *exists*.
                                (lk, true, true)
                            }
                        }
                    }
                };

                // Use `from_encoded_slice` to reserve space for ts, so later we can append ts to
                // the key or its clones without reallocation.
                (Key::from_encoded_slice(res.0), res.1, res.2)
            };

            // `result` stores intermediate values, including KeyLocked errors (but not other kind
            // of errors). If there is KeyLocked errors, we should be able to continue scanning.
            let mut result = Ok(None);

            // `get_ts` is the real used timestamp. If user specifies `MaxInt64` as the timestamp,
            // we need to change it to a most recently available one.
            // But in this scanner we does not allow such operation.
            let get_ts = self.cfg.ts;

            // `met_next_user_key` stores whether the write cursor has been already pointing to
            // the next user key. If so, we don't need to compare it again when trying to step
            // to the next user key later.
            let mut met_next_user_key = false;

            if has_lock {
                match self.cfg.isolation_level {
                    IsolationLevel::Si => {
                        // Only needs to check lock in SI
                        let lock = {
                            let lock_value = self.lock_cursor.value(&mut self.statistics.lock);
                            Lock::parse(lock_value)?
                        };
                        match super::util::check_lock(&current_user_key, self.cfg.ts, &lock)? {
                            CheckLockResult::NotLocked => {}
                            // TODO: We need to scan locks into batch
                            //       in the future.
                            CheckLockResult::Locked(e) => result = Err(e),
                            // TODO: better error handling.
                            CheckLockResult::Ignored(_) => unimplemented!(),
                        }
                    }
                    IsolationLevel::Rc => {}
                }
                self.lock_cursor.next(&mut self.statistics.lock);
            }
            if has_write {
                // We don't need to read version if there is a lock error already.
                if result.is_ok() {
                    // Attempt to read specified version of the key. Note that we may get `None`
                    // indicating that no desired version is found, or a DELETE version is found
                    result = self.get(&current_user_key, get_ts, &mut met_next_user_key);
                }
                // Even if there is a lock error, we still need to step the cursor for future
                // calls. However if we are already pointing at next user key, we don't need to
                // move it any more. `met_next_user_key` eliminates a key compare.
                if !met_next_user_key {
                    self.move_write_cursor_to_next_user_key(&current_user_key)?;
                }
            }

            // If we got something, it can be just used as the return value. Otherwise, we need
            // to continue stepping the cursor.
            if let Some(v) = result? {
                return Ok(Some(v));
            }
        }
    }

    /// Attempt to get the value of a key specified by `user_key` and `self.cfg.ts`. This function
    /// requires that the write cursor is currently pointing to the latest version of `user_key`.
    #[inline]
    fn get(
        &mut self,
        user_key: &Key,
        ts: u64,
        met_next_user_key: &mut bool,
    ) -> Result<Option<TxnEntry>> {
        assert!(self.write_cursor.valid()?);

        // The logic starting from here is similar to `PointGetter`.

        // Try to iterate to `${user_key}_${ts}`. We first `next()` for a few times,
        // and if we have not reached where we want, we use `seek()`.

        // Whether we have *not* reached where we want by `next()`.
        let mut needs_seek = true;

        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write_cursor.next(&mut self.statistics.write);
                if !self.write_cursor.valid()? {
                    // Key space ended.
                    return Ok(None);
                }
            }
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                    // Meet another key.
                    *met_next_user_key = true;
                    return Ok(None);
                }
                if Key::decode_ts_from(current_key)? <= ts {
                    // Founded, don't need to seek again.
                    needs_seek = false;
                    break;
                }
            }
        }
        // If we have not found `${user_key}_${ts}` in a few `next()`, directly `seek()`.
        if needs_seek {
            // `user_key` must have reserved space here, so its clone has reserved space too. So no
            // reallocation happens in `append_ts`.
            self.write_cursor
                .seek(&user_key.clone().append_ts(ts), &mut self.statistics.write)?;
            if !self.write_cursor.valid()? {
                // Key space ended.
                return Ok(None);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                *met_next_user_key = true;
                return Ok(None);
            }
        }

        // Now we must have reached the first key >= `${user_key}_${ts}`. However, we may
        // meet `Lock` or `Rollback`. In this case, more versions needs to be looked up.
        loop {
            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => return Ok(Some(self.load_data_and_write(write, user_key)?)),
                // TODO: we may want to output delete later.
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);

            if !self.write_cursor.valid()? {
                // Key space ended.
                return Ok(None);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                *met_next_user_key = true;
                return Ok(None);
            }
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    ///
    /// The implementation is the same as `PointGetter::load_data_and_write`.
    #[inline]
    fn load_data_and_write(&mut self, write: Write, user_key: &Key) -> Result<TxnEntry> {
        let w = (
            self.write_cursor.key(&mut self.statistics.write).to_vec(),
            self.write_cursor.value(&mut self.statistics.write).to_vec(),
        );
        if write.short_value.is_some() {
            // Value is carried in `write`.
            Ok(TxnEntry::Commit {
                default: (Vec::new(), Vec::new()),
                write: w,
            })
        } else {
            // Value is in the default CF.
            let default_cursor = &mut self.default_cursor;
            let value = super::util::near_load_data_by_write(
                default_cursor,
                user_key,
                write,
                &mut self.statistics,
            )?;
            Ok(TxnEntry::Commit {
                default: (
                    default_cursor.key(&mut self.statistics.data).to_vec(),
                    value,
                ),
                write: w,
            })
        }
    }

    /// After `self.get()`, our write cursor may be pointing to current user key (if we
    /// found a desired version), or next user key (if there is no desired version), or
    /// out of bound.
    ///
    /// If it is pointing to current user key, we need to step it until we meet a new
    /// key. We first try to `next()` a few times. If still not reaching another user
    /// key, we `seek()`.
    #[inline]
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
    use super::super::ScannerBuilder;
    use super::*;
    use crate::storage::mvcc::tests::*;
    use crate::storage::{Engine, Key, TestEngineBuilder};

    use kvproto::kvrpcpb::Context;

    #[derive(Default)]
    struct EntryBuilder {
        key: Vec<u8>,
        value: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
    }

    impl EntryBuilder {
        fn key(&mut self, key: &[u8]) -> &mut Self {
            self.key = key.to_owned();
            self
        }
        fn value(&mut self, val: &[u8]) -> &mut Self {
            self.value = val.to_owned();
            self
        }
        fn start_ts(&mut self, start_ts: u64) -> &mut Self {
            self.start_ts = start_ts;
            self
        }
        fn commit_ts(&mut self, commit_ts: u64) -> &mut Self {
            self.commit_ts = commit_ts;
            self
        }
        fn build_commit(&self, wt: WriteType, is_short_value: bool) -> TxnEntry {
            let write_key = Key::from_raw(&self.key).append_ts(self.commit_ts);
            let (key, value, short) = if is_short_value {
                (vec![], vec![], Some(self.value.clone()))
            } else {
                (
                    Key::from_raw(&self.key)
                        .append_ts(self.start_ts)
                        .into_encoded(),
                    self.value.clone(),
                    None,
                )
            };
            let write_value = Write::new(wt, self.start_ts, short);
            TxnEntry::Commit {
                default: (key, value),
                write: (write_key.into_encoded(), write_value.to_bytes()),
            }
        }
    }

    /// Check whether everything works as usual when `EntryScanner::get()` goes out of bound.
    #[test]
    fn test_get_out_of_bound() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate 5 rollback for [b].
        for ts in 0..5 {
            must_rollback(&engine, b"b", ts);
        }

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10, false)
            .range(None, None)
            .build_entry_scanner()
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_4 b_3 b_2 b_1 b_0
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

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 1. next() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_1() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND / 2 rollback and 1 put for [b] .
        for ts in 0..SEEK_BOUND / 2 {
            must_rollback(&engine, b"b", ts as u64);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND / 2);
        must_commit(&engine, b"b", SEEK_BOUND / 2, SEEK_BOUND / 2);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, SEEK_BOUND * 2, false)
            .range(None, None)
            .build_entry_scanner()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        let entry = EntryBuilder::default()
            .key(b"a")
            .value(b"a_value")
            .start_ts(16)
            .commit_ts(16)
            .build_commit(WriteType::Put, true);
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

        // Before:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND / 2 + 1 next to reach next user key and stop:
        //   a_8 b_2 b_1 b_0
        //                   ^cursor
        let entry = EntryBuilder::default()
            .key(b"b")
            .value(b"b_value")
            .start_ts(4)
            .commit_ts(4)
            .build_commit(WriteType::Put, true);
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 2. seek() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_2() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND-1 rollback and 1 put for [b] .
        for ts in 1..SEEK_BOUND {
            must_rollback(&engine, b"b", ts as u64);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND);
        must_commit(&engine, b"b", SEEK_BOUND, SEEK_BOUND);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, SEEK_BOUND * 2, false)
            .range(None, None)
            .build_entry_scanner()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_4 b_3 b_2 b_1
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        let entry = EntryBuilder::default()
            .key(b"a")
            .value(b"a_value")
            .start_ts(16)
            .commit_ts(16)
            .build_commit(WriteType::Put, true);
        assert_eq!(scanner.next_entry().unwrap(), Some(entry));
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

        // Before:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND-1 next: (TODO: fix it to SEEK_BOUND)
        //   a_8 b_4 b_3 b_2 b_1
        //                   ^cursor
        // We still pointing at current user key, so a seek:
        //   a_8 b_4 b_3 b_2 b_1
        //                       ^cursor
        let entry = EntryBuilder::default()
            .key(b"b")
            .value(b"b_value")
            .start_ts(8)
            .commit_ts(8)
            .build_commit(WriteType::Put, true);
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Range is left open right closed.
    #[test]
    fn test_range() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [1], [2] ... [6].
        for i in 1..7 {
            // ts = 1: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 1);
            must_commit(&engine, &[i], 1, 1);

            // ts = 7: value = [ts]
            must_prewrite_put(&engine, &[i], &[i], &[i], 7);
            must_commit(&engine, &[i], 7, 7);

            // ts = 14: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 14);
            must_commit(&engine, &[i], 14, 14);
        }

        let snapshot = engine.snapshot(&Context::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10, false)
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build_entry_scanner()
            .unwrap();

        let entry = |key, ts| {
            EntryBuilder::default()
                .key(key)
                .value(key)
                .start_ts(ts)
                .commit_ts(ts)
                .build_commit(WriteType::Put, true)
        };

        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[3u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[4u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10, false)
            .range(None, Some(Key::from_raw(&[3u8])))
            .build_entry_scanner()
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10, false)
            .range(Some(Key::from_raw(&[5u8])), None)
            .build_entry_scanner()
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10, false)
            .range(None, None)
            .build_entry_scanner()
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[3u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[4u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7)));
        assert_eq!(scanner.next_entry().unwrap(), None);
    }
}
