// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use engine::CF_DEFAULT;
use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::default_not_found_error;
use crate::storage::mvcc::write::{WriteRef, WriteType};
use crate::storage::mvcc::{Error, Result, TimeStamp};
use crate::storage::{Cursor, Key, Lock, Snapshot, Statistics};

use super::ScannerConfig;

/// This struct can be used to scan keys starting from the given user key (greater than or equal).
///
/// Internally, for each key, rollbacks are ignored and smaller version will be tried. If the
/// isolation level is SI, locks will be checked first.
///
/// Use `ScannerBuilder` to build `ForwardScanner`.
pub struct ForwardScanner<S: Snapshot> {
    cfg: ScannerConfig<S>,
    lock_cursor: Cursor<S::Iter>,
    write_cursor: Cursor<S::Iter>,
    /// `default cursor` is lazy created only when it's needed.
    default_cursor: Option<Cursor<S::Iter>>,
    /// Is iteration started
    is_started: bool,
    need_read_next_finalize: bool,
    statistics: Statistics,

    /// Name this field as unsafe because it's lifetime is not really &'static. Instead it is
    /// valid only when `Self` is valid.
    unsafe_value_slice: &'static [u8],
}

impl<S: Snapshot> ForwardScanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
        write_cursor: Cursor<S::Iter>,
    ) -> ForwardScanner<S> {
        ForwardScanner {
            cfg,
            lock_cursor,
            write_cursor,
            statistics: Statistics::default(),
            default_cursor: None,
            is_started: false,
            need_read_next_finalize: false,
            unsafe_value_slice: &[],
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }

    fn init_cursor(&mut self) -> Result<()> {
        assert!(!self.is_started);

        if self.cfg.lower_bound.is_some() {
            // TODO: `seek_to_first` is better, however it has performance issues currently.
            self.write_cursor.seek(
                self.cfg.lower_bound.as_ref().unwrap(),
                &mut self.statistics.write,
            )?;
            self.lock_cursor.seek(
                self.cfg.lower_bound.as_ref().unwrap(),
                &mut self.statistics.lock,
            )?;
        } else {
            self.write_cursor.seek_to_first(&mut self.statistics.write);
            self.lock_cursor.seek_to_first(&mut self.statistics.lock);
        }
        self.is_started = true;
        Ok(())
    }

    #[inline]
    pub fn read_next(&mut self) -> Result<Option<Key>> {
        if self.need_read_next_finalize {
            panic!("read_next_finalize() is not called");
        }

        self.unsafe_value_slice = &[];

        if !self.is_started {
            // Slow path
            self.init_cursor()?;
        }

        // The general idea is to simultaneously step write cursor and lock cursor.

        loop {
            self.need_read_next_finalize = true;

            // `current_user_key` is `min(user_key(write_cursor), lock_cursor)`, indicating
            // the encoded user key we are currently dealing with. It may not have a write, or
            // may not have a lock. It is not a slice to avoid data being invalidated after
            // cursor moving.
            //
            // `has_write` indicates whether `current_user_key` has at least one corresponding
            // `write`. If there is one, it is what current write cursor pointing to. The pointed
            // `write` must be the most recent (i.e. largest `commit_ts`) write of
            // `current_user_key`.
            //
            // `has_lock` indicates whether `current_user_key` has a corresponding `lock`. If
            // there is one, it is what current lock cursor pointing to.
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
                        self.need_read_next_finalize = false;
                        return Ok(None);
                    }
                    (None, Some(k)) => {
                        // Write cursor yields `None` but lock cursor yields something:
                        // In RC, it means we got nothing.
                        // In SI, we need to check if the lock will cause conflict.
                        self.need_read_next_finalize = false;
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
                                self.need_read_next_finalize = false;
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

            if has_lock {
                // Slow path: there is a lock, check the lock first.
                if let Some(lock_err) = self.process_lock_and_may_finalize(&current_user_key)? {
                    assert!(!self.need_read_next_finalize);
                    return Err(lock_err);
                }
            }

            if has_write {
                if self.process_write(&current_user_key)? {
                    assert!(self.need_read_next_finalize);
                    return Ok(Some(current_user_key));
                }

                // Hide the slow path behind the `if` to avoid unnecessary function call costs.
                if self.need_read_next_finalize {
                    self.read_next_finalize(&current_user_key)?;
                }
            }
        }
    }

    fn process_lock_and_may_finalize(&mut self, user_key: &Key) -> Result<Option<Error>> {
        let mut lock_error = None;
        match self.cfg.isolation_level {
            IsolationLevel::Si => {
                // Only needs to check lock in SI
                let lock = {
                    let lock_value = self.lock_cursor.value(&mut self.statistics.lock);
                    Lock::parse(lock_value)?
                };
                if let Err(err) =
                    lock.check_ts_conflict(user_key, self.cfg.ts, &self.cfg.bypass_locks)
                {
                    lock_error = Some(err);

                    // Always try to advance the write cursor to point to next user key when
                    // there is a lock, since we don't need to access the value and it is safe
                    // to simply move the cursor.
                    self.read_next_finalize(user_key)?;
                }
            }
            IsolationLevel::Rc => {}
        }

        // Always move lock cursor forward, pointing to next lock.
        self.lock_cursor.next(&mut self.statistics.lock);

        Ok(lock_error)
    }

    #[inline]
    fn process_write(&mut self, user_key: &Key) -> Result<bool> {
        let current_key = self.write_cursor.key(&mut self.statistics.write);
        if Key::decode_ts_from(current_key)? > self.cfg.ts {
            // Slow path: Ts not match, move cursor to match ts first.
            if !self.move_write_cursor_to_ts(user_key)? {
                return Ok(false);
            }
        }

        let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;
        self.statistics.write.processed += 1;

        match write.write_type {
            WriteType::Put => {
                if self.cfg.omit_value {
                    return Ok(true);
                }
                match write.short_value {
                    Some(value) => {
                        // Value is carried in `write`.
                        self.unsafe_value_slice =
                            unsafe { std::slice::from_raw_parts(value.as_ptr(), value.len()) };
                        Ok(true)
                    }
                    None => {
                        // Slow path: Value is in the default CF.
                        let start_ts = write.start_ts;
                        self.move_default_cursor_and_load_value(user_key, start_ts)?;
                        Ok(true)
                    }
                }
            }
            WriteType::Delete => Ok(false),
            WriteType::Lock | WriteType::Rollback => {
                // Slow path
                self.move_write_cursor_and_process_write(user_key)
            }
        }
    }

    /// Moves write cursor to `${user_key}_${ts}`. We first `next()` for a few times and if
    /// we have not reached where we want, then we use `seek()`.
    ///
    /// Returns false if the move attempt is failed, i.e. key space is ended or moved to another
    /// user key.
    fn move_write_cursor_to_ts(&mut self, user_key: &Key) -> Result<bool> {
        for _ in 0..SEEK_BOUND {
            if !self.write_cursor.next(&mut self.statistics.write) {
                // Key space ended.
                self.need_read_next_finalize = false;
                return Ok(false);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                self.need_read_next_finalize = false;
                return Ok(false);
            }
            if Key::decode_ts_from(current_key)? <= self.cfg.ts {
                // Founded a write with suitable ts.
                return Ok(true);
            }
        }

        // If we have not found `${user_key}_${ts}` in a few `next()`, directly `seek()`.
        // `user_key` must have reserved space here, so its clone has reserved space too. So no
        // reallocation happens in `append_ts`.
        if !self.write_cursor.seek(
            &user_key.clone().append_ts(self.cfg.ts),
            &mut self.statistics.write,
        )? {
            // Key space ended.
            self.need_read_next_finalize = false;
            return Ok(false);
        }

        let current_key = self.write_cursor.key(&mut self.statistics.write);
        if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
            // Meet another key.
            self.need_read_next_finalize = false;
            return Ok(false);
        }

        Ok(true)
    }

    /// Moves default cursor to `${user_key}_${start_ts}` and ensures that the user value can
    /// be accessed via `value()`.
    ///
    /// Returns error if the move attempt failed.
    fn move_default_cursor_and_load_value(
        &mut self,
        user_key: &Key,
        start_ts: TimeStamp,
    ) -> Result<()> {
        self.ensure_default_cursor()?;
        let default_cursor = self.default_cursor.as_mut().unwrap();
        let seek_key = user_key.clone().append_ts(start_ts);
        default_cursor.near_seek(&seek_key, &mut self.statistics.data)?;
        if !default_cursor.valid()?
            || default_cursor.key(&mut self.statistics.data) != seek_key.as_encoded().as_slice()
        {
            return Err(default_not_found_error(
                user_key.to_raw()?,
                "forward_scan_move_default_cursor_and_load_value",
            ));
        }
        self.statistics.data.processed += 1;
        let value = default_cursor.value(&mut self.statistics.data);
        self.unsafe_value_slice =
            unsafe { std::slice::from_raw_parts(value.as_ptr(), value.len()) };
        Ok(())
    }

    fn move_write_cursor_and_process_write(&mut self, user_key: &Key) -> Result<bool> {
        loop {
            if !self.write_cursor.next(&mut self.statistics.write) {
                // Key space ended.
                self.need_read_next_finalize = false;
                return Ok(false);
            }

            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                self.need_read_next_finalize = false;
                return Ok(false);
            }

            let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => {
                    if self.cfg.omit_value {
                        return Ok(true);
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            self.unsafe_value_slice =
                                unsafe { std::slice::from_raw_parts(value.as_ptr(), value.len()) };
                            return Ok(true);
                        }
                        None => {
                            // Value is in the default CF.
                            let start_ts = write.start_ts;
                            self.move_default_cursor_and_load_value(user_key, start_ts)?;
                            return Ok(true);
                        }
                    }
                }
                WriteType::Delete => return Ok(false),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }
        }
    }

    /// Finalizes and prepares for next `read_next()` call. After calling this function,
    /// the value of the last iterate will be cleared. The caller must supply the same `Key`
    /// that
    ///
    /// Internally, this function moves the cursor to point to next user key.
    pub fn read_next_finalize(&mut self, user_key: &Key) -> Result<()> {
        self.unsafe_value_slice = &[];

        if !self.need_read_next_finalize {
            return Ok(());
        }
        self.need_read_next_finalize = false;

        if !self.write_cursor.next(&mut self.statistics.write) {
            // Key space ended
            return Ok(());
        }

        let current_key = self.write_cursor.key(&mut self.statistics.write);
        if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
            // Found another user key. We are done here.
            return Ok(());
        }

        // Slow path
        self.move_write_cursor_to_next_user_key(user_key)
    }

    fn move_write_cursor_to_next_user_key(&mut self, user_key: &Key) -> Result<()> {
        self.need_read_next_finalize = false;

        for i in 0..SEEK_BOUND {
            if i > 0 {
                if !self.write_cursor.next(&mut self.statistics.write) {
                    // Key space ended.
                    return Ok(());
                }
            }
            if !self.write_cursor.valid()? {
                // Key space ended.
                return Ok(());
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Found another user key. We are done here.
                return Ok(());
            }
        }

        // We have not found another user key for now, so we directly `seek()`.
        // After that, we must pointing to another key, or out of bound.
        // `current_user_key` must have reserved space here, so its clone has reserved space too.
        // So no reallocation happens in `append_ts`.
        self.write_cursor.internal_seek(
            &user_key.clone().append_ts(TimeStamp::zero()),
            &mut self.statistics.write,
        )?;

        Ok(())
    }

    /// Retrieves the value in the last `read_next()` call. You can only retrieve a value
    /// after a successful `read_next()` call and before a `read_next_finalize()` call.
    #[inline]
    pub fn value(&self) -> &[u8] {
        self.unsafe_value_slice
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
}

/*
#[cfg(test)]
mod tests {
    use super::super::ScannerBuilder;
    use super::*;
    use crate::storage::mvcc::tests::*;
    use crate::storage::Scanner;
    use crate::storage::{Engine, Key, TestEngineBuilder};

    use kvproto::kvrpcpb::Context;

    /// Check whether everything works as usual when `ForwardScanner::get()` goes out of bound.
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
        let mut scanner = ScannerBuilder::new(snapshot, 10.into(), false)
            .range(None, None)
            .build()
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"a"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardScanner::move_write_cursor_to_next_user_key()` goes out of bound.
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
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into(), false)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"a"), b"a_value".to_vec())),
        );
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
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardScanner::move_write_cursor_to_next_user_key()` goes out of bound.
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
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into(), false)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_4 b_3 b_2 b_1
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"a"), b"a_value".to_vec())),
        );
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
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
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
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
            .range(None, Some(Key::from_raw(&[3u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
            .range(Some(Key::from_raw(&[5u8])), None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
            .range(None, None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
    }
}
*/
