// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{borrow::Cow, cmp::Ordering};

use engine_traits::CF_DEFAULT;
use kvproto::kvrpcpb::{IsolationLevel, WriteConflictReason};
use txn_types::{Key, TimeStamp, Value, ValueEntry, Write, WriteRef, WriteType};

use super::ScannerConfig;
use crate::storage::{
    kv::{Cursor, SEEK_BOUND, Snapshot, Statistics},
    mvcc::{Error, ErrorInner::WriteConflict, NewerTsCheckState, Result},
    need_check_locks,
};

// When there are many versions for the user key, after several tries,
// we will use seek to locate the right position. But this will turn around
// the write cf's iterator's direction inside RocksDB, and the next user key
// need to turn back the direction to backward. As we have tested, turn around
// iterator's direction from forward to backward is as expensive as seek in
// RocksDB, so don't set REVERSE_SEEK_BOUND too small.
const REVERSE_SEEK_BOUND: u64 = 16;

/// This struct can be used to scan keys starting from the given user key in the
/// reverse order (less than).
///
/// Internally, for each key, rollbacks are ignored and smaller version will be
/// tried. If the isolation level is SI, locks will be checked first.
///
/// Use `ScannerBuilder` to build `BackwardKvScanner`.
pub struct BackwardKvScanner<S: Snapshot> {
    cfg: ScannerConfig<S>,
    lock_cursor: Option<Cursor<S::Iter>>,
    write_cursor: Cursor<S::Iter>,
    /// `default cursor` is lazy created only when it's needed.
    default_cursor: Option<Cursor<S::Iter>>,
    /// Is iteration started
    is_started: bool,
    statistics: Statistics,
    met_newer_ts_data: NewerTsCheckState,
}

impl<S: Snapshot> BackwardKvScanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Option<Cursor<S::Iter>>,
        write_cursor: Cursor<S::Iter>,
    ) -> BackwardKvScanner<S> {
        BackwardKvScanner {
            met_newer_ts_data: if cfg.check_has_newer_ts_data {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            },
            cfg,
            lock_cursor,
            write_cursor,
            statistics: Statistics::default(),
            default_cursor: None,
            is_started: false,
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }

    /// Whether we met newer ts data.
    /// The result is always `Unknown` if `check_has_newer_ts_data` is not set.
    #[inline]
    pub fn met_newer_ts_data(&self) -> NewerTsCheckState {
        self.met_newer_ts_data
    }

    /// Get the next key-value pair, in backward order.
    pub fn read_next(&mut self) -> Result<Option<(Key, ValueEntry)>> {
        if !self.is_started {
            if let Some(upper_bound) = &self.cfg.upper_bound {
                // TODO: `seek_to_last` is better, however it has performance issues currently.
                // TODO: We have no guarantee about whether or not the upper_bound has a
                // timestamp suffix, so currently it is not safe to change write_cursor's
                // reverse_seek to seek_for_prev. However in future, once we have different
                // types for them, this can be done safely.
                self.write_cursor
                    .reverse_seek(upper_bound, &mut self.statistics.write)?;
                if let Some(lock_cursor) = self.lock_cursor.as_mut() {
                    lock_cursor.reverse_seek(upper_bound, &mut self.statistics.lock)?;
                }
            } else {
                self.write_cursor.seek_to_last(&mut self.statistics.write);
                if let Some(lock_cursor) = self.lock_cursor.as_mut() {
                    lock_cursor.seek_to_last(&mut self.statistics.lock);
                }
            }
            self.is_started = true;
        }

        // Similar to forward scanner, the general idea is to simultaneously step write
        // cursor and lock cursor. Please refer to `ForwardKvScanner` for details.

        loop {
            let (current_user_key, mut has_write, has_lock) = {
                let w_key = if self.write_cursor.valid()? {
                    Some(self.write_cursor.key(&mut self.statistics.write))
                } else {
                    None
                };
                let l_key = if let Some(lock_cursor) = self.lock_cursor.as_mut() {
                    if lock_cursor.valid()? {
                        Some(lock_cursor.key(&mut self.statistics.lock))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // `res` is `(current_user_key_slice, has_write, has_lock)`
                let res = match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(lk)) => (lk, false, true),
                    (Some(wk), None) => (Key::truncate_ts_for(wk)?, true, false),
                    (Some(wk), Some(lk)) => {
                        let write_user_key = Key::truncate_ts_for(wk)?;
                        match write_user_key.cmp(lk) {
                            Ordering::Less => {
                                // We are scanning from largest user key to smallest user key, so
                                // this indicate that we meet a lock first, thus its corresponding
                                // write does not exist.
                                (lk, false, true)
                            }
                            Ordering::Greater => {
                                // We meet write first, so the lock of the write key does not exist.
                                (write_user_key, true, false)
                            }
                            Ordering::Equal => (write_user_key, true, true),
                        }
                    }
                };

                // Use `from_encoded_slice` to reserve space for ts, so later we can append ts
                // to the key or its clones without reallocation.
                (Key::from_encoded_slice(res.0), res.1, res.2)
            };

            let mut result = Ok(None);
            let mut met_prev_user_key = false;
            let ts = self.cfg.ts;

            if has_lock {
                if need_check_locks(self.cfg.isolation_level) {
                    let lock_or_shared_locks = {
                        let lock_value = self
                            .lock_cursor
                            .as_mut()
                            .unwrap()
                            .value(&mut self.statistics.lock);
                        txn_types::parse_lock(lock_value)?
                    };
                    if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                        self.met_newer_ts_data = NewerTsCheckState::Met;
                    }
                    result = txn_types::check_ts_conflict(
                        Cow::Borrowed(&lock_or_shared_locks),
                        &current_user_key,
                        ts,
                        &self.cfg.bypass_locks,
                        self.cfg.isolation_level,
                    )
                    .map(|_| None)
                    .map_err(Into::into);
                    if result.is_err() {
                        let lock = lock_or_shared_locks
                            .left()
                            .expect("Err result only for single lock");
                        self.statistics.lock.processed_keys += 1;
                        if !self.cfg.load_commit_ts && self.cfg.access_locks.contains(lock.ts) {
                            self.ensure_default_cursor()?;
                            result = super::load_data_by_lock(
                                &current_user_key,
                                &self.cfg,
                                self.default_cursor.as_mut().unwrap(),
                                lock,
                                &mut self.statistics,
                            )
                            .map(|v| v.map(|val| ValueEntry::new(val, None)));
                            if has_write {
                                // Skip current_user_key because this key is either blocked or
                                // handled.
                                has_write = false;
                                self.move_write_cursor_to_prev_user_key(&current_user_key)?;
                            }
                        }
                    }
                }
                if let Some(lock_cursor) = self.lock_cursor.as_mut() {
                    lock_cursor.prev(&mut self.statistics.lock);
                }
            }
            if has_write {
                if result.is_ok() {
                    result = self.reverse_get(&current_user_key, ts, &mut met_prev_user_key);
                }
                if !met_prev_user_key {
                    // Skip rest later versions and point to previous user key.
                    self.move_write_cursor_to_prev_user_key(&current_user_key)?;
                }
            }

            if let Some(v) = result? {
                self.statistics.write.processed_keys += 1;
                self.statistics.processed_size += current_user_key.len() + v.value.len();
                resource_metering::record_read_keys(1);
                return Ok(Some((current_user_key, v)));
            }
        }
    }

    /// Attempt to get the value of a key specified by `user_key` and
    /// `self.cfg.ts` in reverse order. This function requires that the write
    /// cursor is currently pointing to the earliest version of `user_key`.
    #[inline]
    fn reverse_get(
        &mut self,
        user_key: &Key,
        ts: TimeStamp,
        met_prev_user_key: &mut bool,
    ) -> Result<Option<ValueEntry>> {
        assert!(self.write_cursor.valid()?);

        // At first, we try to use several `prev()` to get the desired version.

        // We need to save last desired version, because when we may move to an unwanted
        // version at any time.
        let mut last_version = None;
        let mut loaded_commit_ts = None;
        let mut last_checked_commit_ts = TimeStamp::zero();

        for i in 0..REVERSE_SEEK_BOUND {
            if i > 0 {
                // We are already pointing at the smallest version, so we don't need to prev()
                // for the first iteration. So we will totally call `prev()` function
                // `REVERSE_SEEK_BOUND - 1` times.
                self.write_cursor.prev(&mut self.statistics.write);
                if !self.write_cursor.valid()? {
                    // Key space ended. We use `last_version` as the return.
                    return self.handle_last_version(last_version, user_key, loaded_commit_ts);
                }
            }

            let mut is_done = false;
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                last_checked_commit_ts = Key::decode_ts_from(current_key)?;

                if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                    // Meet another key, use `last_version` as the return.
                    *met_prev_user_key = true;
                    is_done = true;
                } else if last_checked_commit_ts > ts {
                    // Meet an unwanted version, use `last_version` as the return as well.
                    is_done = true;
                    if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                        self.met_newer_ts_data = NewerTsCheckState::Met;
                    }
                    if self.cfg.isolation_level == IsolationLevel::RcCheckTs {
                        // TODO: the more write recent version with `LOCK` or `ROLLBACK` write type
                        //       could be skipped.
                        return Err(WriteConflict {
                            start_ts: self.cfg.ts,
                            conflict_start_ts: Default::default(),
                            conflict_commit_ts: last_checked_commit_ts,
                            key: current_key.into(),
                            primary: vec![],
                            reason: WriteConflictReason::RcCheckTs,
                        }
                        .into());
                    }
                }
            }
            if is_done {
                return self.handle_last_version(last_version, user_key, loaded_commit_ts);
            }

            let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))
                .map_err(Error::from)?;

            match write.write_type {
                WriteType::Put | WriteType::Delete => {
                    last_version = Some(write.to_owned());
                    if self.cfg.load_commit_ts {
                        loaded_commit_ts = Some(last_checked_commit_ts);
                    }
                }
                WriteType::Lock | WriteType::Rollback => {}
            }
        }

        // At this time, we must have current commit_ts <= ts. If commit_ts == ts,
        // we don't need to seek any more and we can just utilize `last_version`.
        if last_checked_commit_ts == ts {
            if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                // move cursor backward again to check whether there are larger ts.
                self.write_cursor.prev(&mut self.statistics.write);
                if self.write_cursor.valid()? {
                    let current_key = self.write_cursor.key(&mut self.statistics.write);
                    if Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                        self.met_newer_ts_data = NewerTsCheckState::Met;
                    } else {
                        *met_prev_user_key = true;
                    }
                }
            }
            return self.handle_last_version(last_version, user_key, loaded_commit_ts);
        }
        assert!(ts > last_checked_commit_ts);

        // After several `prev()`, we still not get the latest version for the specified
        // ts, use seek to locate the latest version.

        // Check whether newer version exists.
        let mut use_near_seek = false;
        let mut seek_key = user_key.clone();

        if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
            seek_key = seek_key.append_ts(TimeStamp::max());
            self.write_cursor
                .internal_seek(&seek_key, &mut self.statistics.write)?;
            assert!(self.write_cursor.valid()?);
            seek_key = seek_key.truncate_ts()?;
            use_near_seek = true;

            let current_key = self.write_cursor.key(&mut self.statistics.write);
            debug_assert!(Key::is_user_key_eq(
                current_key,
                user_key.as_encoded().as_slice()
            ));
            let key_ts = Key::decode_ts_from(current_key)?;
            if key_ts > ts {
                self.met_newer_ts_data = NewerTsCheckState::Met
            }
        }

        // `user_key` must have reserved space here, so its clone `seek_key` has
        // reserved space too. Thus no reallocation happens in `append_ts`.
        seek_key = seek_key.append_ts(ts);
        if use_near_seek {
            self.write_cursor
                .near_seek(&seek_key, &mut self.statistics.write)?;
        } else {
            self.write_cursor
                .internal_seek(&seek_key, &mut self.statistics.write)?;
        }
        assert!(self.write_cursor.valid()?);

        loop {
            // After seek, or after some `next()`, we may reach `last_checked_commit_ts`
            // again. It means we have checked all versions for this user key.
            // We use `last_version` as return.
            let current_ts = {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                // We should never reach another user key.
                debug_assert!(Key::is_user_key_eq(
                    current_key,
                    user_key.as_encoded().as_slice()
                ));
                Key::decode_ts_from(current_key)?
            };
            if current_ts <= last_checked_commit_ts {
                // We reach the last handled key
                return self.handle_last_version(last_version, user_key, loaded_commit_ts);
            }

            let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;

            if !write.check_gc_fence_as_latest_version(self.cfg.ts) {
                return Ok(None);
            }

            match write.write_type {
                WriteType::Put => {
                    if self.cfg.load_commit_ts {
                        loaded_commit_ts = Some(current_ts);
                    }
                    let write = write.to_owned();
                    return Ok(Some(ValueEntry::new(
                        self.reverse_load_data_by_write(write, user_key)?,
                        loaded_commit_ts,
                    )));
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                    self.write_cursor.next(&mut self.statistics.write);
                    assert!(self.write_cursor.valid()?);
                }
            }
        }
    }

    /// Handle last version. Last version may be PUT or DELETE. If it is a PUT,
    /// value should be load.
    #[inline]
    fn handle_last_version(
        &mut self,
        some_write: Option<Write>,
        user_key: &Key,
        commit_ts: Option<TimeStamp>,
    ) -> Result<Option<ValueEntry>> {
        match some_write {
            None => Ok(None),
            Some(write) => {
                if !write.as_ref().check_gc_fence_as_latest_version(self.cfg.ts) {
                    return Ok(None);
                }
                match write.write_type {
                    WriteType::Put => Ok(Some(ValueEntry::new(
                        self.reverse_load_data_by_write(write, user_key)?,
                        commit_ts,
                    ))),
                    WriteType::Delete => Ok(None),
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Load the value by the given `some_write`. If value is carried in
    /// `some_write`, it will be returned directly. Otherwise there will be a
    /// default CF look up.
    ///
    /// The implementation is similar to `PointGetter::load_data_by_write`.
    #[inline]
    fn reverse_load_data_by_write(&mut self, write: Write, user_key: &Key) -> Result<Value> {
        if self.cfg.omit_value {
            return Ok(vec![]);
        }
        match write.short_value {
            Some(value) => {
                // Value is carried in `write`.
                Ok(value)
            }
            None => {
                // Value is in the default CF.
                self.ensure_default_cursor()?;
                let value = super::near_reverse_load_data_by_write(
                    self.default_cursor.as_mut().unwrap(),
                    user_key,
                    write.start_ts,
                    &mut self.statistics,
                )?;
                Ok(value)
            }
        }
    }

    /// After `self.reverse_get()`, our write cursor may be pointing to current
    /// user key (if we found a desired version), or previous user key (if there
    /// is no desired version), or out of bound.
    ///
    /// If it is pointing to current user key, we need to step it until we meet
    /// a new key. We first try to `prev()` a few times. If still not reaching
    /// another user key, we `seek_for_prev()`.
    #[inline]
    fn move_write_cursor_to_prev_user_key(&mut self, current_user_key: &Key) -> Result<()> {
        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write_cursor.prev(&mut self.statistics.write);
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

        self.statistics.write.over_seek_bound += 1;
        // We have not found another user key for now, so we directly `seek_for_prev()`.
        // After that, we must pointing to another key, or out of bound.
        self.write_cursor
            .internal_seek_for_prev(current_user_key, &mut self.statistics.write)?;

        Ok(())
    }

    /// Create the default cursor if it doesn't exist.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        self.default_cursor = Some(self.cfg.create_cf_cursor(CF_DEFAULT)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{CF_LOCK, CF_WRITE};
    use kvproto::kvrpcpb::Context;

    use super::{
        super::{ScannerBuilder, test_util::prepare_test_data_for_check_gc_fence},
        *,
    };
    use crate::storage::{
        Scanner,
        kv::{Engine, Modify, TestEngineBuilder},
        mvcc::tests::write,
        txn::tests::{
            must_acquire_pessimistic_lock, must_commit, must_gc, must_prewrite_delete,
            must_prewrite_lock, must_prewrite_put, must_rollback,
        },
    };

    #[test]
    fn test_basic() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let ctx = Context::default();
        // Generate REVERSE_SEEK_BOUND / 2 Put for key [10].
        let k = &[10_u8];
        for ts in 1..=REVERSE_SEEK_BOUND / 2 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            must_commit(&mut engine, k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND + 1 Put for key [9].
        let k = &[9_u8];
        for ts in 1..=REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            must_commit(&mut engine, k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND / 2 Put and REVERSE_SEEK_BOUND / 2 + 1 Rollback
        // for key [8].
        let k = &[8_u8];
        for ts in 1..=REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            if ts < REVERSE_SEEK_BOUND / 2 + 1 {
                must_commit(&mut engine, k, ts, ts);
            } else {
                let modifies = vec![
                    // ts is rather small, so it is ok to `as u8`
                    Modify::Put(
                        CF_WRITE,
                        Key::from_raw(k).append_ts(TimeStamp::new(ts)),
                        vec![b'R', ts as u8],
                    ),
                    Modify::Delete(CF_LOCK, Key::from_raw(k)),
                ];
                write(&engine, &ctx, modifies);
            }
        }

        // Generate REVERSE_SEEK_BOUND / 2 Put, 1 Delete and REVERSE_SEEK_BOUND / 2
        // Rollback for key [7].
        let k = &[7_u8];
        for ts in 1..=REVERSE_SEEK_BOUND / 2 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            must_commit(&mut engine, k, ts, ts);
        }
        {
            let ts = REVERSE_SEEK_BOUND / 2 + 1;
            must_prewrite_delete(&mut engine, k, k, ts);
            must_commit(&mut engine, k, ts, ts);
        }
        for ts in REVERSE_SEEK_BOUND / 2 + 2..=REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(k).append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(k)),
            ];
            write(&engine, &ctx, modifies);
        }

        // Generate 1 PUT for key [6].
        let k = &[6_u8];
        for ts in 1..2 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            must_commit(&mut engine, k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND + 1 Rollback for key [5].
        let k = &[5_u8];
        for ts in 1..=REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(k).append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(k)),
            ];
            write(&engine, &ctx, modifies);
        }

        // Generate 1 PUT with ts = REVERSE_SEEK_BOUND and 1 PUT
        // with ts = REVERSE_SEEK_BOUND + 1 for key [4].
        let k = &[4_u8];
        for ts in REVERSE_SEEK_BOUND + 1..REVERSE_SEEK_BOUND + 3 {
            must_prewrite_put(&mut engine, k, &[ts as u8], k, ts);
            must_commit(&mut engine, k, ts, ts);
        }

        // Assume REVERSE_SEEK_BOUND == 4, we have keys:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (REVERSE_SEEK_BOUND + 1).into())
            .desc(true)
            .range(None, Some(Key::from_raw(&[11_u8])))
            .build()
            .unwrap();

        // Initial position: 1 seek_for_prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                                  ^cursor
        // When get key [10]: REVERSE_SEEK_BOUND / 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                             ^cursor
        //                                               ^last_version
        // After get key [10]: 0 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                             ^
        assert_eq!(
            scanner.next().unwrap(),
            Some((
                Key::from_raw(&[10_u8]),
                vec![(REVERSE_SEEK_BOUND / 2) as u8]
            ))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize / 2);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.seek_for_prev, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(&[10_u8]).len() + vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8].len()
        );

        // Before get key [9]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                             ^cursor
        // When get key [9]:
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                       ^cursor
        //                                       ^last_version
        //                                       ^last_handled_key
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                     ^cursor
        //                                       ^last_handled_key
        // Now we got key[9].
        // After get key [9]: 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                   ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[9_u8]), vec![REVERSE_SEEK_BOUND as u8 + 1]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(&[9_u8]).len() + vec![(REVERSE_SEEK_BOUND) as u8].len()
        );

        // Before get key [8]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                   ^cursor
        // When get key [8]:
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                             ^cursor
        //                                 ^last_version
        //                             ^last_handled_key
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                           ^cursor
        //                                 ^last_version
        //                             ^last_handled_key
        // Got ROLLBACK, so 1 next:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                             ^cursor
        //                                 ^last_version
        //                             ^last_handled_key
        // Hit last_handled_key, so use last_version and we get key [8].
        // After get key [8]: 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                         ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[8_u8]), vec![(REVERSE_SEEK_BOUND / 2) as u8]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize + 1);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(&[8_u8]).len() + vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8].len()
        );

        // Before get key [7]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                         ^cursor
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                   ^cursor
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                 ^cursor
        // Got ROLLBACK, so 1 next:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                   ^cursor
        // Hit last_handled_key, so use last_version and we get None.
        // Skip this key's versions and go to next key: 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //               ^cursor
        // Current commit ts > ts is not satisfied, so 1 prev:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //             ^cursor
        //               ^last_version
        // We reached another key, use last_version and we get [6].
        // After get key [6]: 0 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //             ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6_u8]), vec![1_u8]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize + 2);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.processed_size, 10);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(&[6_u8]).len() + vec![0_u8].len()
        );

        // Before get key [5]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //             ^cursor
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //       ^cursor (last_version == None)
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //     ^cursor (last_version == None)
        // Got ROLLBACK, so 1 next:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //       ^cursor (last_version == None)
        // Hit last_handled_key, so use last_version and we get None.
        // Skip this key's versions and go to next key: 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //   ^cursor
        // Current commit ts > ts is not satisfied, so 1 prev:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        // ^cursor
        //   ^last_version
        // Current commit ts > ts is satisfied, use last_version and we get [4].
        // After get key [4]: 1 prev
        //   4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        // ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4_u8]), vec![REVERSE_SEEK_BOUND as u8 + 1]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize + 3);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(&[4_u8]).len() + vec![REVERSE_SEEK_BOUND as u8].len()
        );

        // Scan end.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `BackwardKvScanner::reverse_get()` goes out of bound.
    ///
    /// Case 1. prev out of bound, next_version is None.
    #[test]
    fn test_reverse_get_out_of_bound_1() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        // Generate N/2 rollback for [b].
        for ts in 0..REVERSE_SEEK_BOUND / 2 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        // Generate 1 put for [c].
        must_prewrite_put(&mut engine, b"c", b"value", b"c", REVERSE_SEEK_BOUND * 2);
        must_commit(
            &mut engine,
            b"c",
            REVERSE_SEEK_BOUND * 2,
            REVERSE_SEEK_BOUND * 2,
        );

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (REVERSE_SEEK_BOUND * 2).into())
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that REVERSE_SEEK_BOUND = 4.

        // Initial position: 1 seek_to_last:
        //   b_1 b_0 c_8
        //           ^cursor
        // Got ts == specified ts, advance the cursor.
        //   b_1 b_0 c_8
        //       ^cursor
        // Now we get [c]. Trying to reach prev user key: no operation needed.
        //   b_1 b_0 c_8
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"c"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"c").len() + b"value".len()
        );

        // Use N/2 prev and reach out of bound:
        //   b_1 b_0 c_8
        // ^cursor
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, (REVERSE_SEEK_BOUND / 2) as usize);
        assert_eq!(statistics.processed_size, 0);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `BackwardKvScanner::reverse_get()` goes out of bound.
    ///
    /// Case 2. prev out of bound, next_version is Some.
    #[test]
    fn test_reverse_get_out_of_bound_2() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        // Generate 1 put and N/2 rollback for [b].
        must_prewrite_put(&mut engine, b"b", b"value_b", b"b", 0);
        must_commit(&mut engine, b"b", 0, 0);
        for ts in 1..=REVERSE_SEEK_BOUND / 2 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        // Generate 1 put for [c].
        must_prewrite_put(&mut engine, b"c", b"value_c", b"c", REVERSE_SEEK_BOUND * 2);
        must_commit(
            &mut engine,
            b"c",
            REVERSE_SEEK_BOUND * 2,
            REVERSE_SEEK_BOUND * 2,
        );

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (REVERSE_SEEK_BOUND * 2).into())
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that REVERSE_SEEK_BOUND = 4.

        // Initial position: 1 seek_to_last:
        //   b_2 b_1 b_0 c_8
        //               ^cursor
        // Got ts == specified ts, advance the cursor.
        //   b_2 b_1 b_0 c_8
        //           ^cursor
        // Now we get [c]. Trying to reach prev user key: no operation needed.
        //   b_2 b_1 b_0 c_8
        //           ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"c"), b"value_c".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"c").len() + b"value_c".len()
        );

        // Use N/2+1 prev and reach out of bound:
        //   b_2 b_1 b_0 c_8
        // ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"value_b".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, (REVERSE_SEEK_BOUND / 2 + 1) as usize);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"b").len() + b"value_b".len()
        );

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `BackwardKvScanner::move_write_cursor_to_prev_user_key()` goes out of
    /// bound.
    ///
    /// Case 1. prev() out of bound
    #[test]
    fn test_move_prev_user_key_out_of_bound_1() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [c].
        must_prewrite_put(&mut engine, b"c", b"value", b"c", 1);
        must_commit(&mut engine, b"c", 1, 1);

        // Generate N/2 put for [b] .
        for ts in 1..=SEEK_BOUND / 2 {
            must_prewrite_put(&mut engine, b"b", &[ts as u8], b"b", ts);
            must_commit(&mut engine, b"b", ts, ts);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 1.into())
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_last:
        //   b_2 b_1 c_1
        //           ^cursor
        // Got ts == specified ts, advance the cursor.
        //   b_2 b_1 c_1
        //       ^cursor
        // Now we get [c]. Trying to reach prev user key: no operation needed.
        //   b_2 b_1 c_1
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"c"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"c").len() + b"value".len()
        );

        // Before:
        //   b_2 b_1 c_1
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use N/2 prev to reach prev key and stop:
        //   b_2 b_1 c_1
        // ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), vec![1u8])),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, (SEEK_BOUND / 2) as usize);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"b").len() + vec![1u8].len()
        );

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `BackwardKvScanner::move_write_cursor_to_prev_user_key()` goes out of
    /// bound.
    ///
    /// Case 2. seek_for_prev() out of bound
    #[test]
    fn test_move_prev_user_key_out_of_bound_2() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [c].
        must_prewrite_put(&mut engine, b"c", b"value", b"c", 1);
        must_commit(&mut engine, b"c", 1, 1);

        // Generate N+1 put for [b] .
        for ts in 1..SEEK_BOUND + 2 {
            must_prewrite_put(&mut engine, b"b", &[ts as u8], b"b", ts);
            must_commit(&mut engine, b"b", ts, ts);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 1.into())
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_last:
        //   b_5 b_4 b_3 b_2 b_1 c_1
        //                       ^cursor
        // Got ts == specified ts, advance the cursor.
        //   b_5 b_4 b_3 b_2 b_1 c_1
        //                   ^cursor
        // Now we get [c]. Trying to reach prev user key: no operation needed.
        //   b_5 b_4 b_3 b_2 b_1 c_1
        //                   ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"c"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"c").len() + b"value".len()
        );

        // Before:
        //   b_5 b_4 b_3 b_2 b_1 c_1
        //                   ^cursor
        // Got ts == specified ts, advance the cursor.
        //   b_5 b_4 b_3 b_2 b_1 c_1
        //               ^cursor
        // Now we get [b].
        // After get the value, use N-1 prev: (TODO: Should be SEEK_BOUND)
        //   b_5 b_4 b_3 b_2 b_1 c_1
        //   ^cursor
        // Then, use seek_for_prev:
        //   b_5 b_4 b_3 b_2 b_1 c_1
        // ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), vec![1u8])),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 1);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, SEEK_BOUND as usize);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"b").len() + vec![1u8].len()
        );

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `BackwardKvScanner::move_write_cursor_to_prev_user_key()` goes out of
    /// bound.
    ///
    /// Case 3. a more complicated case
    #[test]
    fn test_move_prev_user_key_out_of_bound_3() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // N denotes for SEEK_BOUND, M denotes for REVERSE_SEEK_BOUND

        // Generate 1 put for [c].
        must_prewrite_put(&mut engine, b"c", b"value", b"c", 1);
        must_commit(&mut engine, b"c", 1, 1);

        // Generate N+M+1 put for [b] .
        for ts in 1..SEEK_BOUND + REVERSE_SEEK_BOUND + 2 {
            must_prewrite_put(&mut engine, b"b", &[ts as u8], b"b", ts);
            must_commit(&mut engine, b"b", ts, ts);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (REVERSE_SEEK_BOUND + 1).into())
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4,
        // REVERSE_SEEK_BOUND = 6.

        // Initial position: 1 seek_to_last:
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //                                                 ^cursor
        // Got ts < specified ts, advance the cursor.
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //                                             ^cursor
        // Now we get [c]. Trying to reach prev user key: no operation needed.
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //                                             ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"c"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"c").len() + b"value".len()
        );

        // Before:
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //                                             ^cursor
        // Use M-1 prev trying to get specified version:
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //                         ^cursor
        // Possible more version, so use seek:
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //                     ^cursor
        // Now we got wanted value.
        // After get the value, use N-1 prev trying to reach prev user key:
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        //        ^cursor
        // Then, use seek_for_prev:
        //   b_11 b_10 b_9 b_8 b_7 b_6 b_5 b_4 b_3 b_2 b_1 c_1
        // ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), vec![(REVERSE_SEEK_BOUND + 1) as u8])),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.seek_for_prev, 1);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(
            statistics.write.prev,
            (REVERSE_SEEK_BOUND - 1 + SEEK_BOUND - 1) as usize
        );
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"b").len() + vec![(REVERSE_SEEK_BOUND + 1) as u8].len()
        );

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    #[test]
    fn test_load_commit_ts_with_top_lock_versions() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // One visible PUT and many newer committed LOCK writes on top.
        must_prewrite_put(&mut engine, b"k", b"v", b"k", 1);
        must_commit(&mut engine, b"k", 1, 1);
        for ts in 2..=(REVERSE_SEEK_BOUND + 4) {
            must_prewrite_lock(&mut engine, b"k", b"k", ts);
            must_commit(&mut engine, b"k", ts, ts);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (REVERSE_SEEK_BOUND + 2).into())
            .desc(true)
            .range(None, None)
            .set_load_commit_ts(true)
            .build()
            .unwrap();

        let (key, entry) = scanner.next_entry().unwrap().unwrap();
        assert_eq!(key, Key::from_raw(b"k"));
        assert_eq!(entry.value, b"v".to_vec());
        // commit_ts should come from the returned PUT version, not newer LOCK versions.
        assert_eq!(entry.commit_ts.unwrap().into_inner(), 1);
        assert!(scanner.next_entry().unwrap().is_none());
    }

    /// Range is left open right closed.
    #[test]
    fn test_range() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [1], [2] ... [6].
        for i in 1..7 {
            // ts = 1: value = []
            must_prewrite_put(&mut engine, &[i], &[], &[i], 1);
            must_commit(&mut engine, &[i], 1, 1);

            // ts = 7: value = [ts]
            must_prewrite_put(&mut engine, &[i], &[i], &[i], 7);
            must_commit(&mut engine, &[i], 7, 7);

            // ts = 14: value = []
            must_prewrite_put(&mut engine, &[i], &[], &[i], 14);
            must_commit(&mut engine, &[i], 14, 14);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .desc(true)
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            Key::from_raw(&[4u8]).len()
                + vec![4u8].len()
                + Key::from_raw(&[3u8]).len()
                + vec![3u8].len()
        );

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .desc(true)
            .range(None, Some(Key::from_raw(&[3u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            Key::from_raw(&[2u8]).len()
                + vec![2u8].len()
                + Key::from_raw(&[1u8]).len()
                + vec![1u8].len()
        );

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .desc(true)
            .range(Some(Key::from_raw(&[5u8])), None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            Key::from_raw(&[6u8]).len()
                + vec![6u8].len()
                + Key::from_raw(&[5u8]).len()
                + vec![5u8].len()
        );

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            (1u8..=6u8)
                .rev()
                .map(|i| Key::from_raw(&[i]).len() + vec![i].len())
                .sum::<usize>()
        );
    }

    #[test]
    fn test_many_tombstones() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // Generate RocksDB tombstones in write cf.
        let start_ts = 1;
        let safe_point = 2;
        for i in 0..16 {
            for y in 0..16 {
                let pk = &[i as u8, y as u8];
                must_prewrite_put(&mut engine, pk, b"", pk, start_ts);
                must_rollback(&mut engine, pk, start_ts, false);
                // Generate 254 RocksDB tombstones between [0,0] and [15,15].
                if !((i == 0 && y == 0) || (i == 15 && y == 15)) {
                    must_gc(&mut engine, pk, safe_point);
                }
            }
        }

        // Generate 16 locks in lock cf.
        let start_ts = 3;
        for i in 0..16 {
            let pk = &[i as u8];
            must_prewrite_put(&mut engine, pk, b"", pk, start_ts);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let row = &[15_u8];
        let k = Key::from_raw(row);

        // Call reverse scan
        let ts = 2.into();
        let mut scanner = ScannerBuilder::new(snapshot, ts)
            .desc(true)
            .range(None, Some(k))
            .build()
            .unwrap();
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.lock.prev, 15);
        assert_eq!(statistics.write.prev, 1);
        assert_eq!(scanner.take_statistics().processed_size, 0);
    }

    #[test]
    fn test_backward_scanner_check_gc_fence() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (read_ts, expected_result) = prepare_test_data_for_check_gc_fence(&mut engine);
        let expected_result: Vec<_> = expected_result
            .into_iter()
            .filter_map(|(key, value)| value.map(|v| (key, v)))
            .rev()
            .collect();

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, read_ts)
            .desc(true)
            .range(None, None)
            .build()
            .unwrap();
        let result: Vec<_> = scanner
            .scan(100, 0)
            .unwrap()
            .into_iter()
            .map(|result| result.unwrap())
            .collect();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_rc_read_check_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key0, val0) = (b"k0", b"v0");
        must_prewrite_put(&mut engine, key0, val0, key0, 60);

        let (key1, val1) = (b"k1", b"v1");
        must_prewrite_put(&mut engine, key1, val1, key1, 25);
        must_commit(&mut engine, key1, 25, 30);

        let (key2, val2, val22) = (b"k2", b"v2", b"v22");
        must_prewrite_put(&mut engine, key2, val2, key2, 6);
        must_commit(&mut engine, key2, 6, 9);
        must_prewrite_put(&mut engine, key2, val22, key2, 10);
        must_commit(&mut engine, key2, 10, 20);

        let (key3, val3) = (b"k3", b"v3");
        must_prewrite_put(&mut engine, key3, val3, key3, 5);
        must_commit(&mut engine, key3, 5, 6);

        let (key4, val4) = (b"k4", b"val4");
        must_prewrite_put(&mut engine, key4, val4, key4, 3);
        must_commit(&mut engine, key4, 3, 4);
        must_prewrite_lock(&mut engine, key4, key4, 5);

        let (key5, val5) = (b"k5", b"val5");
        must_prewrite_put(&mut engine, key5, val5, key5, 1);
        must_commit(&mut engine, key5, 1, 2);
        must_acquire_pessimistic_lock(&mut engine, key5, key5, 3, 3);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 29.into())
            .range(None, None)
            .desc(true)
            .isolation_level(IsolationLevel::RcCheckTs)
            .build()
            .unwrap();

        // Scanner has met a more recent version.
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key5), val5.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key4), val4.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key3), val3.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key2), val22.to_vec()))
        );
        scanner.next().unwrap_err();

        // Scanner has met a lock though lock.ts > read_ts.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 55.into())
            .range(None, None)
            .desc(true)
            .isolation_level(IsolationLevel::RcCheckTs)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key5), val5.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key4), val4.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key3), val3.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key2), val22.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key1), val1.to_vec()))
        );
        scanner.next().unwrap_err();
    }
}
