// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine::CF_HISTORY;

use engine::CF_DEFAULT;
use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::reader::util::{check_lock, CheckLockResult};
use crate::storage::mvcc::write::{WriteRef, WriteType};
use crate::storage::mvcc::{Result, TimeStamp};
use crate::storage::{Cursor, Key, Lock, Snapshot, Statistics, Value};

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
    latest_cursor: Cursor<S::Iter>,
    /// `history cursor` is lazy created only when it's needed.
    history_cursor: Option<Cursor<S::Iter>>,
    history_valid: bool,
    /// Is iteration started
    is_started: bool,
    statistics: Statistics,
}

impl<S: Snapshot> ForwardScanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
        latest_cursor: Cursor<S::Iter>,
    ) -> ForwardScanner<S> {
        ForwardScanner {
            cfg,
            lock_cursor,
            latest_cursor,
            statistics: Statistics::default(),
            history_cursor: None,
            history_valid: true,
            is_started: false,
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }

    pub fn check_locks(&mut self) -> Result<()> {
        if self.cfg.lower_bound.is_some() {
            self.lock_cursor.seek(
                self.cfg.lower_bound.as_ref().unwrap(),
                &mut self.statistics.lock,
            )?;
        } else {
            self.lock_cursor.seek_to_first(&mut self.statistics.lock);
        }

        loop {
            if self.lock_cursor.valid()? {
                let current_user_key =
                    Key::from_encoded_slice(self.lock_cursor.key(&mut self.statistics.lock));
                let lock = Lock::parse(self.lock_cursor.value(&mut self.statistics.lock))?;
                if let CheckLockResult::Locked(e) =
                    super::super::util::check_lock(&current_user_key, self.cfg.ts, &lock)?
                {
                    return Err(e.into());
                }
            } else {
                return Ok(());
            }
            self.lock_cursor.next(&mut self.statistics.lock);
        }
    }

    fn ensure_history_cursor(&mut self) -> Result<()> {
        if self.history_cursor.is_some() {
            Ok(())
        } else {
            self.history_cursor = Some(self.cfg.create_cf_cursor(CF_HISTORY)?);
            Ok(())
        }
    }

    /// Get the next key-value pair, in forward order.
    pub fn read_next(&mut self) -> Result<Option<(Key, Value)>> {
        if !self.is_started {
            if self.cfg.lower_bound.is_some() {
                // TODO: `seek_to_first` is better, however it has performance issues currently.
                self.latest_cursor.seek(
                    self.cfg.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.write,
                )?;
            } else {
                self.latest_cursor.seek_to_first(&mut self.statistics.write);
            }
            self.is_started = true;
        }

        let mut found = false;
        loop {
            if self.latest_cursor.valid()? {
                let key =
                    Key::from_encoded_slice(self.latest_cursor.key(&mut self.statistics.latest));
                let mut value = None;
                let mut latest =
                    WriteRef::parse(self.latest_cursor.value(&mut self.statistics.latest))?;
                if self.cfg.ts >= latest.commit_ts {
                    if latest.write_type == WriteType::Put {
                        found = true;
                        value = latest.copy_value();
                    }
                } else if self.history_valid {
                    // seek from history
                    self.ensure_history_cursor()?;
                    let seek_key = key.clone().append_ts(self.cfg.ts);
                    self.history_cursor
                        .as_mut()
                        .unwrap()
                        .near_seek(&seek_key, &mut self.statistics.history)?;
                    if self.history_cursor.as_ref().unwrap().valid()? {
                        let history_key = self
                            .history_cursor
                            .as_ref()
                            .unwrap()
                            .key(&mut self.statistics.history);
                        if Key::is_user_key_eq(history_key, key.as_encoded()) {
                            let mut history = WriteRef::parse(
                                self.history_cursor
                                    .as_ref()
                                    .unwrap()
                                    .value(&mut self.statistics.history),
                            )?;
                            if history.write_type == WriteType::Put {
                                found = true;
                                value = history.copy_value();
                            }
                        }
                    } else {
                        self.history_valid = false;
                    }
                }
                // move to next key
                self.latest_cursor.next(&mut self.statistics.latest);
                if found {
                    return Ok(Some((key, value.unwrap())));
                }
            } else {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ScannerBuilder;
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
