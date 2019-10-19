// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A dedicate scanner that outputs content in each CF.

use engine::CF_DEFAULT;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::write::{Write, WriteType};
use crate::storage::mvcc::Result;
use crate::storage::txn::{Result as TxnResult, TxnEntry, TxnEntryScanner};
use crate::storage::{Cursor, Key, Snapshot, Statistics};

use super::ScannerConfig;

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
    ) -> DeltaScanner<S> {
        DeltaScanner {
            cfg,
            lock_cursor,
            write_cursor,
            statistics: Statistics::default(),
            default_cursor: None,
            is_started: false,
            begin_ts,
        }
    }
    pub fn read_next(&mut self) -> Result<Option<TxnEntry>> {
        self.check_and_start()?;
        if self.lock_cursor.is_none() {
            return self.read_next_write_key();
        }
        unimplemented!()
    }

    pub fn read_next_write_key(&mut self) -> Result<Option<TxnEntry>> {
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
                return Ok(Some(v));
            }
            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    ///
    /// The implementation is the same as `PointGetter::load_data_and_write`.
    #[inline]
    fn load_data_and_write(&mut self, user_key: &Key) -> Result<TxnEntry> {
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
        // Value is in the default CF.
        // Value is in the default CF.
        self.ensure_default_cursor()?;
        let mut default_cf = self.default_cursor.as_mut().unwrap();
        let value = super::super::util::near_load_data_by_write(
            &mut default_cf,
            user_key,
            write,
            &mut self.statistics,
        )?;
        Ok(TxnEntry::Commit {
            default: (default_cf.key(&mut self.statistics.data).to_vec(), value),
            write: w,
        })
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
        // TODO LockCF
        } else {
            self.write_cursor.seek_to_first(&mut self.statistics.write);
            // ToDO lockCF
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
    use crate::storage::{Engine, TestEngineBuilder};

    use kvproto::kvrpcpb::{Context, IsolationLevel};

    #[test]
    fn test_with_rc() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

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
            .build_delta(begin_ts)
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
}
