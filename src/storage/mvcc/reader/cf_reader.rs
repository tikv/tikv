// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use storage::engine::ScanMode;
use storage::mvcc::Result;
use storage::mvcc::{Lock, Write, WriteType};
use storage::{Cursor, Snapshot, Statistics, CF_DEFAULT, CF_LOCK, CF_WRITE};
use storage::{Key, Value};

/// `CFReader` factory.
pub struct CFReaderBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
}

impl<S: Snapshot> CFReaderBuilder<S> {
    /// Initialize a new `CFReaderBuilder`.
    pub fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            fill_cache: true,
        }
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    /// Build `CFReader` from the current configuration.
    pub fn build(self) -> Result<CFReader<S>> {
        Ok(CFReader {
            snapshot: self.snapshot,
            fill_cache: self.fill_cache,
            statistics: Statistics::default(),

            lock_cursor: None,
            write_cursor: None,
            reverse_write_cursor: None,
            default_cursor: None,
        })
    }
}

/// A handy utility around functions in `mvcc::reader::util`. This struct does not provide
/// performance guarantee. Please carefully review each interface's requirement.
///
/// Use `CFReaderBuilder` to build `CFReader`.
pub struct CFReader<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,

    statistics: Statistics,

    lock_cursor: Option<Cursor<S::Iter>>,
    write_cursor: Option<Cursor<S::Iter>>,
    // TODO: If there is already a write_cursor, which approach is faster?
    // - convert it to a reverse cursor and use it
    // - create a new cursor specifically for reverse iteration
    reverse_write_cursor: Option<Cursor<S::Iter>>,
    default_cursor: Option<Cursor<S::Iter>>,
}

impl<S: Snapshot> CFReader<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the lock of a user key in the lock CF.
    ///
    /// Internally, there is a db `get`.
    #[inline]
    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        // TODO: `load_lock` should respect `fill_cache` options as well.
        super::util::load_lock(&self.snapshot, key, &mut self.statistics)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be checked to
    /// see whether it conflicts with the given `ts`. If there is no conflict or no lock,
    /// the safe `ts` will be returned.
    ///
    /// Internally, there is a db `get`.
    #[inline]
    pub fn load_and_check_lock(&mut self, key: &Key, ts: u64) -> Result<u64> {
        super::util::load_and_check_lock(&self.snapshot, key, ts, &mut self.statistics)
    }

    /// Iterate and get all user keys in the write CF within the given key space
    /// (specified by `start_key` and `limit`). `limit` must not be `0`.
    ///
    /// Internally, there will be a `seek` operation for the first iteration and
    /// `near_seek` for other iterations.
    ///
    /// The return type is `(keys, next_start_key)`. `next_start_key` is the `start_key`
    /// that can be used to continue scanning keys. If `next_start_key` is `None`, it means
    /// that there is no more keys.
    ///
    /// # Panics
    ///
    /// Panics if `limit` is `0`.
    pub fn scan_keys(
        &mut self,
        start_key: Option<&Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        self.ensure_write_cursor()?;
        let write_cursor = self.write_cursor.as_mut().unwrap();

        // TODO: We need to ensure that cursor is not prefix seek.
        assert!(limit > 0);

        let ok = match start_key {
            Some(ref x) => write_cursor.seek(x, &mut self.statistics.write)?,
            None => write_cursor.seek_to_first(&mut self.statistics.write),
        };
        if !ok {
            return Ok((vec![], None));
        }
        let mut keys = Vec::with_capacity(limit);
        let mut next_start_key;
        loop {
            // TODO: We don't really need to copy slice to a vector here.
            let key = Key::from_encoded(write_cursor.key(&mut self.statistics.write).to_vec())
                .truncate_ts()?;
            // Jump to the last version of the key. We assumed that there is no key that ts == 0.
            next_start_key = Some(key.clone().append_ts(0)); // TODO: Eliminate clone (might not be possible?)
            keys.push(key);
            if !write_cursor.near_seek(
                next_start_key.as_ref().unwrap(),
                false,
                &mut self.statistics.write,
            )? {
                // No more keys found, we don't need to scan keys next time
                next_start_key = None;
                break;
            }
            if keys.len() >= limit {
                // Reach limit
                break;
            }
        }
        self.statistics.write.processed += keys.len();
        Ok((keys, next_start_key))
    }

    /// Iterate and get all locks in the lock CF that `predicate` returns `true` within
    /// the given key space (specified by `start_key` and `limit`). If `limit` is `0`,
    /// the key space only has left bound.
    ///
    /// Internally, there will be a `seek` for the first iteration and `next` for
    /// other iterations.
    ///
    /// The return type is `(locks, has_remain)`. `has_remain` indicates whether there
    /// MAY be remaining locks that can be scanned.
    pub fn scan_locks<F>(
        &mut self,
        predicate: F,
        start_key: Option<&Key>,
        limit: usize,
    ) -> Result<(Vec<(Key, Lock)>, bool)>
    where
        F: Fn(&Lock) -> bool,
    {
        self.ensure_lock_cursor()?;
        let lock_cursor = self.lock_cursor.as_mut().unwrap();

        let ok = match start_key {
            Some(ref start_key) => lock_cursor.seek(start_key, &mut self.statistics.lock)?,
            None => lock_cursor.seek_to_first(&mut self.statistics.lock),
        };
        if !ok {
            return Ok((vec![], false));
        }
        let mut locks = Vec::with_capacity(limit);
        let mut has_remain = false;
        loop {
            let key = Key::from_encoded(lock_cursor.key(&mut self.statistics.lock).to_vec());
            let lock = Lock::parse(lock_cursor.value(&mut self.statistics.lock))?;
            if predicate(&lock) {
                locks.push((key, lock));
                if limit > 0 && locks.len() >= limit {
                    // Reach limit. There might be remainings.
                    has_remain = true;
                    break;
                }
            }
            if !lock_cursor.next(&mut self.statistics.lock) {
                // No more keys
                break;
            }
        }
        self.statistics.lock.processed += locks.len();
        Ok((locks, has_remain))
    }

    /// Iterate and get all `Write`s for a key whose commit_ts <= `max_ts`.
    ///
    /// Internally, there will be a `seek` operation the for the first iteration and
    /// `next` operation for other iterations.
    ///
    /// The return value is a `Vec` of type `(commit_ts, write)`.
    pub fn scan_writes(&mut self, user_key: &Key, max_ts: u64) -> Result<Vec<(u64, Write)>> {
        self.ensure_write_cursor()?;
        let write_cursor = self.write_cursor.as_mut().unwrap();

        // TODO: We need to ensure that cursor is not prefix seek.

        let mut writes = vec![];
        write_cursor.seek(
            &user_key.clone().append_ts(max_ts),
            &mut self.statistics.write,
        )?;
        while write_cursor.valid() {
            // TODO: We don't really need to copy slice to a vector here.
            let current_key =
                Key::from_encoded(write_cursor.key(&mut self.statistics.write).to_vec());
            let commit_ts = current_key.decode_ts()?;
            let current_user_key = current_key.truncate_ts()?;
            if *user_key != current_user_key {
                // Meet another key: don't need to scan more.
                break;
            }

            let write = Write::parse(write_cursor.value(&mut self.statistics.write))?;
            writes.push((commit_ts, write));
            self.statistics.write.processed += 1;

            write_cursor.next(&mut self.statistics.write);
        }
        Ok(writes)
    }

    /// Iterate and get values of all versions for a given key in the default CF.
    ///
    /// Notice that small values are embedded in `Write`, which will not be retrieved
    /// by this function.
    ///
    /// Internally, there will be a `seek` operation for the first iteration and
    /// `next` operation for other iterations.
    ///
    /// The return value is a `Vec` of type `(start_ts, value)`.
    pub fn scan_values(&mut self, user_key: &Key) -> Result<Vec<(u64, Value)>> {
        self.ensure_default_cursor()?;
        let default_cursor = self.default_cursor.as_mut().unwrap();

        // TODO: We need to ensure that cursor is not prefix seek.

        let mut values = vec![];
        default_cursor.seek(user_key, &mut self.statistics.data)?;
        while default_cursor.valid() {
            // TODO: We don't really need to copy slice to a vector here.
            let current_key =
                Key::from_encoded(default_cursor.key(&mut self.statistics.data).to_vec());
            let start_ts = current_key.decode_ts()?;
            let current_user_key = current_key.truncate_ts()?;
            if *user_key != current_user_key {
                // Meet another key: don't need to scan more.
                break;
            }

            let value = default_cursor.value(&mut self.statistics.data).to_vec();
            self.statistics.data.processed += 1;

            values.push((start_ts, value));

            default_cursor.next(&mut self.statistics.write);
        }
        Ok(values)
    }

    /// Seek for the first committed user key with the given `start_ts`.
    ///
    /// WARN: This function performs a full scan from the beginning. Use with caution.
    pub fn slowly_seek_key_by_start_ts(&mut self, start_ts: u64) -> Result<Option<Key>> {
        self.ensure_write_cursor()?;
        let write_cursor = self.write_cursor.as_mut().unwrap();

        write_cursor.seek_to_first(&mut self.statistics.write);
        while write_cursor.valid() {
            let write = Write::parse(write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;
            if write.start_ts == start_ts {
                // TODO: We don't really need to copy slice to a vector here.
                let write_key =
                    Key::from_encoded(write_cursor.key(&mut self.statistics.write).to_vec());;
                return Ok(Some(write_key.truncate_ts()?));
            }
            write_cursor.next(&mut self.statistics.write);
        }
        Ok(None)
    }

    /// Seek for a `Write` of a given key in the write CF whose `commit_ts` is the greatest within
    /// the given `max_commit_ts`.
    ///
    /// Internally, there will be a `near_seek` operation.
    pub fn near_seek_write(
        &mut self,
        user_key: &Key,
        max_commit_ts: u64,
        allow_reseek: bool,
    ) -> Result<Option<(u64, Write)>> {
        self.ensure_write_cursor()?;
        let write_cursor = self.write_cursor.as_mut().unwrap();

        write_cursor.near_seek(
            &user_key.clone().append_ts(max_commit_ts),
            allow_reseek,
            &mut self.statistics.write,
        )?;
        let write_key = Key::from_encoded(write_cursor.key(&mut self.statistics.write).to_vec());
        let commit_ts = write_key.decode_ts()?;
        let current_user_key = write_key.truncate_ts()?;
        if &current_user_key != user_key {
            return Ok(None);
        }
        let write = Write::parse(write_cursor.value(&mut self.statistics.write))?;
        self.statistics.write.processed += 1;
        Ok(Some((commit_ts, write)))
    }

    /// Seek for a `Write` of a given key in the write CF by the given `start_ts`.
    ///
    /// Internally, there will be a `near_seek_for_prev` operation for the first
    /// iteration and `prev` for other iterations.
    ///
    /// The return value is a `Vec` of type `(commit_ts, write)`.
    ///
    /// # Algorithm Explanation
    ///
    /// We start from finding the first `write` whose `commit_ts` <= given `start_ts`
    /// (note that we must have `this_write.start_ts` < given `start_ts` because
    /// `this_write.start_ts` < `this_write.commit_ts` <= given `start_ts`).
    ///
    /// Then, we move cursor backward so that we will get `write`s with larger
    /// `commit_ts`. It is done repeatly until we get a `write` that
    /// `this_write.start_ts` == given `start_ts`.
    ///
    /// The loop will break when we get a `write` that `this_write.start_ts` > given
    /// `start_ts` and `write` is not a rollback. In this case, there must be no
    /// future `write`s whose `start_ts` matches our given `start_ts`.
    ///
    /// The rollback is an exception. We may have:
    ///
    /// ```ignore
    /// KEY_10 => PUT_1
    /// KEY_5 => ROLLBACK_5
    /// ```
    ///
    /// In this case if we want to find by `start_ts == 1`, we will meet rollback's
    /// larger `start_ts == 5` first. So we just ignore it.
    pub fn near_reverse_seek_write_by_start_ts(
        &mut self,
        user_key: &Key,
        start_ts: u64,
        allow_reseek: bool,
    ) -> Result<Option<(u64, Write)>> {
        self.ensure_reverse_write_cursor()?;
        let write_cursor = self.reverse_write_cursor.as_mut().unwrap();

        write_cursor.near_seek_for_prev(
            &user_key.clone().append_ts(start_ts),
            allow_reseek,
            &mut self.statistics.write,
        )?;
        while write_cursor.valid() {
            // TODO: We don't really need to copy slice to a vector here.
            let current_key =
                Key::from_encoded(write_cursor.key(&mut self.statistics.write).to_vec());
            let commit_ts = current_key.decode_ts()?;
            let current_user_key = current_key.truncate_ts()?;
            if *user_key != current_user_key {
                // Meet another key: don't need to scan more.
                break;
            }
            let write = Write::parse(write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            if write.start_ts == start_ts {
                return Ok(Some((commit_ts, write)));
            }

            // If we reach a commit version whose type is not Rollback and start ts is
            // larger than the given start ts, stop searching.
            if write.write_type != WriteType::Rollback && write.start_ts > start_ts {
                break;
            }

            write_cursor.prev(&mut self.statistics.write);
        }
        Ok(None)
    }

    /// Seek for a `Write` of a given key in the write CF by the given `start_ts`.
    /// Return its `WriteType` if `Write` is found. Otherwise `None`.
    ///
    /// Internally, there will be a `near_seek_for_prev` operation for the first
    /// iteration and `prev` for other iterations.
    ///
    /// The return value is a `Vec` of type `(commit_ts, write_type)`.
    ///
    /// This is a convenience method, wrapped over `near_reverse_seek_write_by_start_ts`.
    #[inline]
    pub fn near_reverse_seek_write_type_by_start_ts(
        &mut self,
        user_key: &Key,
        start_ts: u64,
        allow_reseek: bool,
    ) -> Result<Option<(u64, WriteType)>> {
        let some_pairs =
            self.near_reverse_seek_write_by_start_ts(user_key, start_ts, allow_reseek)?;
        match some_pairs {
            None => Ok(None),
            Some((commit_ts, write)) => Ok(Some((commit_ts, write.write_type))),
        }
    }

    /// Create the lock cursor if it doesn't exist.
    fn ensure_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_some() {
            return Ok(());
        }
        let cursor = super::util::CursorBuilder::new(&self.snapshot, CF_LOCK)
            .fill_cache(self.fill_cache)
            .build()?;
        self.lock_cursor = Some(cursor);
        Ok(())
    }

    /// Create the write cursor if it doesn't exist.
    fn ensure_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_some() {
            return Ok(());
        }
        let cursor = super::util::CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .build()?;
        self.write_cursor = Some(cursor);
        Ok(())
    }

    /// Create the reverse write cursor if it doesn't exist.
    fn ensure_reverse_write_cursor(&mut self) -> Result<()> {
        if self.reverse_write_cursor.is_some() {
            return Ok(());
        }
        let cursor = super::util::CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .scan_mode(ScanMode::Backward)
            .build()?;
        self.reverse_write_cursor = Some(cursor);
        Ok(())
    }

    /// Create the default cursor if it doesn't exist.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = super::util::CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .fill_cache(self.fill_cache)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}
