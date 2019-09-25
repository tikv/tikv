// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::CF_DEFAULT;
use keys::Key;
use kvproto::kvrpcpb::IsolationLevel;
use tikv_util::buffer_vec::BufferVec;

use super::ScannerConfig;
use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::reader::util::CheckLockResult;
use crate::storage::mvcc::write2::Write2;
use crate::storage::mvcc::{Lock, WriteType};
use crate::storage::{Cursor, RangeScanner, Snapshot, Statistics};

//const BUF_KV_PAIRS: usize = 2048;
const BUF_KEY_SIZE: usize = 64;
//const BUF_VALUE_SIZE: usize = 100;

enum MoveToTsVersionStatus {
    MeetAnotherKey,
    KeySpaceEnded,
    MoveSuccess,
}

/// A scanner that only works correctly when scanning all data in the range.
///
/// It works very differently to normal scanners, in that it provides separate interface to scan
/// all locks in the range beforehand. After that, it will only scan KV pairs without looking at
/// the lock. This means it may negatively produce lock error if you only want first several KV
/// pairs in the range since any lock in the range will be returned no matter or not how many
/// real KV pairs there are. For this reason, this scanner is only useful when you want all KV
/// pairs in the range.
pub struct RangeForwardScanner<S: Snapshot> {
    cfg: ScannerConfig<S>,

    lock_cursor: Cursor<S::Iter>,
    write_cursor: Cursor<S::Iter>,
    /// `default cursor` is lazy created only when it's needed.
    default_cursor: Option<Cursor<S::Iter>>,

    /// A buffer used during scanning.
    key_buffer: Key,

    /// Buffers to store KV pairs.
    //    keys_buffer: BufferVec,
    //    values_buffer: BufferVec,
    statistics: Statistics,
}

impl<S: Snapshot> RangeForwardScanner<S> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
        mut write_cursor: Cursor<S::Iter>,
    ) -> failure::Fallible<RangeForwardScanner<S>> {
        let mut stats = Statistics::default();
        if likely!(cfg.lower_bound.is_some()) {
            write_cursor.internal_seek(cfg.lower_bound.as_ref().unwrap(), &mut stats.write)?;
        } else {
            write_cursor.seek_to_first(&mut stats.write);
        }

        Ok(RangeForwardScanner {
            cfg,
            lock_cursor,
            write_cursor,
            statistics: stats,
            default_cursor: None,
            key_buffer: Key(Vec::with_capacity(BUF_KEY_SIZE)),
            //            keys_buffer: BufferVec::with_capacity(BUF_KV_PAIRS, BUF_KV_PAIRS * BUF_KEY_SIZE),
            //            values_buffer: BufferVec::with_capacity(BUF_KV_PAIRS, BUF_KV_PAIRS * BUF_VALUE_SIZE),
        })
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Scans the first lock meet in the range.
    pub fn scan_first_lock(&mut self) -> failure::Fallible<()> {
        // TODO: It is more efficient to return all locks to the client for lock resolving.

        if unlikely!(self.cfg.isolation_level == IsolationLevel::Rc) {
            // Don't check lock in RC.
            return Ok(());
        }

        let is_valid = if likely!(self.cfg.lower_bound.is_some()) {
            self.lock_cursor.internal_seek(
                self.cfg.lower_bound.as_ref().unwrap(),
                &mut self.statistics.lock,
            )?
        } else {
            self.lock_cursor.seek_to_first(&mut self.statistics.lock)
        };

        if likely!(!is_valid) {
            // Nothing in the lock CF.
            return Ok(());
        }

        loop {
            let lock = {
                let lock_value = self.lock_cursor.value(&mut self.statistics.lock);
                Lock::parse(lock_value)?
            };
            // TODO: Avoid allocation
            match super::super::util::check_lock(
                &Key::from_encoded_slice(self.lock_cursor.key(&mut self.statistics.lock)),
                self.cfg.ts,
                &lock,
            )? {
                CheckLockResult::NotLocked => {}
                CheckLockResult::Locked(e) => return Err(e.into()),
                // We don't support scanning latest version (specified by using MAX_U64 as the
                // timestamp) in the scanner.
                // TODO: Better to check ts beforehand.
                CheckLockResult::Ignored(_) => return Err(format_err!("Invalid timestamp")),
            }

            if !self.lock_cursor.next(&mut self.statistics.lock) {
                return Ok(());
            }
        }
    }

    /// Scans next several KV pairs and fills them in the given buffer. Existing buffer content
    /// will be kept.
    ///
    /// Returns how many KV pairs are actually scanned. If number of scanned KV pairs is less than
    /// specified, it means there is no more data.
    ///
    /// If errors are returned:
    /// 1. The scanner enters an invalid state and further calls may result in undefined behaviour.
    /// 2. The output key buffer and value buffer may contain different number of elements, i.e.
    ///    partially scanned data.
    pub fn next(
        &mut self,
        n: usize,
        out_keys: &mut BufferVec,
        out_values: &mut BufferVec,
    ) -> failure::Fallible<usize> {
        if unlikely!(!self.write_cursor.valid()?) {
            return Ok(0);
        }

        let mut scanned_pairs = 0;

        'outer: while scanned_pairs < n {
            // println!("Scan next KV pair");

            self.key_buffer.0.clear();
            self.key_buffer
                .0
                .extend_from_slice(self.write_cursor.key(&mut self.statistics.write));
            let user_key_len = self.key_buffer.0.len() - 8;
            let timestamp =
                codec::number::NumberCodec::decode_u64_desc(&self.key_buffer.0[user_key_len..]);

            // Ensure cursor is pointing to the greatest version that <= ts.
            if unlikely!(timestamp > self.cfg.ts) {
                // println!("Move to ts version");
                match self.step_move_to_ts_version(user_key_len)? {
                    MoveToTsVersionStatus::MoveSuccess => {}
                    MoveToTsVersionStatus::MeetAnotherKey => continue 'outer,
                    MoveToTsVersionStatus::KeySpaceEnded => return Ok(scanned_pairs),
                }
            }

            // Now we must have reached the first key >= `${user_key}_${ts}`. However, we may
            // meet `Lock` or `Rollback`. In this case, more versions needs to be looked up.
            loop {
                // println!("Parse version");
                let write =
                    Write2::from_slice(self.write_cursor.value(&mut self.statistics.write))?;
                match write.write_type {
                    WriteType::Put => {
                        // Decode the key into `keys_buffer` directly.
                        // TODO: Maybe a better way is to implement Iterator trait for the decoder.
                        unsafe {
                            let data_buf = out_keys.mut_data();
                            let offset = data_buf.len();

                            data_buf.reserve(user_key_len);
                            let (_, written_bytes) =
                                codec::byte::MemComparableByteCodec::try_decode_first(
                                    &self.key_buffer.0[..user_key_len],
                                    std::slice::from_raw_parts_mut(
                                        data_buf.as_mut_ptr().add(offset),
                                        user_key_len,
                                    ),
                                )?;
                            // When there are errors, `data_buf` may contain partially decoded data.
                            // However it is still in a valid state since we have not modified its
                            // visible length.

                            // Modify the data buffer and offset buffer only when decoding
                            // succeeded.
                            data_buf.set_len(data_buf.len() + written_bytes);
                            out_keys.mut_offsets().push(offset);
                        }

                        // TODO: Fill values later is faster!
                        if !self.cfg.omit_value {
                            match write.short_value {
                                Some(v) => {
                                    // println!("Fill short value");
                                    out_values.push(v);
                                }
                                None => {
                                    // println!("Lookup default CF");
                                    let start_ts = write.start_ts;
                                    self.step_lookup_user_value_in_default_cf(
                                        user_key_len,
                                        start_ts,
                                        out_values,
                                    )?;
                                }
                            }
                        } else {
                            out_values.push([]);
                        }

                        scanned_pairs += 1;

                        break;
                    }
                    WriteType::Delete => break,
                    WriteType::Lock | WriteType::Rollback => {
                        // Continue iterate next `write`.
                    }
                }

                // TODO: Extract below logic to a function since it may not be a hot path.
                // println!("Move to next version");
                if !self.write_cursor.next(&mut self.statistics.write) {
                    // Key space ended.
                    return Ok(scanned_pairs);
                }
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                let current_user_key = &current_key[..current_key.len() - 8];
                if !is_key_eq(current_user_key, &self.key_buffer.0[..user_key_len]) {
                    // Meet another key.
                    continue 'outer;
                }
            }

            // Move the cursor to point to next user key.
            for _ in 0..SEEK_BOUND {
                // println!("Move to next key");
                if !self.write_cursor.next(&mut self.statistics.write) {
                    // Key space ended.
                    return Ok(scanned_pairs);
                }
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                let current_user_key = &current_key[..current_key.len() - 8];
                if !is_key_eq(current_user_key, &self.key_buffer.0[..user_key_len]) {
                    // Meet another key.
                    continue 'outer;
                }
            }
            // println!("Jump to next key");
            codec::number::NumberCodec::encode_u64_desc(&mut self.key_buffer.0[user_key_len..], 0);
            if !self
                .write_cursor
                .internal_seek(&self.key_buffer, &mut self.statistics.write)?
            {
                // Key space ended.
                return Ok(scanned_pairs);
            }
        }

        Ok(scanned_pairs)
    }

    fn step_move_to_ts_version(
        &mut self,
        user_key_len: usize,
    ) -> failure::Fallible<MoveToTsVersionStatus> {
        // Call next() several times to find a suitable version.
        for _ in 0..SEEK_BOUND {
            if !self.write_cursor.next(&mut self.statistics.write) {
                return Ok(MoveToTsVersionStatus::KeySpaceEnded);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            let (current_user_key, current_timestamp_slice) =
                current_key.split_at(current_key.len() - 8);
            if !is_key_eq(current_user_key, &self.key_buffer.0[..user_key_len]) {
                return Ok(MoveToTsVersionStatus::MeetAnotherKey);
            }
            let current_timestamp =
                codec::number::NumberCodec::decode_u64_desc(current_timestamp_slice);
            if current_timestamp <= self.cfg.ts {
                return Ok(MoveToTsVersionStatus::MoveSuccess);
            }
        }
        // Modify the timestamp part of `key_buffer` and use it for seeking.
        codec::number::NumberCodec::encode_u64_desc(
            &mut self.key_buffer.0[user_key_len..],
            self.cfg.ts,
        );
        if !self
            .write_cursor
            .internal_seek(&self.key_buffer, &mut self.statistics.write)?
        {
            return Ok(MoveToTsVersionStatus::KeySpaceEnded);
        }
        let current_key = self.write_cursor.key(&mut self.statistics.write);
        let current_user_key = &current_key[..current_key.len() - 8];
        if !is_key_eq(current_user_key, &self.key_buffer.0[..user_key_len]) {
            return Ok(MoveToTsVersionStatus::MeetAnotherKey);
        }
        Ok(MoveToTsVersionStatus::MoveSuccess)
    }

    fn step_lookup_user_value_in_default_cf(
        &mut self,
        user_key_len: usize,
        start_ts: u64,
        out_values: &mut BufferVec,
    ) -> failure::Fallible<()> {
        if self.default_cursor.is_none() {
            self.default_cursor = Some(self.cfg.create_cf_cursor(CF_DEFAULT)?);
        }
        // We need to find the value in default CF.
        codec::number::NumberCodec::encode_u64_desc(
            &mut self.key_buffer.0[user_key_len..],
            start_ts,
        );
        let default_cursor = self.default_cursor.as_mut().unwrap();
        if !default_cursor.near_seek(&self.key_buffer, &mut self.statistics.data)? {
            // TODO
            panic!();
        }
        if !is_key_eq(
            default_cursor.key(&mut self.statistics.data),
            self.key_buffer.0.as_slice(),
        ) {
            // TODO
            panic!();
        }
        out_values.push(default_cursor.value(&mut self.statistics.data));
        Ok(())
    }
}

impl<S: Snapshot> RangeScanner for RangeForwardScanner<S> {
    #[inline]
    fn scan_first_lock(&mut self) -> failure::Fallible<()> {
        RangeForwardScanner::scan_first_lock(self)
    }

    #[inline]
    fn next(
        &mut self,
        n: usize,
        out_keys: &mut BufferVec,
        out_values: &mut BufferVec,
    ) -> failure::Fallible<usize> {
        RangeForwardScanner::next(self, n, out_keys, out_values)
    }

    #[inline]
    fn take_statistics(&mut self) -> Statistics {
        RangeForwardScanner::take_statistics(self)
    }
}

fn is_key_eq(key_a: &[u8], key_b: &[u8]) -> bool {
    let key_len = key_a.len();
    if key_len != key_b.len() {
        return false;
    }
    if key_len >= 24 {
        // fast path
        unsafe {
            let left = std::ptr::read_unaligned(key_a.as_ptr().add(key_len - 8) as *const u64);
            let right = std::ptr::read_unaligned(key_b.as_ptr().add(key_len - 8) as *const u64);
            if left != right {
                return false;
            }
            let left = std::ptr::read_unaligned(key_a.as_ptr().add(key_len - 16) as *const u64);
            let right = std::ptr::read_unaligned(key_b.as_ptr().add(key_len - 16) as *const u64);
            if left != right {
                return false;
            }
            let left = std::ptr::read_unaligned(key_a.as_ptr().add(key_len - 24) as *const u64);
            let right = std::ptr::read_unaligned(key_b.as_ptr().add(key_len - 24) as *const u64);
            if left != right {
                return false;
            }
            key_a[..key_len - 24] == key_b[..key_len - 24]
        }
    } else {
        key_a == key_b
    }
}

#[cfg(test)]
mod tests {
    use super::super::ScannerBuilder;
    use super::*;
    use crate::storage::mvcc::tests::*;
    use crate::storage::Scanner;
    use crate::storage::{Engine, Key, TestEngineBuilder};

    use kvproto::kvrpcpb::Context;

    #[test]
    fn test_working() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut keys_buffer = BufferVec::new();
        let mut values_buffer = BufferVec::new();

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10, false)
            .range(None, None)
            .build_forward_range_scanner()
            .unwrap();

        assert!(scanner.scan_first_lock().is_ok());
        assert_eq!(
            scanner
                .next(10, &mut keys_buffer, &mut values_buffer)
                .unwrap(),
            0
        );

        must_prewrite_put(&engine, b"a", b"value_a", b"a", 7);
        must_commit(&engine, b"a", 7, 7);
        for ts in 0..5 {
            must_rollback(&engine, b"b", ts);
        }
        must_prewrite_put(&engine, b"c", b"value_c", b"c", 3);
        must_commit(&engine, b"c", 3, 3);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10, false)
            .range(None, None)
            .build_forward_range_scanner()
            .unwrap();

        assert!(scanner.scan_first_lock().is_ok());
        assert_eq!(
            scanner
                .next(10, &mut keys_buffer, &mut values_buffer)
                .unwrap(),
            2
        );
        assert_eq!(keys_buffer.len(), 2);
        assert_eq!(values_buffer.len(), 2);
        assert_eq!(&keys_buffer[0], b"a");
        assert_eq!(&values_buffer[0], b"value_a");
        assert_eq!(&keys_buffer[1], b"c");
        assert_eq!(&values_buffer[1], b"value_c");

        keys_buffer.clear();
        values_buffer.clear();
        let mut scanner = ScannerBuilder::new(snapshot, 5, false)
            .range(None, None)
            .build_forward_range_scanner()
            .unwrap();

        assert!(scanner.scan_first_lock().is_ok());
        assert_eq!(
            scanner
                .next(10, &mut keys_buffer, &mut values_buffer)
                .unwrap(),
            1
        );
        assert_eq!(keys_buffer.len(), 1);
        assert_eq!(values_buffer.len(), 1);
        assert_eq!(&keys_buffer[0], b"c");
        assert_eq!(&values_buffer[0], b"value_c");
    }
}
