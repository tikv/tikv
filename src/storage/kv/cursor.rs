// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::cmp::Ordering;
use std::ops::Bound;

use engine_traits::CfName;
use engine_traits::{IterOptions, DATA_KEY_PREFIX_LEN};
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};
use txn_types::{Key, TimeStamp};

use crate::storage::kv::{CfStatistics, Error, Iterator, Result, ScanMode, Snapshot, SEEK_BOUND};

pub struct Cursor<I: Iterator> {
    iter: I,
    scan_mode: ScanMode,
    // the data cursor can be seen will be
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,

    // Use `Cell` to wrap these flags to provide interior mutability, so that `key()` and
    // `value()` don't need to have `&mut self`.
    cur_key_has_read: Cell<bool>,
    cur_value_has_read: Cell<bool>,
}

macro_rules! near_loop {
    ($cond:expr, $fallback:expr, $st:expr) => {{
        let mut cnt = 0;
        while $cond {
            cnt += 1;
            if cnt >= SEEK_BOUND {
                $st.over_seek_bound += 1;
                return $fallback;
            }
        }
    }};
}

impl<I: Iterator> Cursor<I> {
    pub fn new(iter: I, mode: ScanMode) -> Self {
        Self {
            iter,
            scan_mode: mode,
            min_key: None,
            max_key: None,

            cur_key_has_read: Cell::new(false),
            cur_value_has_read: Cell::new(false),
        }
    }

    /// Mark key and value as unread. It will be invoked once cursor is moved.
    #[inline]
    fn mark_unread(&self) {
        self.cur_key_has_read.set(false);
        self.cur_value_has_read.set(false);
    }

    /// Mark key as read. Returns whether key was marked as read before this call.
    #[inline]
    fn mark_key_read(&self) -> bool {
        self.cur_key_has_read.replace(true)
    }

    /// Mark value as read. Returns whether value was marked as read before this call.
    #[inline]
    fn mark_value_read(&self) -> bool {
        self.cur_value_has_read.replace(true)
    }

    pub fn seek(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        fail_point!("kv_cursor_seek", |_| {
            Err(box_err!("kv cursor seek error"))
        });

        assert_ne!(self.scan_mode, ScanMode::Backward);
        if self
            .max_key
            .as_ref()
            .map_or(false, |k| k <= key.as_encoded())
        {
            self.iter.validate_key(key)?;
            return Ok(false);
        }

        if self.scan_mode == ScanMode::Forward
            && self.valid()?
            && self.key(statistics) >= key.as_encoded().as_slice()
        {
            return Ok(true);
        }

        if !self.internal_seek(key, statistics)? {
            self.max_key = Some(key.as_encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `seek` instead.
    pub fn near_seek(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Backward);
        if !self.valid()? {
            return self.seek(key, statistics);
        }
        let ord = self.key(statistics).cmp(key.as_encoded());
        if ord == Ordering::Equal
            || (self.scan_mode == ScanMode::Forward && ord == Ordering::Greater)
        {
            return Ok(true);
        }
        if self
            .max_key
            .as_ref()
            .map_or(false, |k| k <= key.as_encoded())
        {
            self.iter.validate_key(key)?;
            return Ok(false);
        }
        if ord == Ordering::Greater {
            near_loop!(
                self.prev(statistics) && self.key(statistics) > key.as_encoded().as_slice(),
                self.seek(key, statistics),
                statistics
            );
            if self.valid()? {
                if self.key(statistics) < key.as_encoded().as_slice() {
                    self.next(statistics);
                }
            } else {
                assert!(self.seek_to_first(statistics));
                return Ok(true);
            }
        } else {
            // ord == Less
            near_loop!(
                self.next(statistics) && self.key(statistics) < key.as_encoded().as_slice(),
                self.seek(key, statistics),
                statistics
            );
        }
        if !self.valid()? {
            self.max_key = Some(key.as_encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Get the value of specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should `seek` first.
    pub fn get(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<Option<&[u8]>> {
        if self.scan_mode != ScanMode::Backward {
            if self.near_seek(key, statistics)? && self.key(statistics) == &**key.as_encoded() {
                return Ok(Some(self.value(statistics)));
            }
            return Ok(None);
        }
        if self.near_seek_for_prev(key, statistics)? && self.key(statistics) == &**key.as_encoded()
        {
            return Ok(Some(self.value(statistics)));
        }
        Ok(None)
    }

    pub fn seek_for_prev(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Forward);
        if self
            .min_key
            .as_ref()
            .map_or(false, |k| k >= key.as_encoded())
        {
            self.iter.validate_key(key)?;
            return Ok(false);
        }

        if self.scan_mode == ScanMode::Backward
            && self.valid()?
            && self.key(statistics) <= key.as_encoded().as_slice()
        {
            return Ok(true);
        }

        if !self.internal_seek_for_prev(key, statistics)? {
            self.min_key = Some(key.as_encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Find the largest key that is not greater than the specific key.
    pub fn near_seek_for_prev(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Forward);
        if !self.valid()? {
            return self.seek_for_prev(key, statistics);
        }
        let ord = self.key(statistics).cmp(key.as_encoded());
        if ord == Ordering::Equal || (self.scan_mode == ScanMode::Backward && ord == Ordering::Less)
        {
            return Ok(true);
        }

        if self
            .min_key
            .as_ref()
            .map_or(false, |k| k >= key.as_encoded())
        {
            self.iter.validate_key(key)?;
            return Ok(false);
        }

        if ord == Ordering::Less {
            near_loop!(
                self.next(statistics) && self.key(statistics) < key.as_encoded().as_slice(),
                self.seek_for_prev(key, statistics),
                statistics
            );
            if self.valid()? {
                if self.key(statistics) > key.as_encoded().as_slice() {
                    self.prev(statistics);
                }
            } else {
                assert!(self.seek_to_last(statistics));
                return Ok(true);
            }
        } else {
            near_loop!(
                self.prev(statistics) && self.key(statistics) > key.as_encoded().as_slice(),
                self.seek_for_prev(key, statistics),
                statistics
            );
        }

        if !self.valid()? {
            self.min_key = Some(key.as_encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    pub fn reverse_seek(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        if !self.seek_for_prev(key, statistics)? {
            return Ok(false);
        }

        if self.key(statistics) == &**key.as_encoded() {
            // should not update min_key here. otherwise reverse_seek_le may not
            // work as expected.
            return Ok(self.prev(statistics));
        }

        Ok(true)
    }

    /// Reverse seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `reverse_seek` instead.
    pub fn near_reverse_seek(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        if !self.near_seek_for_prev(key, statistics)? {
            return Ok(false);
        }

        if self.key(statistics) == &**key.as_encoded() {
            return Ok(self.prev(statistics));
        }

        Ok(true)
    }

    #[inline]
    pub fn key(&self, statistics: &mut CfStatistics) -> &[u8] {
        let key = self.iter.key();
        if !self.mark_key_read() {
            statistics.flow_stats.read_bytes += key.len();
            statistics.flow_stats.read_keys += 1;
        }
        key
    }

    #[inline]
    pub fn value(&self, statistics: &mut CfStatistics) -> &[u8] {
        let value = self.iter.value();
        if !self.mark_value_read() {
            statistics.flow_stats.read_bytes += value.len();
        }
        value
    }

    #[inline]
    pub fn seek_to_first(&mut self, statistics: &mut CfStatistics) -> bool {
        statistics.seek += 1;
        self.mark_unread();
        self.iter.seek_to_first().expect("Invalid Iterator")
    }

    #[inline]
    pub fn seek_to_last(&mut self, statistics: &mut CfStatistics) -> bool {
        statistics.seek += 1;
        self.mark_unread();
        self.iter.seek_to_last().expect("Invalid Iterator")
    }

    #[inline]
    pub fn internal_seek(&mut self, key: &Key, statistics: &mut CfStatistics) -> Result<bool> {
        statistics.seek += 1;
        self.mark_unread();
        self.iter.seek(key)
    }

    #[inline]
    pub fn internal_seek_for_prev(
        &mut self,
        key: &Key,
        statistics: &mut CfStatistics,
    ) -> Result<bool> {
        statistics.seek_for_prev += 1;
        self.mark_unread();
        self.iter.seek_for_prev(key)
    }

    #[inline]
    pub fn next(&mut self, statistics: &mut CfStatistics) -> bool {
        statistics.next += 1;
        self.mark_unread();
        self.iter.next().expect("Invalid Iterator")
    }

    #[inline]
    pub fn prev(&mut self, statistics: &mut CfStatistics) -> bool {
        statistics.prev += 1;
        self.mark_unread();
        self.iter.prev().expect("Invalid Iterator")
    }

    #[inline]
    // As Rocksdb described, if Iterator::Valid() is false, there are two possibilities:
    // (1) We reached the end of the data. In this case, status() is OK();
    // (2) there is an error. In this case status() is not OK().
    // So check status when iterator is invalidated.
    pub fn valid(&self) -> Result<bool> {
        match self.iter.valid() {
            Err(e) => {
                self.handle_error_status(e)?;
                unreachable!();
            }
            Ok(t) => Ok(t),
        }
    }

    #[inline(never)]
    fn handle_error_status(&self, e: Error) -> Result<()> {
        // Split out the error case to reduce hot-path code size.
        CRITICAL_ERROR.with_label_values(&["rocksdb iter"]).inc();
        if panic_when_unexpected_key_or_data() {
            set_panic_mark();
            panic!(
                "failed to iterate: {:?}, min_key: {:?}, max_key: {:?}",
                e,
                self.min_key.as_ref().map(|v| hex::encode_upper(v)),
                self.max_key.as_ref().map(|v| hex::encode_upper(v)),
            );
        } else {
            error!(
                "failed to iterate";
                "min_key" => ?self.min_key.as_ref().map(|v| hex::encode_upper(v)),
                "max_key" => ?self.max_key.as_ref().map(|v| hex::encode_upper(v)),
                "error" => ?e,
            );
            Err(e)
        }
    }
}

/// A handy utility to build a snapshot cursor according to various configurations.
pub struct CursorBuilder<'a, S: Snapshot> {
    snapshot: &'a S,
    cf: CfName,

    scan_mode: ScanMode,
    fill_cache: bool,
    prefix_seek: bool,
    upper_bound: Option<Key>,
    lower_bound: Option<Key>,
    // hint for we will only scan data with commit ts >= hint_min_ts
    hint_min_ts: Option<TimeStamp>,
    // hint for we will only scan data with commit ts <= hint_max_ts
    hint_max_ts: Option<TimeStamp>,
}

impl<'a, S: 'a + Snapshot> CursorBuilder<'a, S> {
    /// Initialize a new `CursorBuilder`.
    pub fn new(snapshot: &'a S, cf: CfName) -> Self {
        CursorBuilder {
            snapshot,
            cf,

            scan_mode: ScanMode::Forward,
            fill_cache: true,
            prefix_seek: false,
            upper_bound: None,
            lower_bound: None,
            hint_min_ts: None,
            hint_max_ts: None,
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

    /// Set whether or not to use prefix seek.
    ///
    /// Defaults to `false`, it means use total order seek.
    #[inline]
    pub fn prefix_seek(mut self, prefix_seek: bool) -> Self {
        self.prefix_seek = prefix_seek;
        self
    }

    /// Set iterator scanning mode.
    ///
    /// Defaults to `ScanMode::Forward`.
    #[inline]
    pub fn scan_mode(mut self, scan_mode: ScanMode) -> Self {
        self.scan_mode = scan_mode;
        self
    }

    /// Set iterator range by giving lower and upper bound.
    /// The range is left closed right open.
    ///
    /// Both default to `None`.
    #[inline]
    pub fn range(mut self, lower: Option<Key>, upper: Option<Key>) -> Self {
        self.lower_bound = lower;
        self.upper_bound = upper;
        self
    }

    /// Set the hint for the minimum commit ts we want to scan.
    ///
    /// Default is empty.
    #[inline]
    pub fn hint_min_ts(mut self, min_ts: Option<TimeStamp>) -> Self {
        self.hint_min_ts = min_ts;
        self
    }

    /// Set the hint for the maximum commit ts we want to scan.
    ///
    /// Default is empty.
    #[inline]
    pub fn hint_max_ts(mut self, max_ts: Option<TimeStamp>) -> Self {
        self.hint_max_ts = max_ts;
        self
    }

    /// Build `Cursor` from the current configuration.
    pub fn build(self) -> Result<Cursor<S::Iter>> {
        let l_bound = if let Some(b) = self.lower_bound {
            let builder = KeyBuilder::from_vec(b.into_encoded(), DATA_KEY_PREFIX_LEN, 0);
            Some(builder)
        } else {
            None
        };
        let u_bound = if let Some(b) = self.upper_bound {
            let builder = KeyBuilder::from_vec(b.into_encoded(), DATA_KEY_PREFIX_LEN, 0);
            Some(builder)
        } else {
            None
        };
        let mut iter_opt = IterOptions::new(l_bound, u_bound, self.fill_cache);
        if let Some(ts) = self.hint_min_ts {
            iter_opt.set_hint_min_ts(Bound::Included(ts.into_inner()));
        }
        if let Some(ts) = self.hint_max_ts {
            iter_opt.set_hint_max_ts(Bound::Included(ts.into_inner()));
        }
        if self.prefix_seek {
            iter_opt = iter_opt.use_prefix_seek().set_prefix_same_as_start(true);
        }
        self.snapshot.iter_cf(self.cf, iter_opt, self.scan_mode)
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::RocksEngine;
    use engine_traits::{IterOptions, KvEngines, SyncMutable};
    use keys::data_key;
    use kvproto::metapb::{Peer, Region};
    use tempfile::Builder;
    use txn_types::Key;

    use crate::storage::{CfStatistics, Cursor, ScanMode};
    use raftstore::store::{new_temp_engine, RegionSnapshot};

    type DataSet = Vec<(Vec<u8>, Vec<u8>)>;

    fn load_default_dataset(engines: KvEngines<RocksEngine, RocksEngine>) -> (Region, DataSet) {
        let mut r = Region::default();
        r.mut_peers().push(Peer::default());
        r.set_id(10);
        r.set_start_key(b"a2".to_vec());
        r.set_end_key(b"a7".to_vec());

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
            (b"a9".to_vec(), b"v9".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engines.kv.put(&data_key(k), v).unwrap();
        }
        (r, base_data)
    }

    #[test]
    fn test_reverse_iterate() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (region, test_data) = load_default_dataset(engines.clone());

        let snap = RegionSnapshot::<RocksEngine>::from_raw(engines.kv.clone(), region);
        let mut statistics = CfStatistics::default();
        let it = snap.iter(IterOptions::default());
        let mut iter = Cursor::new(it, ScanMode::Mixed);
        assert!(!iter
            .reverse_seek(&Key::from_encoded_slice(b"a2"), &mut statistics)
            .unwrap());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a7"), &mut statistics)
            .unwrap());
        let mut pair = (
            iter.key(&mut statistics).to_vec(),
            iter.value(&mut statistics).to_vec(),
        );
        assert_eq!(pair, (b"a5".to_vec(), b"v5".to_vec()));
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a5"), &mut statistics)
            .unwrap());
        pair = (
            iter.key(&mut statistics).to_vec(),
            iter.value(&mut statistics).to_vec(),
        );
        assert_eq!(pair, (b"a3".to_vec(), b"v3".to_vec()));
        assert!(!iter
            .reverse_seek(&Key::from_encoded_slice(b"a3"), &mut statistics)
            .unwrap());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a1"), &mut statistics)
            .is_err());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a8"), &mut statistics)
            .is_err());

        assert!(iter.seek_to_last(&mut statistics));
        let mut res = vec![];
        loop {
            res.push((
                iter.key(&mut statistics).to_vec(),
                iter.value(&mut statistics).to_vec(),
            ));
            if !iter.prev(&mut statistics) {
                break;
            }
        }
        let mut expect = test_data[1..3].to_vec();
        expect.reverse();
        assert_eq!(res, expect);

        // test last region
        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let snap = RegionSnapshot::<RocksEngine>::from_raw(engines.kv, region);
        let it = snap.iter(IterOptions::default());
        let mut iter = Cursor::new(it, ScanMode::Mixed);
        assert!(!iter
            .reverse_seek(&Key::from_encoded_slice(b"a1"), &mut statistics)
            .unwrap());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a2"), &mut statistics)
            .unwrap());
        let pair = (
            iter.key(&mut statistics).to_vec(),
            iter.value(&mut statistics).to_vec(),
        );
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        for kv_pairs in test_data.windows(2) {
            let seek_key = Key::from_encoded(kv_pairs[1].0.clone());
            assert!(
                iter.reverse_seek(&seek_key, &mut statistics).unwrap(),
                "{}",
                seek_key
            );
            let pair = (
                iter.key(&mut statistics).to_vec(),
                iter.value(&mut statistics).to_vec(),
            );
            assert_eq!(pair, kv_pairs[0]);
        }

        assert!(iter.seek_to_last(&mut statistics));
        let mut res = vec![];
        loop {
            res.push((
                iter.key(&mut statistics).to_vec(),
                iter.value(&mut statistics).to_vec(),
            ));
            if !iter.prev(&mut statistics) {
                break;
            }
        }
        let mut expect = test_data;
        expect.reverse();
        assert_eq!(res, expect);
    }
}
