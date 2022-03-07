// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use super::encoded::RawEncodeSnapshot;

use crate::storage::kv::Result;
use crate::storage::kv::{Cursor, ScanMode, Snapshot};
use crate::storage::Statistics;

use api_version::{APIV1TTL, APIV2};
use engine_traits::{CfName, IterOptions, DATA_KEY_PREFIX_LEN};
use kvproto::kvrpcpb::{ApiVersion, KeyRange};
use std::time::Duration;
use tikv_kv::CursorBuilder;
use tikv_util::time::Instant;
use txn_types::{Key, KvPair, TimeStamp};
use yatp::task::future::reschedule;

const MAX_TIME_SLICE: Duration = Duration::from_millis(2);
const MAX_BATCH_SIZE: usize = 1024;

pub enum RawStore<S: Snapshot> {
    V1(RawStoreInner<S>),
    V1TTL(RawStoreInner<RawEncodeSnapshot<S, APIV1TTL>>),
    V2(RawStoreV2Inner<RawEncodeSnapshot<S, APIV2>>),
}

impl<'a, S: Snapshot> RawStore<S> {
    pub fn new(snapshot: S, api_version: ApiVersion) -> Self {
        match api_version {
            ApiVersion::V1 => RawStore::V1(RawStoreInner::new(snapshot)),
            ApiVersion::V1ttl => RawStore::V1TTL(RawStoreInner::new(
                RawEncodeSnapshot::from_snapshot(snapshot),
            )),
            ApiVersion::V2 => RawStore::V2(RawStoreV2Inner::new(RawEncodeSnapshot::from_snapshot(
                snapshot,
            ))),
        }
    }

    pub fn raw_get_key_value(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<Vec<u8>>> {
        match self {
            RawStore::V1(inner) => inner.raw_get_key_value(cf, key, stats),
            RawStore::V1TTL(inner) => inner.raw_get_key_value(cf, key, stats),
            RawStore::V2(inner) => inner.raw_get_key_value(cf, key, stats),
        }
    }

    pub fn raw_get_key_ttl(
        &self,
        cf: CfName,
        key: &'a Key,
        stats: &'a mut Statistics,
    ) -> Result<Option<u64>> {
        match self {
            RawStore::V1(_) => panic!("get ttl on non-ttl store"),
            RawStore::V1TTL(inner) => inner.snapshot.get_key_ttl_cf(cf, key, stats),
            RawStore::V2(inner) => inner.snapshot.get_key_ttl_cf(cf, key, stats),
        }
    }

    pub async fn forward_raw_scan(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        end_key: Option<&'a Key>,
        limit: usize,
        statistics: &'a mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut option = IterOptions::default();
        if let Some(end) = end_key {
            option.set_upper_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        match self {
            RawStore::V1(inner) => {
                if key_only {
                    option.set_key_only(key_only);
                }
                inner
                    .forward_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            RawStore::V1TTL(inner) => {
                inner
                    .forward_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            RawStore::V2(inner) => {
                inner
                    .forward_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
        }
    }

    pub async fn reverse_raw_scan(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        end_key: Option<&'a Key>,
        limit: usize,
        statistics: &'a mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut option = IterOptions::default();
        if let Some(end) = end_key {
            option.set_lower_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        match self {
            RawStore::V1(inner) => {
                if key_only {
                    option.set_key_only(key_only);
                }
                inner
                    .reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            RawStore::V1TTL(inner) => {
                inner
                    .reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            RawStore::V2(inner) => {
                inner
                    .reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
        }
    }

    pub async fn raw_checksum_ranges(
        &'a self,
        cf: CfName,
        ranges: Vec<KeyRange>,
        statistics: &'a mut Statistics,
    ) -> Result<(u64, u64, u64)> {
        match self {
            RawStore::V1(inner) => inner.raw_checksum_ranges(cf, ranges, statistics).await,
            RawStore::V1TTL(inner) => inner.raw_checksum_ranges(cf, ranges, statistics).await,
            RawStore::V2(inner) => inner.raw_checksum_ranges(cf, ranges, statistics).await,
        }
    }
}

pub struct RawStoreInner<S: Snapshot> {
    snapshot: S,
}

impl<'a, S: Snapshot> RawStoreInner<S> {
    pub fn new(snapshot: S) -> Self {
        RawStoreInner { snapshot }
    }

    pub fn raw_get_key_value(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<Vec<u8>>> {
        // no scan_count for this kind of op.
        let key_len = key.as_encoded().len();
        self.snapshot.get_cf(cf, key).map(|value| {
            stats.data.flow_stats.read_keys = 1;
            stats.data.flow_stats.read_bytes =
                key_len + value.as_ref().map(|v| v.len()).unwrap_or(0);
            value
        })
    }

    /// Scan raw keys in [`start_key`, `end_key`), returns at most `limit` keys. If `end_key` is
    /// `None`, it means unbounded.
    ///
    /// If `key_only` is true, the value corresponding to the key will not be read. Only scanned
    /// keys will be returned.
    pub async fn forward_raw_scan(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        limit: usize,
        statistics: &'a mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut cursor = Cursor::new(self.snapshot.iter_cf(cf, option)?, ScanMode::Forward, false);
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            pairs.push(Ok((
                cursor.key(statistics).to_owned(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            if pairs.len() < limit {
                cursor.next(statistics);
            } else {
                break;
            }
        }
        Ok(pairs)
    }

    /// Scan raw keys in [`end_key`, `start_key`) in reverse order, returns at most `limit` keys. If
    /// `start_key` is `None`, it means it's unbounded.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be returned.
    pub async fn reverse_raw_scan(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        limit: usize,
        statistics: &'a mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut cursor = Cursor::new(
            self.snapshot.iter_cf(cf, option)?,
            ScanMode::Backward,
            false,
        );
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            pairs.push(Ok((
                cursor.key(statistics).to_owned(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            if pairs.len() < limit {
                cursor.prev(statistics);
            } else {
                break;
            }
        }
        Ok(pairs)
    }

    pub async fn raw_checksum_ranges(
        &'a self,
        cf: CfName,
        ranges: Vec<KeyRange>,
        statistics: &'a mut Statistics,
    ) -> Result<(u64, u64, u64)> {
        let mut total_bytes = 0;
        let mut total_kvs = 0;
        let mut digest = crc64fast::Digest::new();
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        let statistics = statistics.mut_cf_statistics(cf);
        for r in ranges {
            let mut opts = IterOptions::new(None, None, false);
            opts.set_upper_bound(r.get_end_key(), DATA_KEY_PREFIX_LEN);
            let mut cursor =
                Cursor::new(self.snapshot.iter_cf(cf, opts)?, ScanMode::Forward, false);
            cursor.seek(&Key::from_encoded(r.get_start_key().to_vec()), statistics)?;
            while cursor.valid()? {
                row_count += 1;
                if row_count >= MAX_BATCH_SIZE {
                    if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                        reschedule().await;
                        time_slice_start = Instant::now();
                    }
                    row_count = 0;
                }
                let k = cursor.key(statistics);
                let v = cursor.value(statistics);
                digest.write(k);
                digest.write(v);
                total_kvs += 1;
                total_bytes += k.len() + v.len();
                cursor.next(statistics);
            }
        }
        Ok((digest.sum64(), total_kvs, total_bytes as u64))
    }
}

pub struct RawStoreV2Inner<S: Snapshot> {
    snapshot: S,
}

impl<'a, S: Snapshot> RawStoreV2Inner<S> {
    pub fn new(snapshot: S) -> Self {
        RawStoreV2Inner { snapshot }
    }

    pub fn raw_get_key_value(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<Vec<u8>>> {
        // no scan_count for this kind of op.
        let key_len = key.as_encoded().len();
        let upper_bound = key.clone().append_ts(TimeStamp::zero());
        let mut cursor = CursorBuilder::new(&self.snapshot, cf)
            .fill_cache(true) // fill cache?
            .scan_mode(ScanMode::Forward)
            .range(None, Some(upper_bound))
            .prefix_seek(true)
            .build()?;
        let statistics = stats.mut_cf_statistics(cf);
        if !cursor.seek(key, statistics)? {
            return Ok(None);
        }
        if cursor.valid()? && cursor.key(statistics) == key.as_encoded() {
            let value = cursor.value(statistics);
            stats.data.flow_stats.read_keys = 1;
            stats.data.flow_stats.read_bytes = key_len + value.len();
            Ok(Some(value.to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Scan raw keys in [`start_key`, `end_key`), returns at most `limit` keys. If `end_key` is
    /// `None`, it means unbounded.
    ///
    /// If `key_only` is true, the value corresponding to the key will not be read. Only scanned
    /// keys will be returned.
    pub async fn forward_raw_scan(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        limit: usize,
        statistics: &'a mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut cursor = Cursor::new(self.snapshot.iter_cf(cf, option)?, ScanMode::Forward, false);
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            let cur_key = cursor.key(statistics).to_owned();
            pairs.push(Ok((
                cur_key.clone(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            if pairs.len() < limit {
                cursor.next(statistics);
                while cursor.valid()? && cur_key == cursor.key(statistics) {
                    cursor.next(statistics);
                }
            } else {
                break;
            }
        }
        Ok(pairs)
    }

    /// Scan raw keys in [`end_key`, `start_key`) in reverse order, returns at most `limit` keys. If
    /// `start_key` is `None`, it means it's unbounded.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be returned.
    pub async fn reverse_raw_scan(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        limit: usize,
        statistics: &'a mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut cursor = Cursor::new(
            self.snapshot.iter_cf(cf, option)?,
            ScanMode::Backward,
            false,
        );
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        if !cursor.valid()? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut cur_key = vec![];
        let mut cur_value = vec![];
        let mut key_changed = true;
        while cursor.valid()? {
            if key_changed {
                cur_key = cursor.key(statistics).to_owned();
                if !key_only {
                    cur_value = cursor.value(statistics).to_owned()
                };
            }
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            cursor.prev(statistics);
            // if not valid, reach last one.
            if cursor.valid()? && cursor.key(statistics) == cur_key {
                key_changed = false;
                continue;
            }
            key_changed = true;
            pairs.push(Ok((cur_key.clone(), cur_value.clone())));
            if pairs.len() >= limit {
                break;
            }
        }
        Ok(pairs)
    }

    pub async fn raw_checksum_ranges(
        &'a self,
        cf: CfName,
        ranges: Vec<KeyRange>,
        statistics: &'a mut Statistics,
    ) -> Result<(u64, u64, u64)> {
        let mut total_bytes = 0;
        let mut total_kvs = 0;
        let mut digest = crc64fast::Digest::new();
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        let statistics = statistics.mut_cf_statistics(cf);
        for r in ranges {
            let mut opts = IterOptions::new(None, None, false);
            opts.set_upper_bound(r.get_end_key(), DATA_KEY_PREFIX_LEN);
            let mut cursor =
                Cursor::new(self.snapshot.iter_cf(cf, opts)?, ScanMode::Forward, false);
            cursor.seek(&Key::from_encoded(r.get_start_key().to_vec()), statistics)?;
            while cursor.valid()? {
                row_count += 1;
                if row_count >= MAX_BATCH_SIZE {
                    if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                        reschedule().await;
                        time_slice_start = Instant::now();
                    }
                    row_count = 0;
                }
                let k = cursor.key(statistics);
                let v = cursor.value(statistics);
                digest.write(k);
                digest.write(v);
                total_kvs += 1;
                total_bytes += k.len() + v.len();
                cursor.next(statistics);
            }
        }
        Ok((digest.sum64(), total_kvs, total_bytes as u64))
    }
}
