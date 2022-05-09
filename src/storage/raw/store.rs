// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, time::Duration};

use api_version::{ApiV1, ApiV1Ttl, ApiV2, KvFormat};
use engine_traits::{CfName, IterOptions, DATA_KEY_PREFIX_LEN};
use kvproto::kvrpcpb::{ApiVersion, KeyRange};
use tikv_util::time::Instant;
use txn_types::{Key, KvPair};
use yatp::task::future::reschedule;

use super::{encoded::RawEncodeSnapshot, raw_mvcc::RawMvccSnapshot};
use crate::{
    coprocessor::checksum_crc64_xor,
    storage::{
        kv::{Cursor, Result, ScanMode, Snapshot},
        Statistics,
    },
};

const MAX_TIME_SLICE: Duration = Duration::from_millis(2);
const MAX_BATCH_SIZE: usize = 1024;

// TODO: refactor to utilize generic type `KvFormat` and eliminate matching `api_version`.
pub enum RawStore<S: Snapshot> {
    V1(RawStoreInner<S, ApiV1>),
    V1Ttl(RawStoreInner<RawEncodeSnapshot<S, ApiV1Ttl>, ApiV1Ttl>),
    V2(RawStoreInner<RawEncodeSnapshot<RawMvccSnapshot<S>, ApiV2>, ApiV2>),
}

impl<'a, S: Snapshot> RawStore<S> {
    pub fn new(snapshot: S, api_version: ApiVersion) -> Self {
        match api_version {
            ApiVersion::V1 => RawStore::V1(RawStoreInner::new(snapshot)),
            ApiVersion::V1ttl => RawStore::V1Ttl(RawStoreInner::new(
                RawEncodeSnapshot::from_snapshot(snapshot),
            )),
            ApiVersion::V2 => RawStore::V2(RawStoreInner::new(RawEncodeSnapshot::from_snapshot(
                RawMvccSnapshot::from_snapshot(snapshot),
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
            RawStore::V1Ttl(inner) => inner.raw_get_key_value(cf, key, stats),
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
            RawStore::V1Ttl(inner) => inner.snapshot.get_key_ttl_cf(cf, key, stats),
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
            RawStore::V1Ttl(inner) => {
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
            RawStore::V1Ttl(inner) => {
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
        ranges: &[KeyRange],
        statistics: &'a mut Vec<Statistics>,
    ) -> Result<(u64, u64, u64)> {
        match self {
            RawStore::V1(inner) => inner.raw_checksum_ranges(cf, ranges, statistics).await,
            RawStore::V1Ttl(inner) => inner.raw_checksum_ranges(cf, ranges, statistics).await,
            RawStore::V2(inner) => inner.raw_checksum_ranges(cf, ranges, statistics).await,
        }
    }
}

pub struct RawStoreInner<S: Snapshot, F: KvFormat> {
    snapshot: S,
    _phantom: PhantomData<F>,
}

impl<'a, S: Snapshot, F: KvFormat> RawStoreInner<S, F> {
    pub fn new(snapshot: S) -> Self {
        RawStoreInner {
            snapshot,
            _phantom: PhantomData,
        }
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
        ranges: &[KeyRange],
        statistics: &'a mut Vec<Statistics>,
    ) -> Result<(u64, u64, u64)> {
        let mut total_bytes = 0;
        let mut total_kvs = 0;
        let digest = crc64fast::Digest::new();
        let mut checksum: u64 = 0;
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        for r in ranges {
            let mut stats = Statistics::default();
            let cf_stats = stats.mut_cf_statistics(cf);
            let mut opts = IterOptions::new(None, None, false);
            opts.set_upper_bound(r.get_end_key(), DATA_KEY_PREFIX_LEN);
            let mut cursor =
                Cursor::new(self.snapshot.iter_cf(cf, opts)?, ScanMode::Forward, false);
            cursor.seek(&Key::from_encoded(r.get_start_key().to_vec()), cf_stats)?;
            while cursor.valid()? {
                row_count += 1;
                if row_count >= MAX_BATCH_SIZE {
                    if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                        reschedule().await;
                        time_slice_start = Instant::now();
                    }
                    row_count = 0;
                }
                // Calculate checksum on user key, as timestamp is not visible on client side.
                let v = cursor.value(cf_stats);
                let (raw_key, _) =
                    F::decode_raw_key_owned(Key::from_encoded_slice(cursor.key(cf_stats)), true)?;
                checksum = checksum_crc64_xor(checksum, digest.clone(), &raw_key, v);
                total_kvs += 1;
                total_bytes += raw_key.len() + v.len();
                cursor.next(cf_stats);
            }
            statistics.push(stats);
        }
        Ok((checksum, total_kvs, total_bytes as u64))
    }
}
