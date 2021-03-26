// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::ttl::TTLSnapshot;

use crate::storage::kv::{Cursor, Iterator, ScanMode, Snapshot};
use crate::storage::Statistics;
use crate::storage::{Error, Result};

use engine_traits::{CfName, IterOptions, DATA_KEY_PREFIX_LEN};
use kvproto::kvrpcpb::KeyRange;
use std::time::Duration;
use tikv_util::time::Instant;
use txn_types::{Key, KvPair};
use yatp::task::future::reschedule;

const MAX_TIME_SLICE: Duration = Duration::from_millis(2);
const MAX_BATCH_SIZE: usize = 1024;

pub enum RawStore<S: Snapshot> {
    Vanilla(RawStoreInner<S>),
    TTL(RawStoreInner<TTLSnapshot<S>>),
}

impl<'a, S: Snapshot> RawStore<S> {
    pub fn new(snapshot: S, enable_ttl: bool) -> Self {
        if enable_ttl {
            RawStore::TTL(RawStoreInner::new(TTLSnapshot::from(snapshot)))
        } else {
            RawStore::Vanilla(RawStoreInner::new(snapshot))
        }
    }

    pub fn raw_get_key_value(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<Vec<u8>>> {
        match self {
            RawStore::Vanilla(inner) => inner.raw_get_key_value(cf, key, stats),
            RawStore::TTL(inner) => inner.raw_get_key_value(cf, key, stats),
        }
    }

    pub fn raw_get_key_ttl(
        &self,
        cf: CfName,
        key: &'a Key,
        stats: &'a mut Statistics,
    ) -> Result<Option<u64>> {
        match self {
            RawStore::Vanilla(_) => panic!("get ttl on non-ttl store"),
            RawStore::TTL(inner) => inner
                .snapshot
                .get_key_ttl_cf(cf, key, stats)
                .map_err(Error::from),
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
            RawStore::Vanilla(inner) => {
                if key_only {
                    option.set_key_only(key_only);
                }
                inner
                    .forward_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            RawStore::TTL(inner) => {
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
            RawStore::Vanilla(inner) => {
                if key_only {
                    option.set_key_only(key_only);
                }
                inner
                    .reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            RawStore::TTL(inner) => {
                inner
                    .reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
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
        self.snapshot
            .get_cf(cf, key)
            .map(|value| {
                stats.data.flow_stats.read_keys = 1;
                stats.data.flow_stats.read_bytes =
                    key_len + value.as_ref().map(|v| v.len()).unwrap_or(0);
                value
            })
            .map_err(Error::from)
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
        let mut cursor = Cursor::new(self.snapshot.iter_cf(cf, option)?, ScanMode::Forward, false);
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(&start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? && pairs.len() < limit {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.elapsed() > MAX_TIME_SLICE {
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
            cursor.next(statistics);
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
        let mut cursor = Cursor::new(
            self.snapshot.iter_cf(cf, option)?,
            ScanMode::Backward,
            false,
        );
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(&start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? && pairs.len() < limit {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.elapsed() > MAX_TIME_SLICE {
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
            cursor.prev(statistics);
        }
        Ok(pairs)
    }
}

pub async fn raw_checksum_ranges<S: Snapshot>(
    snapshot: S,
    ranges: Vec<KeyRange>,
) -> Result<(u64, u64, u64)> {
    let mut total_bytes = 0;
    let mut total_kvs = 0;
    let mut digest = crc64fast::Digest::new();
    let mut row_count = 0;
    let mut time_slice_start = Instant::now();
    for r in ranges {
        let mut opts = IterOptions::new(None, None, false);
        opts.set_upper_bound(r.get_end_key(), DATA_KEY_PREFIX_LEN);
        let mut iter = snapshot.iter(opts)?;
        iter.seek(&Key::from_encoded(r.get_start_key().to_vec()))?;
        while iter.valid()? {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            let k = iter.key();
            let v = iter.value();
            digest.write(k);
            digest.write(v);
            total_kvs += 1;
            total_bytes += k.len() + v.len();
            iter.next()?;
        }
    }
    Ok((digest.sum64(), total_kvs, total_bytes as u64))
}
