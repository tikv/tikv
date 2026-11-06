// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, time::Duration};

use api_version::{ApiV1, ApiV1Ttl, ApiV2, KvFormat};
use engine_traits::{CfName, DATA_KEY_PREFIX_LEN, IterOptions};
use kvproto::kvrpcpb::{ApiVersion, KeyRange};
use tikv_util::time::Instant;
use txn_types::{Key, KvPair};
use yatp::task::future::reschedule;

use super::{encoded::RawEncodeSnapshot, raw_mvcc::RawMvccSnapshot};
use crate::{
    coprocessor::checksum_crc64_xor,
    storage::{
        Statistics,
        kv::{Cursor, Result, ScanMode, Snapshot},
    },
};

const MAX_TIME_SLICE: Duration = Duration::from_millis(2);
const MAX_BATCH_SIZE: usize = 1024;

// TODO: refactor to utilize generic type `KvFormat` and eliminate matching
// `api_version`.
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
            ApiVersion::V3 => {
                unreachable!("API V3 is not supported by this TiKV build")
            }
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
            RawStore::V1Ttl(inner) => inner.snapshot.get_cf_with_stats(cf, key, stats),
            RawStore::V2(inner) => inner.snapshot.get_cf_with_stats(cf, key, stats),
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
        // No scan counters are maintained for this point operation.
        let value = self.snapshot.get_cf(cf, key)?;
        super::record_point_read(stats, key, value.as_ref());
        Ok(value)
    }

    /// Scan raw keys in [`start_key`, `end_key`), returns at most `limit` keys.
    /// If `end_key` is `None`, it means unbounded.
    ///
    /// If `key_only` is true, the value corresponding to the key will not be
    /// read. Only scanned keys will be returned.
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
        let mut cursor = Cursor::new(
            self.snapshot.iter(cf, option)?,
            ScanMode::Forward,
            false,
            false,
        );
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
                cursor.key().to_owned(),
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

    /// Scan raw keys in [`end_key`, `start_key`) in reverse order, returns at
    /// most `limit` keys. If `start_key` is `None`, it means it's unbounded.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be
    /// returned.
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
            self.snapshot.iter(cf, option)?,
            ScanMode::Backward,
            false,
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
                cursor.key().to_owned(),
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
            let mut cursor = Cursor::new(
                self.snapshot.iter(cf, opts)?,
                ScanMode::Forward,
                false,
                false,
            );
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
                    F::decode_raw_key_owned(Key::from_encoded_slice(cursor.key()), true)?;
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

#[cfg(test)]
mod tests {
    use api_version::{ApiV1, ApiV1Ttl, ApiV2, KvFormat, RawValue};
    use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, CfName};
    use futures::executor::block_on;
    use kvproto::kvrpcpb::{ApiVersion, Context};
    use tikv_kv::{BTreeEngine, Engine, Statistics};
    use txn_types::TimeStamp;

    use super::RawStore;

    fn encode_raw_value<F: KvFormat>(user_value: &[u8]) -> Vec<u8> {
        F::encode_raw_value_owned(RawValue {
            user_value: user_value.to_vec(),
            // Keep all records visible while exercising the TTL/MVCC decoder.
            expire_ts: F::IS_TTL_ENABLED.then_some(u64::MAX),
            is_delete: false,
        })
    }

    fn put_raw<F: KvFormat>(
        engine: &BTreeEngine,
        cf: CfName,
        user_key: &[u8],
        ts: Option<u64>,
        user_value: &[u8],
    ) {
        let key = F::encode_raw_key_owned(user_key.to_vec(), ts.map(TimeStamp::from));
        let value = encode_raw_value::<F>(user_value);
        engine
            .put_cf(&Context::default(), cf, key, value)
            .expect("put raw test value");
    }

    fn assert_point_read_flow<F: KvFormat>(cf: CfName, ts: Option<u64>) {
        let mut engine = BTreeEngine::default();
        let user_key = b"r\0point";
        let user_value = b"point-value";
        let encoded_value = encode_raw_value::<F>(user_value);
        put_raw::<F>(&engine, cf, user_key, ts, user_value);

        let key = F::encode_raw_key(user_key, None);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let store = RawStore::new(snapshot, F::TAG);
        let mut get_statistics = Statistics::default();
        assert_eq!(
            store
                .raw_get_key_value(cf, &key, &mut get_statistics)
                .unwrap(),
            Some(user_value.to_vec())
        );
        let mut reads = vec![get_statistics];
        if F::IS_TTL_ENABLED {
            let mut ttl_statistics = Statistics::default();
            assert!(
                store
                    .raw_get_key_ttl(cf, &key, &mut ttl_statistics)
                    .unwrap()
                    .is_some()
            );
            reads.push(ttl_statistics);
        }
        for statistics in reads {
            // Reads of every RawKV CF are charged to the default-CF flow, which
            // `ReadStats` reports to PD.
            assert_eq!(statistics.data.flow_stats.read_keys, 1, "cf={cf}");
            assert_eq!(
                statistics.data.flow_stats.read_bytes,
                key.len() + encoded_value.len(),
                "cf={cf}"
            );
            assert_eq!(statistics.lock.flow_stats.read_keys, 0, "cf={cf}");
            assert_eq!(statistics.write.flow_stats.read_keys, 0, "cf={cf}");
        }
    }

    #[test]
    fn test_raw_point_get_flow_statistics() {
        for cf in [CF_DEFAULT, CF_LOCK, CF_WRITE] {
            assert_point_read_flow::<ApiV1>(cf, None);
            assert_point_read_flow::<ApiV1Ttl>(cf, None);
        }
        // API V2 only accepts the default CF.
        assert_point_read_flow::<ApiV2>(CF_DEFAULT, Some(10));
    }

    fn assert_scan_flow<F: KvFormat>(key_only: bool, reverse: bool) {
        let mut engine = BTreeEngine::default();
        let records: [(&[u8], &[u8]); 2] = [(b"r\0a", b"one"), (b"r\0b", b"three")];
        for (user_key, user_value) in records {
            put_raw::<F>(&engine, CF_DEFAULT, user_key, Some(10), user_value);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let store = RawStore::new(snapshot, F::TAG);
        let start_key = if reverse {
            F::encode_raw_key(b"r\0z", None)
        } else {
            F::encode_raw_key(b"r\0a", None)
        };
        let mut statistics = Statistics::default();
        let pairs = if reverse {
            block_on(store.reverse_raw_scan(
                CF_DEFAULT,
                &start_key,
                None,
                2,
                &mut statistics,
                key_only,
            ))
        } else {
            block_on(store.forward_raw_scan(
                CF_DEFAULT,
                &start_key,
                None,
                2,
                &mut statistics,
                key_only,
            ))
        }
        .unwrap();
        assert_eq!(pairs.len(), 2);

        let key_bytes: usize = pairs
            .iter()
            .map(|pair| pair.as_ref().unwrap().0.len())
            .sum();
        let stored_value_bytes: usize = records
            .iter()
            .map(|(_, user_value)| encode_raw_value::<F>(user_value).len())
            .sum();
        // API V1 charges a value only when the scan reads it. The TTL and V2
        // iterators read every value they return to apply the TTL/delete
        // filter, so its stored length is charged once, even for key-only
        // scans.
        let value_bytes = if F::TAG == ApiVersion::V1 && key_only {
            0
        } else {
            stored_value_bytes
        };
        assert_eq!(statistics.data.flow_stats.read_keys, 2);
        assert_eq!(
            statistics.data.flow_stats.read_bytes,
            key_bytes + value_bytes,
            "key_only={key_only} reverse={reverse}"
        );
    }

    #[test]
    fn test_raw_scan_flow_statistics() {
        for key_only in [false, true] {
            for reverse in [false, true] {
                assert_scan_flow::<ApiV1>(key_only, reverse);
                assert_scan_flow::<ApiV1Ttl>(key_only, reverse);
                assert_scan_flow::<ApiV2>(key_only, reverse);
            }
        }
    }
}
