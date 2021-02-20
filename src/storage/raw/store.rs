// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::ttl::TTLSnapshot;

use crate::storage::kv::{Result, ScanMode, Snapshot};
use crate::storage::Statistics;

use engine_traits::{CfName, IterOptions, DATA_KEY_PREFIX_LEN};
use txn_types::{Key, KvPair};

pub enum RawStore<S: Snapshot> {
    Vanilla(RawStoreInner<S>),
    TTL(RawStoreInner<TTLSnapshot<S>>),
}

impl<S: Snapshot> RawStore<S> {
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
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<u64>> {
        match self {
            RawStore::Vanilla(_) => panic!("get ttl on non-ttl store"),
            RawStore::TTL(inner) => inner.snapshot.get_key_ttl_cf(cf, key, stats),
        }
    }

    pub fn forward_raw_scan(
        &self,
        cf: CfName,
        start_key: &Key,
        end_key: Option<&Key>,
        limit: usize,
        statistics: &mut Statistics,
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
                inner.forward_raw_scan(cf, start_key, limit, statistics, option, key_only)
            }
            RawStore::TTL(inner) => {
                inner.forward_raw_scan(cf, start_key, limit, statistics, option, key_only)
            }
        }
    }

    pub fn reverse_raw_scan(
        &self,
        cf: CfName,
        start_key: &Key,
        end_key: Option<&Key>,
        limit: usize,
        statistics: &mut Statistics,
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
                inner.reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
            }
            RawStore::TTL(inner) => {
                inner.reverse_raw_scan(cf, start_key, limit, statistics, option, key_only)
            }
        }
    }
}

pub struct RawStoreInner<S: Snapshot> {
    snapshot: S,
}

impl<S: Snapshot> RawStoreInner<S> {
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
    pub fn forward_raw_scan(
        &self,
        cf: CfName,
        start_key: &Key,
        limit: usize,
        statistics: &mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut cursor = self.snapshot.iter_cf(cf, option, ScanMode::Forward)?;
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(&start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid()? && pairs.len() < limit {
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
    pub fn reverse_raw_scan(
        &self,
        cf: CfName,
        start_key: &Key,
        limit: usize,
        statistics: &mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut cursor = self.snapshot.iter_cf(cf, option, ScanMode::Backward)?;
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(&start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid()? && pairs.len() < limit {
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
