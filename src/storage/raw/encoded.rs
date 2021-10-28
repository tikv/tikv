// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::kv::{Iterator, Result, Snapshot, TTL_TOMBSTONE};
use crate::storage::Statistics;

use engine_traits::raw_value::{ttl_current_ts, RawValue};
use engine_traits::CfName;
use engine_traits::{IterOptions, ReadOptions};
use kvproto::kvrpcpb::ApiVersion;
use txn_types::{Key, Value};

// TODO: remove this
#[derive(Clone)]
pub struct RawEncodeSnapshot<S: Snapshot> {
    s: S,
    current_ts: u64,
    api_version: ApiVersion,
}

impl<S: Snapshot> RawEncodeSnapshot<S> {
    pub fn from_snapshot(s: S, api_version: ApiVersion) -> Self {
        RawEncodeSnapshot {
            s,
            current_ts: ttl_current_ts(),
            api_version,
        }
    }

    fn map_value(&self, value: Result<Option<Value>>) -> Result<Option<Value>> {
        match value? {
            Some(v) => {
                let raw_value = RawValue::from_owned_bytes(v, self.api_version)?;
                if raw_value
                    .expire_ts
                    .map(|expire_ts| expire_ts <= self.current_ts)
                    .unwrap_or(false)
                {
                    return Ok(None);
                }
                Ok(Some(raw_value.user_value))
            }
            None => Ok(None),
        }
    }

    pub fn get_key_ttl_cf(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<u64>> {
        stats.data.flow_stats.read_keys = 1;
        stats.data.flow_stats.read_bytes = key.as_encoded().len();
        if let Some(v) = self.s.get_cf(cf, key)? {
            stats.data.flow_stats.read_bytes += v.len();
            let raw_value = RawValue::from_bytes(&v, self.api_version)?;
            return match raw_value.expire_ts {
                Some(expire_ts) if expire_ts <= self.current_ts => Ok(None),
                Some(expire_ts) => Ok(Some(expire_ts - self.current_ts)),
                None => Ok(Some(0)),
            };
        }
        Ok(None)
    }
}

impl<S: Snapshot> Snapshot for RawEncodeSnapshot<S> {
    type Iter = RawEncodeIterator<S::Iter>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get(key))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get_cf(cf, key))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get_cf_opt(opts, cf, key))
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawEncodeIterator::new(
            self.s.iter(iter_opt)?,
            self.current_ts,
            self.api_version,
        ))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawEncodeIterator::new(
            self.s.iter_cf(cf, iter_opt)?,
            self.current_ts,
            self.api_version,
        ))
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        self.s.lower_bound()
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        self.s.upper_bound()
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.s.get_data_version()
    }

    fn is_max_ts_synced(&self) -> bool {
        self.s.is_max_ts_synced()
    }
}

pub struct RawEncodeIterator<I: Iterator> {
    i: I,
    current_ts: u64,
    skip_ttl: usize,
    api_version: ApiVersion,
}

impl<I: Iterator> RawEncodeIterator<I> {
    fn new(i: I, current_ts: u64, api_version: ApiVersion) -> Self {
        RawEncodeIterator {
            i,
            current_ts,
            skip_ttl: 0,
            api_version,
        }
    }

    fn find_valid_value(&mut self, mut res: Result<bool>, forward: bool) -> Result<bool> {
        loop {
            if res.is_err() {
                break;
            }

            if *res.as_ref().unwrap() {
                let raw_value = RawValue::from_bytes(self.i.value(), self.api_version)?;
                if raw_value
                    .expire_ts
                    .map(|expire_ts| expire_ts <= self.current_ts)
                    .unwrap_or(false)
                {
                    self.skip_ttl += 1;
                    res = if forward {
                        self.i.next()
                    } else {
                        self.i.prev()
                    };
                    continue;
                }
            }
            break;
        }
        res
    }
}

impl<I: Iterator> Drop for RawEncodeIterator<I> {
    fn drop(&mut self) {
        TTL_TOMBSTONE.with(|m| {
            *m.borrow_mut() += self.skip_ttl;
        });
    }
}

impl<I: Iterator> Iterator for RawEncodeIterator<I> {
    fn next(&mut self) -> Result<bool> {
        let res = self.i.next();
        self.find_valid_value(res, true)
    }

    fn prev(&mut self) -> Result<bool> {
        let res = self.i.prev();
        self.find_valid_value(res, false)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        let res = self.i.seek(key);
        self.find_valid_value(res, true)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        let res = self.i.seek_for_prev(key);
        self.find_valid_value(res, false)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        let res = self.i.seek_to_first();
        self.find_valid_value(res, true)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let res = self.i.seek_to_last();
        self.find_valid_value(res, false)
    }

    fn valid(&self) -> Result<bool> {
        self.i.valid()
    }

    fn validate_key(&self, key: &Key) -> Result<()> {
        self.i.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        self.i.key()
    }

    fn value(&self) -> &[u8] {
        RawValue::from_bytes(self.i.value(), self.api_version)
            .unwrap()
            .user_value
    }
}
