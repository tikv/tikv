// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::kv::{Iterator, Result, Snapshot, RAW_VALUE_TOMBSTONE};
use crate::storage::Statistics;

use api_version::KvFormat;
use engine_traits::raw_ttl::ttl_current_ts;
use engine_traits::CfName;
use engine_traits::{IterOptions, ReadOptions};
use std::marker::PhantomData;
use txn_types::{Key, Value};

#[derive(Clone)]
pub struct RawEncodeSnapshot<S: Snapshot, F: KvFormat> {
    snap: S,
    current_ts: u64,
    _phantom: PhantomData<F>,
}

impl<S: Snapshot, F: KvFormat> RawEncodeSnapshot<S, F> {
    pub fn from_snapshot(snap: S) -> Self {
        RawEncodeSnapshot {
            snap,
            current_ts: ttl_current_ts(),
            _phantom: PhantomData,
        }
    }

    fn map_value(&self, value: Result<Option<Value>>) -> Result<Option<Value>> {
        if let Some(v) = value? {
            let raw_value = F::decode_raw_value_owned(v)?;
            if raw_value.is_valid(self.current_ts) {
                return Ok(Some(raw_value.user_value));
            }
        }
        Ok(None)
    }

    pub fn get_key_ttl_cf(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<u64>> {
        stats.data.flow_stats.read_keys = 1;
        stats.data.flow_stats.read_bytes = key.as_encoded().len();
        if let Some(v) = self.snap.get_cf(cf, key)? {
            stats.data.flow_stats.read_bytes += v.len();
            let raw_value = F::decode_raw_value_owned(v)?;
            return match raw_value.expire_ts {
                Some(expire_ts) if expire_ts <= self.current_ts => Ok(None),
                Some(expire_ts) => Ok(Some(expire_ts - self.current_ts)),
                None => Ok(Some(0)),
            };
        }
        Ok(None)
    }
}

impl<S: Snapshot, F: KvFormat> Snapshot for RawEncodeSnapshot<S, F> {
    type Iter = RawEncodeIterator<S::Iter, F>;
    type Ext<'a>
    where
        S: 'a,
    = S::Ext<'a>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.snap.get(key))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.snap.get_cf(cf, key))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.snap.get_cf_opt(opts, cf, key))
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawEncodeIterator::new(
            self.snap.iter(iter_opt)?,
            self.current_ts,
        ))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawEncodeIterator::new(
            self.snap.iter_cf(cf, iter_opt)?,
            self.current_ts,
        ))
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        self.snap.lower_bound()
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        self.snap.upper_bound()
    }

    fn ext(&self) -> S::Ext<'_> {
        self.snap.ext()
    }
}

pub struct RawEncodeIterator<I: Iterator, F: KvFormat> {
    inner: I,
    current_ts: u64,
    skip_invalid: usize,
    _phantom: PhantomData<F>,
}

impl<I: Iterator, F: KvFormat> RawEncodeIterator<I, F> {
    fn new(inner: I, current_ts: u64) -> Self {
        RawEncodeIterator {
            inner,
            current_ts,
            skip_invalid: 0,
            _phantom: PhantomData,
        }
    }

    fn find_valid_value(&mut self, mut res: Result<bool>, forward: bool) -> Result<bool> {
        loop {
            if res.is_err() {
                break;
            }

            if *res.as_ref().unwrap() {
                let raw_value = F::decode_raw_value(self.inner.value())?;
                if !raw_value.is_valid(self.current_ts) {
                    self.skip_invalid += 1;
                    res = if forward {
                        self.inner.next()
                    } else {
                        self.inner.prev()
                    };
                    continue;
                }
            }
            break;
        }
        res
    }
}

impl<I: Iterator, F: KvFormat> Drop for RawEncodeIterator<I, F> {
    fn drop(&mut self) {
        RAW_VALUE_TOMBSTONE.with(|m| {
            *m.borrow_mut() += self.skip_invalid;
        });
    }
}

impl<I: Iterator, F: KvFormat> Iterator for RawEncodeIterator<I, F> {
    fn next(&mut self) -> Result<bool> {
        let res = self.inner.next();
        self.find_valid_value(res, true)
    }

    fn prev(&mut self) -> Result<bool> {
        let res = self.inner.prev();
        self.find_valid_value(res, false)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        let res = self.inner.seek(key);
        self.find_valid_value(res, true)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        let res = self.inner.seek_for_prev(key);
        self.find_valid_value(res, false)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        let res = self.inner.seek_to_first();
        self.find_valid_value(res, true)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let res = self.inner.seek_to_last();
        self.find_valid_value(res, false)
    }

    fn valid(&self) -> Result<bool> {
        self.inner.valid()
    }

    fn validate_key(&self, key: &Key) -> Result<()> {
        self.inner.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        F::decode_raw_value(self.inner.value()).unwrap().user_value
    }
}
