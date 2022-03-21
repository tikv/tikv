// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Error, ErrorInner, Iterator, Result, Snapshot};

use engine_traits::{CfName, DATA_KEY_PREFIX_LEN};
use engine_traits::{IterOptions, ReadOptions};
use txn_types::{Key, TimeStamp, Value};

const VEC_SHRINK_THRESHOLD: usize = 512; // shrink vec when it's over 512.

#[derive(Clone)]
pub struct RawMvccSnapshot<S: Snapshot> {
    snap: S,
}

impl<S: Snapshot> RawMvccSnapshot<S> {
    pub fn from_snapshot(snap: S) -> Self {
        RawMvccSnapshot { snap }
    }

    pub fn seek_first_key_value_cf(
        &self,
        cf: Option<CfName>,
        opts: Option<ReadOptions>,
        key: &Key,
    ) -> Result<Option<Value>> {
        let mut iter_opt = IterOptions::default();
        iter_opt.set_fill_cache(opts.map_or(true, |v| v.fill_cache()));
        iter_opt.use_prefix_seek();
        iter_opt.set_prefix_same_as_start(true);
        let end_key = key.clone().append_ts(TimeStamp::zero());
        iter_opt.set_upper_bound(end_key.as_encoded(), DATA_KEY_PREFIX_LEN);
        let mut iter = match cf {
            Some(cf_name) => self.iter_cf(cf_name, iter_opt)?,
            None => self.iter(iter_opt)?,
        };
        if iter.seek(key)? {
            Ok(Some(iter.value().to_owned()))
        } else {
            Ok(None)
        }
    }
}

impl<S: Snapshot> Snapshot for RawMvccSnapshot<S> {
    type Iter = RawMvccIterator<S::Iter>;
    type Ext<'a>
    where
        S: 'a,
    = S::Ext<'a>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.seek_first_key_value_cf(None, None, key)
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.seek_first_key_value_cf(Some(cf), None, key)
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.seek_first_key_value_cf(Some(cf), Some(opts), key)
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawMvccIterator::new(self.snap.iter(iter_opt)?))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawMvccIterator::new(self.snap.iter_cf(cf, iter_opt)?))
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

pub struct RawMvccIterator<I: Iterator> {
    inner: I,
    cur_key: Option<Vec<u8>>,
    cur_value: Option<Vec<u8>>,
    is_valid: Option<bool>,
    is_forward: bool,
}

fn is_user_key_eq(left: &[u8], right: &[u8]) -> bool {
    let len = left.len();
    if len != right.len() {
        return false;
    }
    // ensure all keys is encoded with ts.
    Key::is_user_key_eq(left, Key::truncate_ts_for(right).unwrap())
}

impl<I: Iterator> RawMvccIterator<I> {
    fn new(inner: I) -> Self {
        RawMvccIterator {
            inner,
            cur_key: None,
            cur_value: None,
            is_valid: None,
            is_forward: true,
        }
    }

    fn update_cur_kv(&mut self) {
        if let Some(ref mut key) = self.cur_key {
            key.clear();
            key.extend_from_slice(self.inner.key());
            if key.capacity() > key.len() * 2 && key.capacity() > VEC_SHRINK_THRESHOLD {
                key.shrink_to_fit();
            }
        } else {
            self.cur_key = Some(Vec::from(self.inner.key()));
        };
        if let Some(ref mut value) = self.cur_value {
            value.clear();
            value.extend_from_slice(self.inner.value());
            if value.capacity() > value.len() * 2 && value.capacity() > VEC_SHRINK_THRESHOLD {
                value.shrink_to_fit();
            }
        } else {
            self.cur_value = Some(Vec::from(self.inner.value()));
        };
        self.is_valid = Some(true);
    }

    fn clear_cur_kv(&mut self) {
        self.cur_key = None;
        self.cur_value = None;
        self.is_valid = None;
    }

    fn move_to_prev_max_ts(&mut self) -> Result<bool> {
        if self.inner.valid()? {
            self.update_cur_kv();
            self.inner.prev()?;
        } else {
            self.clear_cur_kv();
            return Ok(false);
        }
        while self.inner.valid()? {
            // cur_key should not be None here.
            if is_user_key_eq(self.cur_key.as_ref().unwrap(), self.inner.key()) {
                self.update_cur_kv();
                self.inner.prev()?;
            } else {
                break;
            }
        }
        Ok(true)
    }
}

// RawMvccIterator always return the latest ts of user key.
// ts is desc encoded after user key, so it's placed the first one for the same user key.
// Only one-way direction scan is supported. Like `seek` then `next` or `seek_for_prev` then `prev`
impl<I: Iterator> Iterator for RawMvccIterator<I> {
    fn next(&mut self) -> Result<bool> {
        if !self.is_forward {
            return Err(Error::from(ErrorInner::Other(Box::from(
                "invalid raw mvcc operation",
            ))));
        }
        let cur_key = self.inner.key().to_owned();
        let mut res = self.inner.next()?;
        while res && self.inner.valid()? && is_user_key_eq(&cur_key, self.inner.key()) {
            res = self.inner.next()?;
        }
        self.clear_cur_kv();
        Ok(res)
    }

    fn prev(&mut self) -> Result<bool> {
        if self.is_forward {
            return Err(Error::from(ErrorInner::Other(Box::from(
                "invalid raw mvcc operation",
            ))));
        }
        self.move_to_prev_max_ts()
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        self.is_forward = true;
        self.clear_cur_kv();
        self.inner.seek(key)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        self.is_forward = false;
        if self.inner.seek_for_prev(key)? {
            self.move_to_prev_max_ts()
        } else {
            Ok(false)
        }
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        self.is_forward = true;
        self.clear_cur_kv();
        self.inner.seek_to_first()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.is_forward = false;
        if self.inner.seek_to_last()? {
            self.move_to_prev_max_ts()
        } else {
            Ok(false)
        }
    }

    fn valid(&self) -> Result<bool> {
        self.is_valid.map_or_else(|| self.inner.valid(), Ok)
    }

    fn validate_key(&self, key: &Key) -> Result<()> {
        self.inner.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        // need map_or_else to lazy evaluate the default func, as it will abort when invalid.
        self.cur_key.as_deref().unwrap_or_else(|| self.inner.key())
    }

    fn value(&self) -> &[u8] {
        self.cur_value
            .as_deref()
            .unwrap_or_else(|| self.inner.value())
    }
}
