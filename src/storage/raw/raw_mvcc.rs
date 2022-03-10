// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Iterator, Result, Snapshot};

use engine_traits::{CfName, DATA_KEY_PREFIX_LEN};
use engine_traits::{IterOptions, ReadOptions};
use tikv_util::codec::number::U64_SIZE;
use txn_types::{Key, TimeStamp, Value};

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
}
fn is_user_key_eq(left: &[u8], right: &[u8]) -> bool {
    let len = left.len();
    if len != right.len() {
        return false;
    }
    Key::is_user_key_eq(left, &right[..len - U64_SIZE])
}

impl<I: Iterator> RawMvccIterator<I> {
    fn new(inner: I) -> Self {
        RawMvccIterator {
            inner,
            cur_key: None,
            cur_value: None,
            is_valid: None,
        }
    }

    fn update_cur_kv(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.cur_key = Some(key);
        self.cur_value = Some(val);
        self.is_valid = Some(true);
    }

    fn clear_cur_kv(&mut self) {
        self.cur_key = None;
        self.cur_value = None;
        self.is_valid = None;
    }

    fn move_to_prev_max_ts(&mut self, res: Result<bool>) -> Result<bool> {
        if *res.as_ref().unwrap() && self.inner.valid()? {
            self.update_cur_kv(self.inner.key().to_vec(), self.inner.value().to_vec());
            self.inner.prev()?;
        } else {
            self.clear_cur_kv();
            return Ok(false);
        }
        while *res.as_ref().unwrap() && self.inner.valid()? {
            if is_user_key_eq(self.cur_key.as_ref().unwrap(), self.inner.key()) {
                self.update_cur_kv(self.inner.key().to_vec(), self.inner.value().to_vec());
                self.inner.prev()?;
            } else {
                break;
            }
        }
        res
    }
}

impl<I: Iterator> Iterator for RawMvccIterator<I> {
    fn next(&mut self) -> Result<bool> {
        let cur_key = self.inner.key().to_owned();
        let mut res = self.inner.next();
        while *res.as_ref().unwrap()
            && self.inner.valid()?
            && is_user_key_eq(&cur_key, self.inner.key())
        {
            res = self.inner.next();
        }
        self.clear_cur_kv();
        res
    }

    fn prev(&mut self) -> Result<bool> {
        self.move_to_prev_max_ts(Ok(true))
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        self.inner.seek(key)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        let res = self.inner.seek_for_prev(key);
        self.move_to_prev_max_ts(res)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        self.inner.seek_to_first()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let res = self.inner.seek_to_last();
        self.move_to_prev_max_ts(res)
    }

    fn valid(&self) -> Result<bool> {
        self.is_valid.map_or_else(|| self.inner.valid(), |v| Ok(v))
    }

    fn validate_key(&self, key: &Key) -> Result<()> {
        self.inner.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        // need map_or_else to lazy evaluate the default func, as it will abort when invalid.
        self.cur_key
            .as_ref()
            .map_or_else(|| self.inner.key(), |k| k)
    }

    fn value(&self) -> &[u8] {
        self.cur_value
            .as_ref()
            .map_or_else(|| self.inner.value(), |v| v)
    }
}
