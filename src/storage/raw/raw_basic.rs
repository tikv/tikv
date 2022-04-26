// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Iterator, Result, Snapshot};
use api_version::{APIVersion, APIV2};

use engine_traits::CfName;
use engine_traits::{IterOptions, ReadOptions};
use tikv_kv::Modify;
use txn_types::{Key, Value};

#[derive(Clone)]
pub struct RawBasicSnapshot<S: Snapshot> {
    snap: S,
}

impl<S: Snapshot> RawBasicSnapshot<S> {
    pub fn from_snapshot(snap: S) -> Self {
        RawBasicSnapshot { snap }
    }
}

pub struct MvccRaw {
    pub(crate) write_size: usize,
    pub(crate) modifies: Vec<Modify>,
}

impl MvccRaw {
    pub fn new() -> MvccRaw {
        // FIXME: use session variable to indicate fill cache or not.

        MvccRaw {
            write_size: 0,
            modifies: vec![],
        }
    }
    pub fn write_size(&self) -> usize {
        self.write_size
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.modifies
    }
}

impl<S: Snapshot> Snapshot for RawBasicSnapshot<S> {
    type Iter = RawBasicIterator<S::Iter>;
    type Ext<'a>
    where
        S: 'a,
    = S::Ext<'a>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.snap.get(key)
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.snap.get_cf(cf, key)
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.snap.get_cf_opt(opts, cf, key)
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawBasicIterator::new(self.snap.iter(iter_opt)?))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawBasicIterator::new(self.snap.iter_cf(cf, iter_opt)?))
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

pub struct RawBasicIterator<I: Iterator> {
    inner: I,
}

impl<I: Iterator> RawBasicIterator<I> {
    fn new(inner: I) -> Self {
        RawBasicIterator { inner }
    }
}

// RawBasicIterator always return the latest ts of user key.
// ts is desc encoded after user key, so it's placed the first one for the same user key.
// Only one-way direction scan is supported. Like `seek` then `next` or `seek_for_prev` then `prev`
impl<I: Iterator> Iterator for RawBasicIterator<I> {
    fn next(&mut self) -> Result<bool> {
        let res = self.inner.next();
        res
    }

    fn prev(&mut self) -> Result<bool> {
        let res = self.inner.prev();
        res
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        let res = self.inner.seek(key);
        res
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        let res = self.inner.seek_for_prev(key);
        res
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        let res = self.inner.seek_to_first();
        res
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let res = self.inner.seek_to_last();
        res
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
        APIV2::decode_raw_value(self.inner.value())
            .unwrap()
            .user_value
    }
}
