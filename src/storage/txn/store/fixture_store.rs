// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;

use crate::storage::txn::Result;
use crate::storage::{Key, Statistics};

/// A Store that reads on fixtures.
#[derive(Clone)]
pub struct FixtureStore {
    data: Arc<BTreeMap<Key, Result<Vec<u8>>>>,
}

impl FixtureStore {
    pub fn new(data: BTreeMap<Key, Result<Vec<u8>>>) -> Self {
        FixtureStore {
            data: Arc::new(data),
        }
    }
}

impl super::Store for FixtureStore {
    type Scanner = FixtureStoreScanner;

    #[inline]
    fn get(&self, key: &Key, _statistics: &mut Statistics) -> Result<Option<Vec<u8>>> {
        let r = self.data.get(key);
        match r {
            None => Ok(None),
            Some(Ok(v)) => Ok(Some(v.clone())),
            Some(Err(e)) => Err(e.maybe_clone().unwrap()),
        }
    }

    #[inline]
    fn batch_get(&self, keys: &[Key], statistics: &mut Statistics) -> Vec<Result<Option<Vec<u8>>>> {
        keys.iter().map(|key| self.get(key, statistics)).collect()
    }

    #[inline]
    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<FixtureStoreScanner> {
        use std::ops::Bound;

        let lower = lower_bound.as_ref().map_or(Bound::Unbounded, |v| {
            if !desc {
                Bound::Included(v)
            } else {
                Bound::Excluded(v)
            }
        });
        let upper = upper_bound.as_ref().map_or(Bound::Unbounded, |v| {
            if desc {
                Bound::Included(v)
            } else {
                Bound::Excluded(v)
            }
        });

        let range_data_ref = self.data.clone();
        // Erase the lifetime to be 'static.
        let range_unsafe = unsafe { std::mem::transmute(range_data_ref.range((lower, upper))) };

        Ok(FixtureStoreScanner {
            key_only,
            desc,
            _range_data_ref: range_data_ref,
            range_unsafe,
        })
    }
}

/// A Scanner that scans on fixtures.
// TODO: Replace it using GATs when available, to avoid `Arc` cost.
#[allow(clippy::type_complexity)]
pub struct FixtureStoreScanner {
    key_only: bool,
    desc: bool,
    /// Accessing this field is unsafe, since it's lifetime is faked. This field is valid only if
    /// `range_data_ref` is valid. When `range_data_ref` is destroyed, this field is no longer
    /// valid. Thus we must *NEVER* leak the reference of this field to the outside.
    range_unsafe: btree_map::Range<'static, Key, Result<Vec<u8>>>,
    _range_data_ref: Arc<BTreeMap<Key, Result<Vec<u8>>>>,
}

impl super::Scanner for FixtureStoreScanner {
    #[inline]
    fn next(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
        let value = if !self.desc {
            // During the call of this function, `range_data_ref` must be valid and we are only
            // returning data clones to outside, so this access is safe, which does not violate
            // the *real* life time restriction.
            self.range_unsafe.next()
        } else {
            self.range_unsafe.next_back()
        };
        match value {
            None => Ok(None),
            Some((k, Ok(v))) => {
                if !self.key_only {
                    Ok(Some((k.clone(), v.clone())))
                } else {
                    Ok(Some((k.clone(), Vec::new())))
                }
            }
            Some((_k, Err(e))) => Err(e.maybe_clone().unwrap()),
        }
    }

    #[inline]
    fn take_statistics(&mut self) -> Statistics {
        Statistics::default()
    }
}
