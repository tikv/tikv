// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;

use super::range::*;
use crate::coprocessor::{Error, Result};

type ErrorBuilder = Box<dyn Send + Sync + Fn() -> Error>;

type FixtureValue = std::result::Result<Vec<u8>, ErrorBuilder>;

/// A `Storage` implementation that returns fixed source data (i.e. fixture). Useful in tests.
#[derive(Clone)]
pub struct FixtureStorage {
    data: Arc<BTreeMap<Vec<u8>, FixtureValue>>,
    data_view_unsafe: Option<btree_map::Range<'static, Vec<u8>, FixtureValue>>,
    is_backward_scan: bool,
    is_key_only: bool,
}

impl FixtureStorage {
    pub fn new(data: BTreeMap<Vec<u8>, FixtureValue>) -> Self {
        Self {
            data: Arc::new(data),
            data_view_unsafe: None,
            is_backward_scan: false,
            is_key_only: false,
        }
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>)>> for FixtureStorage {
    fn from(v: Vec<(Vec<u8>, Vec<u8>)>) -> FixtureStorage {
        let tree: BTreeMap<_, _> = v.into_iter().map(|(k, v)| (k, Ok(v))).collect();
        Self::new(tree)
    }
}

impl super::Storage for FixtureStorage {
    type Statistics = ();

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        range: IntervalRange,
    ) -> Result<()> {
        let data_view = self
            .data
            .range(range.lower_inclusive..range.upper_exclusive);
        // Erase the lifetime to be 'static.
        self.data_view_unsafe = unsafe { Some(std::mem::transmute(data_view)) };
        self.is_backward_scan = is_backward_scan;
        self.is_key_only = is_key_only;
        Ok(())
    }

    fn scan_next(&mut self) -> Result<Option<super::OwnedKvPair>> {
        let value = if !self.is_backward_scan {
            // During the call of this function, `data` must be valid and we are only returning
            // data clones to outside, so this access is safe.
            self.data_view_unsafe.as_mut().unwrap().next()
        } else {
            self.data_view_unsafe.as_mut().unwrap().next_back()
        };
        match value {
            None => Ok(None),
            Some((k, Ok(v))) => {
                if !self.is_key_only {
                    Ok(Some((k.clone(), v.clone())))
                } else {
                    Ok(Some((k.clone(), Vec::new())))
                }
            }
            Some((_k, Err(err_producer))) => Err(err_producer()),
        }
    }

    fn get(&mut self, _is_key_only: bool, range: PointRange) -> Result<Option<super::OwnedKvPair>> {
        let r = self.data.get(&range.0);
        match r {
            None => Ok(None),
            Some(Ok(v)) => Ok(Some((range.0, v.clone()))),
            Some(Err(err_producer)) => Err(err_producer()),
        }
    }

    fn collect_statistics(&mut self, _dest: &mut Self::Statistics) {}
}
