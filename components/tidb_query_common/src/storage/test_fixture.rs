// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, btree_map},
    sync::Arc,
};

use super::{OwnedKvPairEntry, Result, range::*};

pub type ErrorBuilder = Box<dyn Send + Sync + Fn() -> crate::error::StorageError>;

pub type FixtureValue = std::result::Result<Vec<u8>, ErrorBuilder>;

/// A `Storage` implementation that returns fixed source data (i.e. fixture).
/// Useful in tests.
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

impl<'a, 'b> From<&'b [(&'a [u8], &'a [u8])]> for FixtureStorage {
    fn from(v: &'b [(&'a [u8], &'a [u8])]) -> FixtureStorage {
        let tree: BTreeMap<_, _> = v
            .iter()
            .map(|(k, v)| (k.to_vec(), Ok(v.to_vec())))
            .collect();
        Self::new(tree)
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
        _load_commit_ts: bool,
        range: IntervalRange,
    ) -> Result<()> {
        let data_view = self
            .data
            .range(range.lower_inclusive..range.upper_exclusive);

        // Erase the lifetime to be 'static.
        self.data_view_unsafe = unsafe {
            #[allow(clippy::missing_transmute_annotations)]
            Some(std::mem::transmute(data_view))
        };
        self.is_backward_scan = is_backward_scan;
        self.is_key_only = is_key_only;
        Ok(())
    }

    fn scan_next_entry(&mut self) -> Result<Option<super::OwnedKvPairEntry>> {
        let value = if !self.is_backward_scan {
            // During the call of this function, `data` must be valid and we are only
            // returning data clones to outside, so this access is safe.
            self.data_view_unsafe.as_mut().unwrap().next()
        } else {
            self.data_view_unsafe.as_mut().unwrap().next_back()
        };
        match value {
            None => Ok(None),
            Some((k, Ok(v))) => {
                if !self.is_key_only {
                    Ok(Some(OwnedKvPairEntry {
                        key: k.clone(),
                        value: v.clone(),
                        commit_ts: None,
                    }))
                } else {
                    Ok(Some(OwnedKvPairEntry {
                        key: k.clone(),
                        value: Vec::new(),
                        commit_ts: None,
                    }))
                }
            }
            Some((_k, Err(err_producer))) => Err(err_producer()),
        }
    }

    fn get_entry(
        &mut self,
        is_key_only: bool,
        _load_commit_ts: bool,
        range: PointRange,
    ) -> Result<Option<super::OwnedKvPairEntry>> {
        let r = self.data.get(&range.0);
        match r {
            None => Ok(None),
            Some(Ok(v)) => {
                if !is_key_only {
                    Ok(Some(OwnedKvPairEntry {
                        key: range.0,
                        value: v.clone(),
                        commit_ts: None,
                    }))
                } else {
                    Ok(Some(OwnedKvPairEntry {
                        key: range.0,
                        value: Vec::new(),
                        commit_ts: None,
                    }))
                }
            }
            Some(Err(err_producer)) => Err(err_producer()),
        }
    }

    fn collect_statistics(&mut self, _dest: &mut Self::Statistics) {}

    fn met_uncacheable_data(&self) -> Option<bool> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    #[test]
    fn test_basic() {
        let data: &[(&'static [u8], &'static [u8])] = &[
            (b"foo", b"1"),
            (b"bar", b"2"),
            (b"foo_2", b"3"),
            (b"bar_2", b"4"),
            (b"foo_3", b"5"),
        ];
        let mut storage = FixtureStorage::from(data);

        // Get Key only = false
        assert_eq!(storage.get(false, PointRange::from("a")).unwrap(), None);
        assert_eq!(
            storage.get(false, PointRange::from("foo")).unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );

        // Get Key only = true
        assert_eq!(storage.get(true, PointRange::from("a")).unwrap(), None);
        assert_eq!(
            storage.get(true, PointRange::from("foo")).unwrap(),
            Some((b"foo".to_vec(), Vec::new()))
        );

        // Scan Backward = false, Key only = false
        storage
            .begin_scan(false, false, false, IntervalRange::from(("foo", "foo_3")))
            .unwrap();

        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );

        let mut s2 = storage.clone();
        assert_eq!(
            s2.scan_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );

        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(storage.scan_next().unwrap(), None);
        assert_eq!(storage.scan_next().unwrap(), None);

        assert_eq!(s2.scan_next().unwrap(), None);
        assert_eq!(s2.scan_next().unwrap(), None);

        // Scan Backward = false, Key only = false
        storage
            .begin_scan(false, false, false, IntervalRange::from(("bar", "bar_2")))
            .unwrap();

        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(storage.scan_next().unwrap(), None);

        // Scan Backward = false, Key only = true
        storage
            .begin_scan(false, true, false, IntervalRange::from(("bar", "foo_")))
            .unwrap();

        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"bar".to_vec(), Vec::new()))
        );
        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"bar_2".to_vec(), Vec::new()))
        );
        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"foo".to_vec(), Vec::new()))
        );
        assert_eq!(storage.scan_next().unwrap(), None);

        // Scan Backward = true, Key only = false
        storage
            .begin_scan(true, false, false, IntervalRange::from(("foo", "foo_3")))
            .unwrap();

        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            storage.scan_next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(storage.scan_next().unwrap(), None);
        assert_eq!(storage.scan_next().unwrap(), None);

        // Scan empty range
        storage
            .begin_scan(false, false, false, IntervalRange::from(("faa", "fab")))
            .unwrap();
        assert_eq!(storage.scan_next().unwrap(), None);

        storage
            .begin_scan(false, false, false, IntervalRange::from(("foo", "foo")))
            .unwrap();
        assert_eq!(storage.scan_next().unwrap(), None);
    }
}
