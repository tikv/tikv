// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{self as kv, DummySnapshotExt, Error, Error as KvError, ErrorInner, Iterator as EngineIterator, Snapshot as EngineSnapshot};
use engine_traits::CfName;
use engine_traits::{IterOptions, Peekable, ReadOptions, Snapshot};
use rfstore::store::{RegionIterator, RegionSnapshot};
use rfstore::Error as RaftServerError;
use std::sync::atomic::Ordering;
use txn_types::{Key, Value};

impl From<RaftServerError> for Error {
    fn from(e: RaftServerError) -> Error {
        Error(Box::new(ErrorInner::Request(e.into())))
    }
}

impl EngineSnapshot for RegionSnapshot {
    type Iter = RegionIterator;
    type Ext<'a> = DummySnapshotExt;

    fn get(&self, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get", |_| Err(box_err!(
            "injected error for get"
        )));
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf(cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf_opt(&opts, cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOptions) -> kv::Result<Self::Iter> {
        fail_point!("raftkv_snapshot_iter", |_| Err(box_err!(
            "injected error for iter"
        )));
        Ok(RegionSnapshot::iter(self, iter_opt, false))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> kv::Result<Self::Iter> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        RegionSnapshot::iter_cf(self, cf, iter_opt, false).map_err(kv::Error::from)
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        Some(self.get_start_key())
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        Some(self.get_end_key())
    }

    fn ext(&self) -> DummySnapshotExt {
        DummySnapshotExt
    }
}

impl EngineIterator for RegionIterator {
    fn next(&mut self) -> kv::Result<bool> {
        assert!(!self.is_reverse());
        Ok(RegionIterator::next(self))
    }

    fn prev(&mut self) -> kv::Result<bool> {
        assert!(self.is_reverse());
        Ok(RegionIterator::next(self))
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        assert!(!self.is_reverse());
        Ok(RegionIterator::seek(self, key.as_encoded()))
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        assert!(self.is_reverse());
        Ok(RegionIterator::seek(self, key.as_encoded()))
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        assert!(!self.is_reverse());
        Ok(self.rewind())
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        assert!(self.is_reverse());
        Ok(self.rewind())
    }

    fn valid(&self) -> kv::Result<bool> {
        Ok(self.item().is_valid())
    }

    fn validate_key(&self, key: &Key) -> kv::Result<()> {
        Ok(())
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        panic!("not supported")
    }
}
