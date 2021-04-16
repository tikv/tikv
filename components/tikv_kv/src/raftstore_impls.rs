// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    self as kv, Error, Error as KvError, ErrorInner, Iterator as EngineIterator,
    Snapshot as EngineSnapshot,
};
use engine_traits::CfName;
use engine_traits::{IterOptions, Peekable, ReadOptions, Snapshot};
use raftstore::store::{RegionIterator, RegionSnapshot};
use raftstore::Error as RaftServerError;
use std::sync::atomic::Ordering;
use txn_types::{Key, Value};

impl From<RaftServerError> for Error {
    fn from(e: RaftServerError) -> Error {
        Error(Box::new(ErrorInner::Request(e.into())))
    }
}

impl<S: Snapshot> EngineSnapshot for RegionSnapshot<S> {
    type Iter = RegionIterator<S>;

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
        Ok(RegionSnapshot::iter(self, iter_opt))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> kv::Result<Self::Iter> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        RegionSnapshot::iter_cf(self, cf, iter_opt).map_err(kv::Error::from)
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        Some(self.get_start_key())
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        Some(self.get_end_key())
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.get_apply_index().ok()
    }

    fn is_max_ts_synced(&self) -> bool {
        self.max_ts_sync_status
            .as_ref()
            .map(|v| v.load(Ordering::SeqCst) & 1 == 1)
            .unwrap_or(false)
    }
}

impl<S: Snapshot> EngineIterator for RegionIterator<S> {
    fn next(&mut self) -> kv::Result<bool> {
        RegionIterator::next(self).map_err(KvError::from)
    }

    fn prev(&mut self) -> kv::Result<bool> {
        RegionIterator::prev(self).map_err(KvError::from)
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        RegionIterator::seek(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        RegionIterator::seek_for_prev(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        RegionIterator::seek_to_first(self).map_err(KvError::from)
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        RegionIterator::seek_to_last(self).map_err(KvError::from)
    }

    fn valid(&self) -> kv::Result<bool> {
        RegionIterator::valid(self).map_err(KvError::from)
    }

    fn validate_key(&self, key: &Key) -> kv::Result<()> {
        self.should_seekable(key.as_encoded()).map_err(From::from)
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        RegionIterator::value(self)
    }
}
