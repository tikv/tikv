// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfName, IterOptions, ReadOptions};

use crate::{self as kv, Error, ErrorInner};

use crate::{DummySnapshotExt, Iterator, Snapshot};
use kvengine::SnapAccess;
use rfstore::Error as RaftServerError;
use txn_types::{Key, Value};

impl From<RaftServerError> for Error {
    fn from(e: RaftServerError) -> Error {
        Error(Box::new(ErrorInner::Request(e.into())))
    }
}

#[allow(unused)]
impl Snapshot for rfstore::store::RegionSnapshot {
    type Iter = rfstore::store::RegionSnapshotIterator;
    type Ext<'a> = DummySnapshotExt;

    fn get(&self, key: &Key) -> kv::Result<Option<Value>> {
        unreachable!()
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        unreachable!()
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        unreachable!()
    }

    fn iter(&self, iter_opt: IterOptions) -> kv::Result<Self::Iter> {
        unreachable!()
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> kv::Result<Self::Iter> {
        unreachable!()
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        unreachable!()
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        unreachable!()
    }

    fn ext(&self) -> DummySnapshotExt {
        DummySnapshotExt
    }

    fn get_kvengine_snap(&self) -> Option<&SnapAccess> {
        Some(&self.snap)
    }
}

#[allow(dead_code)]
impl Iterator for rfstore::store::RegionSnapshotIterator {
    fn next(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn prev(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek(&mut self, _key: &Key) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_for_prev(&mut self, _key: &Key) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn valid(&self) -> kv::Result<bool> {
        unreachable!()
    }

    fn key(&self) -> &[u8] {
        unreachable!()
    }

    fn value(&self) -> &[u8] {
        unreachable!()
    }
}
