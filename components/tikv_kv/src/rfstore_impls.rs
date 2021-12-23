// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfName, IterOptions, Peekable, ReadOptions, Result as EngineResult};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::Region;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{
    self as kv, Error, Error as KvError, ErrorInner, Iterator as EngineIterator,
    Snapshot as EngineSnapshot, SnapshotExt,
};

use crate::{DummySnapshotExt, Iterator, Snapshot};
use engine_traits::Error as EngineError;
use kvengine::SnapAccess;
use rfstore::Error as RaftServerError;
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use txn_types::{Key, Value};

impl From<RaftServerError> for Error {
    fn from(e: RaftServerError) -> Error {
        Error(Box::new(ErrorInner::Request(e.into())))
    }
}

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

    fn get_kvengine_snap(&self) -> Option<&Arc<SnapAccess>> {
        Some(&self.snap)
    }
}

impl Iterator for rfstore::store::RegionSnapshotIterator {
    fn next(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn prev(&mut self) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        unreachable!()
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
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
