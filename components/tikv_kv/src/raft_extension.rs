// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! TiKV uses raft under the hook to provide consistency between replicas.
//! Though technically, `Engine` trait should hide the details of raft, but in
//! some cases it's unavoidable to access raft interface somehow. This module
//! supports the access pattern via extension.

use futures::{future::BoxFuture, Future};
use kvproto::{
    kvrpcpb::Context,
    metapb::{Region, RegionEpoch},
    raft_serverpb::RaftMessage,
};
use raft::SnapshotStatus;
use raftstore::store::region_meta::RegionMeta;

use crate::Result;

/// An interface to provide direct access to raftstore layer.
pub trait RaftExtension: Clone + Send {
    fn feed(&self, _msg: RaftMessage, _key_message: bool) {}
    fn report_reject_message(&self, _region_id: u64, _from_peer_id: u64) {}
    fn report_peer_unreachable(&self, _region_id: u64, _to_peer_id: u64) {}
    fn report_store_unreachable(&self, _store_id: u64) {}
    fn report_snapshot_status(&self, _region_id: u64, _to_peer_id: u64, _status: SnapshotStatus) {}
    fn report_resolved(&self, _store_id: u64, _group_id: u64) {}
    fn split(
        &self,
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _split_keys: Vec<Vec<u8>>,
        _source: String,
    ) -> BoxFuture<'static, Result<Vec<Region>>> {
        Box::pin(async move { Err(box_err!("raft split is not supported")) })
    }

    type ReadIndexRes: Future<Output = Result<u64>> = futures::future::Ready<Result<u64>>;
    fn read_index(&self, _ctx: &Context) -> Self::ReadIndexRes {
        unimplemented!("read_index is not supported")
    }

    fn query_region(&self, _region_id: u64) -> BoxFuture<'static, Result<RegionMeta>> {
        Box::pin(async move { Err(box_err!("query region is not supported")) })
    }

    fn check_consistency(&self, _region_id: u64) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move { Err(box_err!("consistency check is not supported")) })
    }
}

#[derive(Clone)]
pub struct NotSupported;

impl RaftExtension for NotSupported {}
