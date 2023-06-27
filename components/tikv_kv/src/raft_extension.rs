// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! TiKV uses raft under the hook to provide consistency between replicas.
//! Though technically, `Engine` trait should hide the details of raft, but in
//! some cases it's unavoidable to access raft interface somehow. This module
//! supports the access pattern via extension.

use futures::future::BoxFuture;
use kvproto::{
    metapb::{Region, RegionEpoch},
    raft_serverpb::RaftMessage,
};
use raft::SnapshotStatus;
use raftstore::store::region_meta::RegionMeta;

use crate::Result;

/// An interface to provide direct access to raftstore layer.
pub trait RaftExtension: Clone + Send {
    /// Feed the message to the raft group.
    ///
    /// If it's a `key_message` is true, it will log a warning if the message
    /// failed to send.
    fn feed(&self, _msg: RaftMessage, _key_message: bool) {}

    /// Retport the message is rejected by the remote peer.
    fn report_reject_message(&self, _region_id: u64, _from_peer_id: u64) {}

    /// Report the target peer is unreachable.
    fn report_peer_unreachable(&self, _region_id: u64, _to_peer_id: u64) {}

    /// Report the target store is unreachable.
    fn report_store_unreachable(&self, _store_id: u64) {}

    /// Report the status of snapshot.
    fn report_snapshot_status(&self, _region_id: u64, _to_peer_id: u64, _status: SnapshotStatus) {}

    /// Report the address of a store is resolved.
    fn report_resolved(&self, _store_id: u64, _group_id: u64) {}

    /// Split the region with the given keys.
    ///
    /// Use `BoxFuture` for simplicity as it's not performance critical path.
    fn split(
        &self,
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _split_keys: Vec<Vec<u8>>,
        _source: String,
    ) -> BoxFuture<'static, Result<Vec<Region>>> {
        Box::pin(async move { Err(box_err!("raft split is not supported")) })
    }

    /// Get the region meta of the given region.
    fn query_region(&self, _region_id: u64) -> BoxFuture<'static, Result<RegionMeta>> {
        Box::pin(async move { Err(box_err!("query region is not supported")) })
    }

    /// Ask the raft group to do a consistency check.
    fn check_consistency(&self, _region_id: u64) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move { Err(box_err!("consistency check is not supported")) })
    }
}

/// An extension that does nothing or panic on all operations.
#[derive(Clone)]
pub struct FakeExtension;

impl RaftExtension for FakeExtension {}
