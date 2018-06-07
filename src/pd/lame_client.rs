// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::future;
use kvproto::{metapb, pdpb};

use super::{PdClient, PdFuture, RegionInfo, RegionStat, Result};

/// This is a handy trait for mocking pd client for tests, which will report error
/// for all actions by default.
pub trait LamePdClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Err(box_err!("calling lame pd client."))
    }

    fn bootstrap_cluster(&self, _: metapb::Store, _: metapb::Region) -> Result<()> {
        Err(box_err!("calling lame pd client."))
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        Err(box_err!("calling lame pd client."))
    }

    fn alloc_id(&self) -> Result<u64> {
        Err(box_err!("calling lame pd client."))
    }

    fn put_store(&self, _: metapb::Store) -> Result<()> {
        Err(box_err!("calling lame pd client."))
    }

    fn get_store(&self, _: u64) -> PdFuture<metapb::Store> {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn get_all_stores(&self) -> Result<Vec<metapb::Store>> {
        Err(box_err!("calling lame pd client."))
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        Err(box_err!("calling lame pd client."))
    }

    fn get_region(&self, _: &[u8]) -> Result<metapb::Region> {
        Err(box_err!("calling lame pd client."))
    }

    // Get region info which the key belong to.
    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        self.get_region(key)
            .map(|region| RegionInfo::new(region, None))
    }

    fn get_region_by_id(&self, _: u64) -> PdFuture<Option<metapb::Region>> {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn region_heartbeat(&self, _: metapb::Region, _: metapb::Peer, _: RegionStat) -> PdFuture<()> {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn handle_region_heartbeat_response<F>(&self, _: u64, _: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn ask_split(&self, _: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn store_heartbeat(&self, _: pdpb::StoreStats) -> PdFuture<()> {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn report_split(&self, _: metapb::Region, _: metapb::Region) -> PdFuture<()> {
        Box::new(future::err(box_err!("calling lame pd client.")))
    }

    fn scatter_region(&self, _: RegionInfo) -> Result<()> {
        Err(box_err!("calling lame pd client."))
    }

    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, _: F) {}
}

impl<T: LamePdClient + Sync + Send> PdClient for T {
    fn get_cluster_id(&self) -> Result<u64> {
        LamePdClient::get_cluster_id(self)
    }

    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()> {
        LamePdClient::bootstrap_cluster(self, stores, region)
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        LamePdClient::is_cluster_bootstrapped(self)
    }

    fn alloc_id(&self) -> Result<u64> {
        LamePdClient::alloc_id(self)
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        LamePdClient::put_store(self, store)
    }

    fn get_store(&self, store_id: u64) -> PdFuture<metapb::Store> {
        LamePdClient::get_store(self, store_id)
    }

    fn get_all_stores(&self) -> Result<Vec<metapb::Store>> {
        LamePdClient::get_all_stores(self)
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        LamePdClient::get_cluster_config(self)
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        LamePdClient::get_region(self, key)
    }

    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        LamePdClient::get_region_info(self, key)
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        LamePdClient::get_region_by_id(self, region_id)
    }

    fn region_heartbeat(
        &self,
        region: metapb::Region,
        peer: metapb::Peer,
        stat: RegionStat,
    ) -> PdFuture<()> {
        LamePdClient::region_heartbeat(self, region, peer, stat)
    }

    fn handle_region_heartbeat_response<F>(&self, store_id: u64, f: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        LamePdClient::handle_region_heartbeat_response(self, store_id, f)
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        LamePdClient::ask_split(self, region)
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        LamePdClient::store_heartbeat(self, stats)
    }

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()> {
        LamePdClient::report_split(self, left, right)
    }

    fn scatter_region(&self, info: RegionInfo) -> Result<()> {
        LamePdClient::scatter_region(self, info)
    }

    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, f: F) {
        LamePdClient::handle_reconnect(self, f)
    }
}
