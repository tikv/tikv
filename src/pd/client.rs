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

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::sync::mpsc;
use futures::{future, Future, Sink, Stream};
use grpc::{CallOption, EnvBuilder, WriteFlags};
use kvproto::metapb;
use kvproto::pdpb::{self, Member};
use protobuf::RepeatedField;

use super::metrics::*;
use super::util::{check_resp_header, sync_request, validate_endpoints, Inner, LeaderClient};
use super::{Error, PdClient, RegionInfo, RegionStat, Result, REQUEST_TIMEOUT};
use pd::{Config, PdFuture};
use util::security::SecurityManager;
use util::time::{duration_to_sec, time_now_sec};
use util::{Either, HandyRwLock};

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "pd";

pub struct RpcClient {
    cluster_id: u64,
    leader_client: LeaderClient,
}

impl RpcClient {
    pub fn new(cfg: &Config, security_mgr: Arc<SecurityManager>) -> Result<RpcClient> {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(CQ_COUNT)
                .name_prefix(thd_name!(CLIENT_PREFIX))
                .build(),
        );
        let (client, members) = validate_endpoints(Arc::clone(&env), cfg, &security_mgr)?;

        Ok(RpcClient {
            cluster_id: members.get_header().get_cluster_id(),
            leader_client: LeaderClient::new(env, security_mgr, client, members),
        })
    }

    fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::new();
        header.set_cluster_id(self.cluster_id);
        header
    }

    pub fn get_leader(&self) -> Member {
        self.leader_client.get_leader()
    }

    #[inline]
    fn call_option() -> CallOption {
        CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT))
    }

    fn get_region_and_leader(&self, key: &[u8]) -> Result<(metapb::Region, Option<metapb::Peer>)> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_region"])
            .start_coarse_timer();

        let mut req = pdpb::GetRegionRequest::new();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_region_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        let region = if resp.has_region() {
            resp.take_region()
        } else {
            return Err(Error::RegionNotFound(key.to_owned()));
        };
        let leader = if resp.has_leader() {
            Some(resp.take_leader())
        } else {
            None
        };
        Ok((region, leader))
    }
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RpcClient")
            .field("cluster_id", &self.cluster_id)
            .field("leader", &self.get_leader())
            .finish()
    }
}

const LEADER_CHANGE_RETRY: usize = 10;

impl PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["bootstrap_cluster"])
            .start_coarse_timer();

        let mut req = pdpb::BootstrapRequest::new();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.bootstrap_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;
        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["is_cluster_bootstrapped"])
            .start_coarse_timer();

        let mut req = pdpb::IsBootstrappedRequest::new();
        req.set_header(self.header());

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.is_bootstrapped_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["alloc_id"])
            .start_coarse_timer();

        let mut req = pdpb::AllocIDRequest::new();
        req.set_header(self.header());

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.alloc_id_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["put_store"])
            .start_coarse_timer();

        let mut req = pdpb::PutStoreRequest::new();
        req.set_header(self.header());
        req.set_store(store);

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.put_store_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_store"])
            .start_coarse_timer();

        let mut req = pdpb::GetStoreRequest::new();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_store_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        let store = resp.take_store();
        if store.get_state() != metapb::StoreState::Tombstone {
            Ok(store)
        } else {
            Err(Error::StoreTombstone(format!("{:?}", store)))
        }
    }

    fn get_all_stores(&self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_all_stores"])
            .start_coarse_timer();

        let mut req = pdpb::GetAllStoresRequest::new();
        req.set_header(self.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_all_stores_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_stores().into_vec())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_cluster_config"])
            .start_coarse_timer();

        let mut req = pdpb::GetClusterConfigRequest::new();
        req.set_header(self.header());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_cluster_config_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        self.get_region_and_leader(key).map(|x| x.0)
    }

    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        self.get_region_and_leader(key)
            .map(|x| RegionInfo::new(x.0, x.1))
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = move |client: &RwLock<Inner>, req: pdpb::GetRegionByIDRequest| {
            let handler = client
                .rl()
                .client
                .get_region_by_id_async_opt(&req, Self::call_option())
                .unwrap();
            Box::new(handler.map_err(Error::Grpc).and_then(move |mut resp| {
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_by_id"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() {
                    Ok(Some(resp.take_region()))
                } else {
                    Ok(None)
                }
            })) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn region_heartbeat(
        &self,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
    ) -> PdFuture<()> {
        PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["send"]).inc();

        let mut req = pdpb::RegionHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(RepeatedField::from_vec(region_stat.down_peers));
        req.set_pending_peers(RepeatedField::from_vec(region_stat.pending_peers));
        req.set_bytes_written(region_stat.written_bytes);
        req.set_keys_written(region_stat.written_keys);
        req.set_bytes_read(region_stat.read_bytes);
        req.set_keys_read(region_stat.read_bytes);
        req.set_approximate_size(region_stat.approximate_size);
        req.set_approximate_keys(region_stat.approximate_keys);
        let mut interval = pdpb::TimeInterval::new();
        interval.set_start_timestamp(region_stat.last_report_ts);
        interval.set_end_timestamp(time_now_sec());
        req.set_interval(interval);

        let executor = |client: &RwLock<Inner>, req: pdpb::RegionHeartbeatRequest| {
            let mut inner = client.wl();
            if let Either::Right(ref sender) = inner.hb_sender {
                return Box::new(future::result(
                    sender
                        .unbounded_send(req)
                        .map_err(|e| Error::Other(Box::new(e))),
                )) as PdFuture<_>;
            }

            info!("heartbeat sender is refreshed.");
            let sender = inner.hb_sender.as_mut().left().unwrap().take().unwrap();
            let (tx, rx) = mpsc::unbounded();
            tx.unbounded_send(req).unwrap();
            inner.hb_sender = Either::Right(tx);
            Box::new(
                sender
                    .sink_map_err(Error::Grpc)
                    .send_all(rx.then(|r| match r {
                        Ok(r) => Ok((r, WriteFlags::default())),
                        Err(()) => Err(Error::Other(box_err!("failed to recv heartbeat"))),
                    }))
                    .then(|result| match result {
                        Ok((mut sender, _)) => {
                            info!("cancel region heartbeat sender");
                            sender.get_mut().cancel();
                            Ok(())
                        }
                        Err(e) => {
                            error!("failed to send heartbeat: {:?}", e);
                            Err(e)
                        }
                    }),
            ) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn handle_region_heartbeat_response<F>(&self, _: u64, f: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        self.leader_client.handle_region_heartbeat_response(f)
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        let executor = move |client: &RwLock<Inner>, req: pdpb::AskSplitRequest| {
            let handler = client
                .rl()
                .client
                .ask_split_async_opt(&req, Self::call_option())
                .unwrap();
            Box::new(handler.map_err(Error::Grpc).and_then(move |resp| {
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            })) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn ask_batch_split(
        &self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskBatchSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);
        req.set_split_count(count as u32);

        let executor = move |client: &RwLock<Inner>, req: pdpb::AskBatchSplitRequest| {
            let handler = client
                .rl()
                .client
                .ask_batch_split_async_opt(&req, Self::call_option())
                .unwrap();
            Box::new(handler.map_err(Error::Grpc).and_then(move |resp| {
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_batch_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            })) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn store_heartbeat(&self, mut stats: pdpb::StoreStats) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        stats.mut_interval().set_end_timestamp(time_now_sec());
        req.set_stats(stats);
        let executor = move |client: &RwLock<Inner>, req: pdpb::StoreHeartbeatRequest| {
            let handler = client
                .rl()
                .client
                .store_heartbeat_async_opt(&req, Self::call_option())
                .unwrap();
            Box::new(handler.map_err(Error::Grpc).and_then(move |resp| {
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["store_heartbeat"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            })) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportBatchSplitRequest::new();
        req.set_header(self.header());
        req.set_regions(RepeatedField::from_vec(regions));

        let executor = move |client: &RwLock<Inner>, req: pdpb::ReportBatchSplitRequest| {
            let handler = client
                .rl()
                .client
                .report_batch_split_async_opt(&req, Self::call_option())
                .unwrap();
            Box::new(handler.map_err(Error::Grpc).and_then(move |resp| {
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["report_batch_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            })) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn scatter_region(&self, mut region: RegionInfo) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["scatter_region"])
            .start_coarse_timer();

        let mut req = pdpb::ScatterRegionRequest::new();
        req.set_header(self.header());
        req.set_region_id(region.get_id());
        if let Some(leader) = region.leader.take() {
            req.set_leader(leader);
        }
        req.set_region(region.region);

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.scatter_region_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())
    }

    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, f: F) {
        self.leader_client.on_reconnect(Box::new(f))
    }
}
