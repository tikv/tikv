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

use protobuf::RepeatedField;
use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc;
use grpc::{CallOption, EnvBuilder, WriteFlags};
use kvproto::metapb;
use kvproto::pdpb::{self, Member};

use util::{Either, HandyRwLock};
use util::time::duration_to_sec;
use pd::PdFuture;
use super::{Error, PdClient, RegionStat, Result, REQUEST_TIMEOUT};
use super::util::{check_resp_header, sync_request, validate_endpoints, Inner, LeaderClient};
use super::metrics::*;

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &'static str = "pd";

pub struct RpcClient {
    cluster_id: u64,
    leader_client: LeaderClient,
}

impl RpcClient {
    pub fn new(endpoints: &[String]) -> Result<RpcClient> {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(CQ_COUNT)
                .name_prefix(thd_name!(CLIENT_PREFIX))
                .build(),
        );
        let (client, members) = validate_endpoints(env.clone(), endpoints)?;

        Ok(RpcClient {
            cluster_id: members.get_header().get_cluster_id(),
            leader_client: LeaderClient::new(env, client, members),
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
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.bootstrap_opt(req.clone(), option)
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
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.is_bootstrapped_opt(req.clone(), option)
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
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.alloc_id_opt(req.clone(), option)
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
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.put_store_opt(req.clone(), option)
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
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.get_store_opt(req.clone(), option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_store())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_cluster_config"])
            .start_coarse_timer();

        let mut req = pdpb::GetClusterConfigRequest::new();
        req.set_header(self.header());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.get_cluster_config_opt(req.clone(), option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_region"])
            .start_coarse_timer();

        let mut req = pdpb::GetRegionRequest::new();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            client.get_region_opt(req.clone(), option)
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_region())
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = move |client: &RwLock<Inner>, req: pdpb::GetRegionByIDRequest| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            let handler = client.rl().client.get_region_by_id_async_opt(req, option);
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

        let executor = |client: &RwLock<Inner>, req: pdpb::RegionHeartbeatRequest| {
            let mut inner = client.wl();
            let sender = match inner.hb_sender {
                Either::Left(ref mut sender) => sender.take(),
                Either::Right(ref sender) => {
                    return Box::new(future::result(
                        sender
                            .unbounded_send(req)
                            .map_err(|e| Error::Other(Box::new(e))),
                    )) as PdFuture<_>
                }
            };

            match sender {
                Some(sender) => {
                    let (tx, rx) = mpsc::unbounded();
                    tx.unbounded_send(req).unwrap();
                    inner.hb_sender = Either::Right(tx);
                    Box::new(
                        sender
                            .sink_map_err(Error::Grpc)
                            .send_all(
                                rx.map_err(|e| {
                                    Error::Other(box_err!("failed to recv heartbeat: {:?}", e))
                                }).map(|r| (r, WriteFlags::default())),
                            )
                            .map(|_| ()),
                    ) as PdFuture<_>
                }
                None => unreachable!(),
            }
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
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            let handler = client.rl().client.ask_split_async_opt(req, option);
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

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_stats(stats);

        let executor = move |client: &RwLock<Inner>, req: pdpb::StoreHeartbeatRequest| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            let handler = client.rl().client.store_heartbeat_async_opt(req, option);
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

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        let executor = move |client: &RwLock<Inner>, req: pdpb::ReportSplitRequest| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            let handler = client.rl().client.report_split_async_opt(req, option);
            Box::new(handler.map_err(Error::Grpc).and_then(move |resp| {
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["report_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            })) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }
}
