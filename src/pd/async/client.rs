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

use protobuf::RepeatedField;
use futures::Future;

use kvproto::metapb;
use kvproto::pdpb::{self, Member};
use kvproto::pdpb_grpc::PDAsync;

use super::super::PdFuture;
use super::super::{Result, Error, PdClient};
use super::util::LeaderClient;
use super::util::Request;
use super::util::validate_endpoints;

// TODO: revoke pubs.
pub struct RpcClient {
    pub cluster_id: u64,
    pub inner: LeaderClient,
}

impl RpcClient {
    pub fn new(endpoints: &str) -> Result<RpcClient> {
        let endpoints: Vec<_> = endpoints.split(',')
            .map(|s| if !s.starts_with("http://") {
                format!("http://{}", s)
            } else {
                s.to_owned()
            })
            .collect();

        let (client, members) = try!(validate_endpoints(&endpoints));

        Ok(RpcClient {
            cluster_id: members.get_header().get_cluster_id(),
            inner: LeaderClient::new(client, members),
        })
    }

    pub fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::new();
        header.set_cluster_id(self.cluster_id);
        header
    }

    // For tests
    pub fn get_leader(&self) -> Member {
        self.inner.clone_members().take_leader()
    }
}

fn check_resp_header(header: &pdpb::ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    // TODO: translate more error types
    let err = header.get_error();
    Err(box_err!(err.get_message()))
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "PD gRPC Client connects to cluster {:?}",
               self.cluster_id)
    }
}

const LEADER_CHANGE_RETRY: usize = 10;
const REQUEST_RETRY: usize = 10;

impl PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()> {
        let mut req = pdpb::BootstrapRequest::new();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.Bootstrap(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let mut req = pdpb::IsBootstrappedRequest::new();
        req.set_header(self.header());

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.IsBootstrapped(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp.get_bootstrapped())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    fn alloc_id(&self) -> Result<u64> {
        let mut req = pdpb::AllocIDRequest::new();
        req.set_header(self.header());

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.AllocID(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp.get_id())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let mut req = pdpb::PutStoreRequest::new();
        req.set_header(self.header());
        req.set_store(store);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.PutStore(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let mut req = pdpb::GetStoreRequest::new();
        req.set_header(self.header());
        req.set_store_id(store_id);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.GetStore(req)
                        .map_err(Error::Grpc)
                        .and_then(|mut resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp.take_store())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let mut req = pdpb::GetClusterConfigRequest::new();
        req.set_header(self.header());

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.GetClusterConfig(req)
                        .map_err(Error::Grpc)
                        .and_then(|mut resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp.take_cluster())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let mut req = pdpb::GetRegionRequest::new();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.GetRegion(req)
                        .map_err(Error::Grpc)
                        .and_then(|mut resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp.take_region())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
            .wait()
    }

    // Get region by region id.
    fn get_region_by_id_async(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let mut req = pdpb::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.GetRegionByID(req)
                        .map_err(Error::Grpc)
                        .and_then(|mut resp| {
                            try!(check_resp_header(resp.get_header()));
                            if resp.has_region() {
                                Ok(Some(resp.take_region()))
                            } else {
                                Ok(None)
                            }
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
    }

    // Leader for a region will use this to heartbeat Pd.
    fn region_heartbeat_async(&self,
                              region: metapb::Region,
                              leader: metapb::Peer,
                              down_peers: Vec<pdpb::PeerStats>,
                              pending_peers: Vec<metapb::Peer>,
                              written_bytes: u64)
                              -> PdFuture<pdpb::RegionHeartbeatResponse> {
        let mut req = pdpb::RegionHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(RepeatedField::from_vec(down_peers));
        req.set_pending_peers(RepeatedField::from_vec(pending_peers));
        req.set_bytes_written(written_bytes);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.RegionHeartbeat(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp)
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
    }

    // Ask pd for split, pd will returns the new split region id.
    fn ask_split_async(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let mut req = pdpb::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.AskSplit(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(resp)
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
    }

    // Send store statistics regularly.
    fn store_heartbeat_async(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        let mut req = pdpb::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_stats(stats);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.StoreHeartbeat(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
    }

    // Report pd the split region.
    fn report_split_async(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()> {
        let mut req = pdpb::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        self.inner
            .client(LEADER_CHANGE_RETRY, req, |client, req| {
                let retry_req = Request::new(REQUEST_RETRY, client, req, |client, req| {
                    client.ReportSplit(req)
                        .map_err(Error::Grpc)
                        .and_then(|resp| {
                            try!(check_resp_header(resp.get_header()));
                            Ok(())
                        })
                        .boxed()
                });

                retry_req.retry()
            })
            .reconnect()
    }
}
