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

use std::io;
use std::fmt;
use std::thread;
use std::sync::RwLock;
use std::sync::mpsc::channel as std_channel;

use protobuf::RepeatedField;

use futures;
use futures::Future;
use futures::sync::mpsc::{unbounded, UnboundedSender};
use futures::Stream;

use tokio_core::reactor::Core;
use tokio_core::reactor::Remote;

use kvproto::metapb;
use kvproto::pdpb::{self, GetMembersResponse, Member};
use kvproto::pdpb_grpc::{PDAsync, PDAsyncClient};

use super::super::{Result, Error, PdFuture};
use super::super::AsyncPdClient;
use super::validate_endpoints;

// TODO: revoke pubs.
pub struct Inner {
    pub members: GetMembersResponse,
    pub client: PDAsyncClient,
}

// TODO: revoke pubs.
pub struct RpcAsyncClient {
    pub cluster_id: u64,
    pub inner: RwLock<Inner>,
    shutdown_tx: UnboundedSender<()>,
    remote: Remote,
}

impl RpcAsyncClient {
    pub fn new(endpoints: &str) -> Result<RpcAsyncClient> {
        let endpoints: Vec<_> = endpoints.split(',')
            .map(|s| if !s.starts_with("http://") {
                format!("http://{}", s)
            } else {
                s.to_owned()
            })
            .collect();

        let (client, members) = try!(validate_endpoints(&endpoints));

        let (tx, rx) = std_channel();

        thread::Builder::new().name("PdClient".to_owned())
            .spawn(move || {
                let mut core = match Core::new() {
                    Ok(core) => core,
                    Err(err) => {
                        tx.send(Err(err)).ok();
                        return;
                    }
                };

                let handle = core.remote();
                let (shutdown_tx, shutdown_rx) = unbounded::<()>();
                let shutdown = shutdown_rx.into_future()
                    .map_err(|((), _)| Error::Io(io::Error::new(io::ErrorKind::Other, "shutdown")))
                    .and_then(move |_| {
                        debug!("client shutdown");
                        futures::failed::<(), _>(Error::Io(io::Error::new(io::ErrorKind::Other,
                                                                          "shutdown")))
                    });

                tx.send(Ok((shutdown_tx, handle))).ok();
                core.run(shutdown).ok();
            })?;

        let (shutdown_tx, remote) = try!(rx.recv().unwrap());

        Ok(RpcAsyncClient {
            cluster_id: members.get_header().get_cluster_id(),
            inner: RwLock::new(Inner {
                members: members,
                client: client,
            }),
            shutdown_tx: shutdown_tx,
            remote: remote,
        })
    }

    pub fn spawn<F>(&self, f: F)
        where F: Future<Item = (), Error = ()> + Send + 'static
    {
        self.remote.spawn(|h| {
            h.spawn(f);
            Ok(())
        })
    }

    pub fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::new();
        header.set_cluster_id(self.cluster_id);
        header
    }

    // For tests
    pub fn get_leader(&self) -> Member {
        let inner = self.inner.read().unwrap();
        inner.members.get_leader().clone()
    }
}

impl Drop for RpcAsyncClient {
    fn drop(&mut self) {
        self.shutdown_tx.send(()).unwrap();
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

impl fmt::Debug for RpcAsyncClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "PD gRPC Client connects to cluster {:?}",
               self.cluster_id)
    }
}

// TODO: retry...
impl AsyncPdClient for RpcAsyncClient {
    // Get region by region id.
    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let mut req = pdpb::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let inner = self.inner.read().unwrap();
        inner.client
            .GetRegionByID(req)
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
    }

    // Leader for a region will use this to heartbeat Pd.
    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> PdFuture<pdpb::RegionHeartbeatResponse> {
        let mut req = pdpb::RegionHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(RepeatedField::from_vec(down_peers));
        req.set_pending_peers(RepeatedField::from_vec(pending_peers));

        let inner = self.inner.read().unwrap();
        inner.client
            .RegionHeartbeat(req)
            .map_err(Error::Grpc)
            .and_then(|resp| {
                try!(check_resp_header(resp.get_header()));
                Ok(resp)
            })
            .boxed()
    }

    // Ask pd for split, pd will returns the new split region id.
    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let mut req = pdpb::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        let inner = self.inner.read().unwrap();
        inner.client
            .AskSplit(req)
            .map_err(Error::Grpc)
            .and_then(|resp| {
                try!(check_resp_header(resp.get_header()));
                Ok(resp)
            })
            .boxed()
    }

    // Send store statistics regularly.
    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        let mut req = pdpb::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_stats(stats);

        let inner = self.inner.read().unwrap();
        inner.client
            .StoreHeartbeat(req)
            .map_err(Error::Grpc)
            .and_then(|resp| {
                try!(check_resp_header(resp.get_header()));
                Ok(())
            })
            .boxed()
    }

    // Report pd the split region.
    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()> {
        let mut req = pdpb::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        let inner = self.inner.read().unwrap();
        inner.client
            .ReportSplit(req)
            .map_err(Error::Grpc)
            .and_then(|resp| {
                try!(check_resp_header(resp.get_header()));
                Ok(())
            })
            .boxed()
    }

    fn resolve(&self, future: Box<Future<Item = (), Error = ()> + Send + 'static>) {
        self.spawn(future);
    }
}
