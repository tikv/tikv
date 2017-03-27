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
use std::thread;
use std::sync::RwLock;
use std::sync::mpsc::channel as std_channel;

use protobuf::RepeatedField;

use futures::future::{self, loop_fn, Loop};
use futures::Future;
use futures::sync::oneshot::{channel, Sender};

use tokio_core::reactor::Core;
use tokio_core::reactor::Remote;

use kvproto::metapb;
use kvproto::pdpb::{self, Member};
use kvproto::pdpb_grpc::{PDAsync, PDAsyncClient};

use super::super::{Result, Error, PdFuture};
use super::super::AsyncPdClient;
use super::validate_endpoints;

use super::util::LeaderClient;
use super::util::Request;

// TODO: revoke pubs.
pub struct RpcAsyncClient {
    pub cluster_id: u64,
    pub inner: RwLock<LeaderClient<PDAsyncClient>>,
    remote: Remote,
    _shutdown: Sender<()>,
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

        // TODO: move it out.
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
                let (shutdown_tx, shutdown_rx) = channel::<()>();

                // The shutdown future will be resolved once the Sender is been dropped.
                // and this thread returns.
                let shutdown = shutdown_rx.then(|_| {
                    debug!("PD client shutdown");
                    future::ok::<(), ()>(())
                });

                tx.send(Ok((shutdown_tx, handle))).ok();

                core.run(shutdown).ok();
            })?;

        let (shutdown_tx, remote) = try!(rx.recv().unwrap());

        Ok(RpcAsyncClient {
            cluster_id: members.get_header().get_cluster_id(),
            inner: RwLock::new(LeaderClient::new(client, members)),
            remote: remote,
            _shutdown: shutdown_tx,
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
        inner.get_members().get_leader().clone()
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

// TODO: Retry
// TODO: Leader Change
impl AsyncPdClient for RpcAsyncClient {
    // Get region by region id.
    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let mut req = pdpb::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let client = self.inner.read().unwrap().clone_client();
        let retry_req = Request::new(10, client, move |client| {
            client.GetRegionByID(req.clone())
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

        loop_fn(retry_req, |retry_req| {
                retry_req.send()
                    .and_then(|retry_req| retry_req.receive())
                    .and_then(|(retry_req, done)| {
                        if done {
                            Ok(Loop::Break(retry_req))
                        } else {
                            Ok(Loop::Continue(retry_req))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("fail to request")),
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

        let client = self.inner.read().unwrap().clone_client();
        let retry_req = Request::new(10, client, move |client| {
            client.RegionHeartbeat(req.clone())
                .map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(resp)
                })
                .boxed()
        });

        loop_fn(retry_req, |retry_req| {
                retry_req.send()
                    .and_then(|retry_req| retry_req.receive())
                    .and_then(|(retry_req, done)| {
                        if done {
                            Ok(Loop::Break(retry_req))
                        } else {
                            Ok(Loop::Continue(retry_req))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("fail to request")),
                }
            })
            .boxed()
    }

    // Ask pd for split, pd will returns the new split region id.
    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let mut req = pdpb::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        let client = self.inner.read().unwrap().clone_client();
        let retry_req = Request::new(10, client, move |client| {
            client.AskSplit(req.clone())
                .map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(resp)
                })
                .boxed()
        });

        loop_fn(retry_req, |retry_req| {
                retry_req.send()
                    .and_then(|retry_req| retry_req.receive())
                    .and_then(|(retry_req, done)| {
                        if done {
                            Ok(Loop::Break(retry_req))
                        } else {
                            Ok(Loop::Continue(retry_req))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("fail to request")),
                }
            })
            .boxed()
    }

    // Send store statistics regularly.
    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        let mut req = pdpb::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_stats(stats);

        let client = self.inner.read().unwrap().clone_client();
        let retry_req = Request::new(10, client, move |client| {
            client.StoreHeartbeat(req.clone())
                .map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(())
                })
                .boxed()
        });

        loop_fn(retry_req, |retry_req| {
                retry_req.send()
                    .and_then(|retry_req| retry_req.receive())
                    .and_then(|(retry_req, done)| {
                        if done {
                            Ok(Loop::Break(retry_req))
                        } else {
                            Ok(Loop::Continue(retry_req))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("fail to request")),
                }
            })
            .boxed()
    }

    // Report pd the split region.
    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()> {
        let mut req = pdpb::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        let client = self.inner.read().unwrap().clone_client();
        let retry_req = Request::new(10, client, move |client| {
            client.ReportSplit(req.clone())
                .map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(())
                })
                .boxed()
        });

        loop_fn(retry_req, |retry_req| {
                retry_req.send()
                    .and_then(|retry_req| retry_req.receive())
                    .and_then(|(retry_req, done)| {
                        if done {
                            Ok(Loop::Break(retry_req))
                        } else {
                            Ok(Loop::Continue(retry_req))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("fail to request")),
                }
            })
            .boxed()
    }

    fn resolve(&self, future: Box<Future<Item = (), Error = ()> + Send + 'static>) {
        self.spawn(future);
    }
}
