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

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::TryLockError;
use std::time::Instant;
use std::time::Duration;
use std::thread;

use futures::Poll;
use futures::Async;
use futures::Future;
use futures::future::ok;
use futures::future::{self, loop_fn, Loop};

use kvproto::pdpb::GetMembersResponse;
use kvproto::pdpb_grpc::PDAsyncClient;

use util::HandyRwLock;

use super::super::PdFuture;
use super::super::Result;
use super::super::Error;
use super::client::try_connect_leader;

struct Bundle {
    client: Arc<PDAsyncClient>,
    members: GetMembersResponse,
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    inner: Arc<RwLock<Bundle>>,
}

impl LeaderClient {
    pub fn new(client: PDAsyncClient, members: GetMembersResponse) -> LeaderClient {
        LeaderClient {
            inner: Arc::new(RwLock::new(Bundle {
                client: Arc::new(client),
                members: members,
            })),
        }
    }

    pub fn client<Req, Resp, F>(&self, req: Req, f: F, retry: usize) -> Request<Req, Resp, F>
        where Req: Clone + 'static,
              F: FnMut(&PDAsyncClient, Req) -> PdFuture<Resp> + Send + 'static
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            bundle: self.inner.clone(),
            req: req,
            resp: None,
            client: None,
            func: f,
        }
    }

    pub fn get_client(&self) -> Arc<PDAsyncClient> {
        self.inner.rl().client.clone()
    }

    pub fn set_client(&self, client: PDAsyncClient) {
        let mut bundle = self.inner.wl();
        bundle.client = Arc::new(client);
    }

    pub fn get_members(&self) -> GetMembersResponse {
        self.inner.rl().members.clone()
    }

    pub fn set_members(&self, members: GetMembersResponse) {
        let mut inner = self.inner.wl();
        inner.members = members;
    }
}

struct BundleRead {
    inner: Option<Arc<RwLock<Bundle>>>,
}

impl Future for BundleRead {
    type Item = Arc<PDAsyncClient>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = self.inner.take().expect("BundleRead cannot poll twice");
        let read = inner.try_read();
        match read {
            Ok(bundle) => Ok(Async::Ready(bundle.client.clone())),
            Err(TryLockError::WouldBlock) => Ok(Async::NotReady),
            // TODO: handle `PoisonError`.
            Err(err) => panic!("{:?}", err),
        }
    }
}

/// The context of sending requets.
pub struct Request<Req, Resp, F> {
    reconnect_count: usize,
    request_sent: usize,

    bundle: Arc<RwLock<Bundle>>,

    req: Req,
    resp: Option<Result<Resp>>,
    client: Option<Arc<PDAsyncClient>>,
    func: F,
}

const RECONNECT_THD: usize = 5;

impl<Req, Resp, F> Request<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(&PDAsyncClient, Req) -> PdFuture<Resp> + Send + 'static
{
    fn reconnect_if_needed(mut self) -> PdFuture<Self> {
        debug!("reconnect remains: {}", self.reconnect_count);

        if self.request_sent == 0 {
            let bundle = BundleRead { inner: Some(self.bundle.clone()) };
            return bundle.map(|client| {
                    self.client = Some(client);
                    ok(self)
                })
                .flatten()
                .boxed();
        }

        if self.request_sent < RECONNECT_THD {
            debug!("retry on the same client");
            return ok(self).boxed();
        }

        // Updating client.

        self.request_sent = 0;
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        warn!("updating PD client, block the tokio core");

        let start = Instant::now();
        // Go to sync world.
        let members = self.bundle.rl().members.clone();
        match try_connect_leader(&members) {
            Ok((client, members)) => {
                let mut bundle = self.bundle.wl();
                if members != bundle.members {
                    bundle.client = Arc::new(client);
                    bundle.members = members;
                }
                warn!("updating PD client done, spent {:?}", start.elapsed());
            }

            Err(err) => {
                warn!("updating PD client spent {:?}, err {:?}",
                      start.elapsed(),
                      err);
                // FIXME: use tokio-timer instead.
                thread::sleep(Duration::from_secs(1));
            }
        }

        ok(self).boxed()
    }

    fn send(mut self) -> PdFuture<Self> {
        self.request_sent += 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();
        let req = (self.func)(self.client.as_ref().unwrap(), r);
        req.then(|resp| {
                match resp {
                    Ok(resp) => self.resp = Some(Ok(resp)),
                    Err(err) => {
                        error!("request failed: {:?}", err);
                    }
                };
                ok(self)
            })
            .boxed()
    }

    fn receive(self) -> PdFuture<(Self, bool)> {
        let done = self.reconnect_count == 0 || self.resp.is_some();
        ok((self, done)).boxed()
    }

    fn get_resp(self) -> Option<Result<Resp>> {
        self.resp
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let ctx = self;
        loop_fn(ctx, |ctx| {
                ctx.reconnect_if_needed()
                    .and_then(|ctx| ctx.send())
                    .and_then(|ctx| ctx.receive())
                    .and_then(|(ctx, done)| {
                        if done {
                            Ok(Loop::Break(ctx))
                        } else {
                            Ok(Loop::Continue(ctx))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("Request fail to request")),
                }
            })
            .boxed()
    }
}
