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
use std::time::Instant;
use std::time::Duration;
use std::thread;

use futures::Future;
use futures::future::ok;
use futures::future::{self, loop_fn, Loop};

use kvproto::pdpb::GetMembersResponse;
use kvproto::pdpb_grpc::PDAsyncClient;

use util::HandyRwLock;

use super::super::PdFuture;
use super::super::Result;
use super::client::try_connect_leader;

pub struct Inner {
    pub client: PDAsyncClient,
    pub members: GetMembersResponse,
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    pub inner: Arc<RwLock<Inner>>,
}

impl LeaderClient {
    pub fn new(client: PDAsyncClient, members: GetMembersResponse) -> LeaderClient {
        LeaderClient {
            inner: Arc::new(RwLock::new(Inner {
                client: client,
                members: members,
            })),
        }
    }

    pub fn request<Req, Resp, F>(&self, req: Req, f: F, retry: usize) -> Request<Req, Resp, F>
        where Req: Clone + 'static,
              F: FnMut(&PDAsyncClient, Req) -> PdFuture<Resp> + Send + 'static
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            inner: self.inner.clone(),
            req: req,
            resp: None,
            func: f,
        }
    }
}

/// The context of sending requets.
pub struct Request<Req, Resp, F> {
    reconnect_count: usize,
    request_sent: usize,

    inner: Arc<RwLock<Inner>>,

    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 5;

impl<Req, Resp, F> Request<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(&PDAsyncClient, Req) -> PdFuture<Resp> + Send + 'static
{
    // Re-establish connection with PD leader in synchronized fashion.
    fn reconnect_if_needed(mut self) -> PdFuture<Self> {
        debug!("reconnect remains: {}", self.reconnect_count);

        if self.request_sent == 0 {
            return ok(self).boxed();
        }

        if self.request_sent < MAX_REQUEST_COUNT {
            debug!("retry on the same client");
            return ok(self).boxed();
        }

        // Updating client.

        self.request_sent = 0;
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        warn!("updating PD client, block the tokio core");

        let start = Instant::now();
        match try_connect_leader(&self.inner.rl().members) {
            Ok((client, members)) => {
                let mut inner = self.inner.wl();
                inner.client = client;
                inner.members = members;
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
        let req = (self.func)(&self.inner.rl().client, r);
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

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let ctx = self;
        loop_fn(ctx, |ctx| {
                ctx.reconnect_if_needed()
                    .and_then(|ctx| ctx.send())
                    .and_then(|ctx| {
                        let done = ctx.reconnect_count == 0 || ctx.resp.is_some();
                        if done {
                            Ok(Loop::Break(ctx))
                        } else {
                            Ok(Loop::Continue(ctx))
                        }
                    })
            })
            .then(|ctx| {
                let ctx = ctx.expect("end loop with Ok(_)");
                match ctx.resp {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("fail to request")),
                }
            })
            .boxed()
    }
}
