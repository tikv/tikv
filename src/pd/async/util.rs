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
use futures::future::{loop_fn, Loop};

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
    fn reconnect_if_needed(self) -> PdFuture<Self> {
        let mut ctx = self;
        debug!("reconnect remains: {}", ctx.reconnect_count);

        if ctx.request_sent == 0 {
            return ok(ctx).boxed();
        }

        if ctx.request_sent < MAX_REQUEST_COUNT {
            debug!("retry on the same client");
            return ok(ctx).boxed();
        }

        // Updating client.

        ctx.request_sent = 0;
        ctx.reconnect_count -= 1;

        // FIXME: should not block the core.
        warn!("updating PD client, block the tokio core");

        let start = Instant::now();
        let ret = try_connect_leader(&ctx.inner.rl().members.clone());
        match ret {
            Ok((client, members)) => {
                let mut inner = ctx.inner.wl();
                if members != inner.members {
                    inner.client = client;
                    inner.members = members;
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

        ok(ctx).boxed()
    }

    fn send(self) -> PdFuture<Self> {
        let mut ctx = self;
        ctx.request_sent += 1;
        debug!("request sent: {}", ctx.request_sent);
        let r = ctx.req.clone();
        let req = (ctx.func)(&ctx.inner.rl().client, r);
        req.then(|resp| {
                match resp {
                    Ok(resp) => ctx.resp = Some(Ok(resp)),
                    Err(err) => {
                        error!("request failed: {:?}", err);
                    }
                };
                Ok(ctx)
            })
            .boxed()
    }

    fn receive(self) -> Result<Loop<Self, Self>> {
        let ctx = self;
        let done = ctx.reconnect_count == 0 || ctx.resp.is_some();
        if done {
            Ok(Loop::Break(ctx))
        } else {
            Ok(Loop::Continue(ctx))
        }
    }

    fn post_loop(ctx: Result<Self>) -> Result<Resp> {
        let ctx = ctx.expect("end loop with Ok(_)");
        ctx.resp.unwrap_or_else(|| Err(box_err!("fail to request")))
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let ctx = self;
        loop_fn(ctx, |ctx| {
                ctx.reconnect_if_needed()
                    .and_then(Self::send)
                    .and_then(Self::receive)
            })
            .then(Self::post_loop)
            .boxed()
    }
}
