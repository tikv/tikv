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
use kvproto::pdpb_grpc::PDAsync;
use kvproto::pdpb_grpc::PDAsyncClient;

use util::HandyRwLock;

use super::sync;
use super::super::PdFuture;
use super::super::Result;
use super::super::Error;

#[derive(Debug)]
struct Bundle<C> {
    client: Arc<C>,
    members: GetMembersResponse,
}

pub struct LeaderClient {
    inner: Arc<RwLock<Bundle<PDAsyncClient>>>,
}

impl LeaderClient {
    pub fn new(client: PDAsyncClient, members: GetMembersResponse) -> LeaderClient {
        LeaderClient {
            inner: Arc::new(RwLock::new(Bundle {
                client: Arc::new(client),
                members: members.clone(),
            })),
        }
    }

    pub fn client<Req, Resp, G>(&self, retry: usize, req: Req, f: G) -> GetClient<Req, Resp, G>
        where Req: Clone + 'static,
              G: FnMut(Arc<PDAsyncClient>, Req) -> PdFuture<Resp> + Send + 'static
    {
        GetClient {
            need_update: false,
            retry_count: retry,
            bundle: self.inner.clone(),
            req: req,
            resp: None,
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

    pub fn clone_client(&self) -> Arc<PDAsyncClient> {
        self.inner.rl().client.clone()
    }

    pub fn clone_members(&self) -> GetMembersResponse {
        self.inner.rl().members.clone()
    }

    pub fn set_members(&self, members: GetMembersResponse) {
        let mut inner = self.inner.wl();
        inner.members = members;
    }
}

pub struct GetClient<Req, Resp, F> {
    need_update: bool,
    retry_count: usize,
    bundle: Arc<RwLock<Bundle<PDAsyncClient>>>,
    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

impl<Req, Resp, F> GetClient<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(Arc<PDAsyncClient>, Req) -> PdFuture<Resp> + Send + 'static
{
    fn get(self) -> PdFuture<GetClient<Req, Resp, F>> {
        debug!("GetLeader get remains: {}", self.retry_count);

        let get_read = GetClientRead { inner: Some(self) };

        let ctx = get_read.map(|(mut this, client)| {
                let r = this.req.clone();
                let req = (this.func)(client, r);
                req.then(|resp| ok((this, resp)))
            })
            .flatten();

        ctx.map(|ctx| {
                let (mut this, resp) = ctx;
                match resp {
                    Ok(resp) => this.resp = Some(Ok(resp)),
                    Err(err) => {
                        this.retry_count -= 1;
                        error!("leader request failed: {:?}", err);
                    }
                };
                this
            })
            .boxed()
    }

    fn check(self) -> PdFuture<(GetClient<Req, Resp, F>, bool)> {
        if self.retry_count == 0 || self.resp.is_some() {
            ok((self, true)).boxed()
        } else {
            // FIXME: should not block the core.
            warn!("updating PD client, block the tokio core");

            let start = Instant::now();
            // Go to sync world.
            let members = self.bundle.rl().members.clone();
            match sync::try_connect_leader(&members) {
                Ok((client, members)) => {
                    // TODO: check if it is updated.
                    let mut bundle = self.bundle.wl();
                    let c: PDAsyncClient = client;
                    bundle.client = Arc::new(c);
                    bundle.members = members;
                    warn!("updating PD client done, spent {:?}", start.elapsed());
                }

                Err(err) => {
                    warn!("updating PD client spent {:?}, err {:?}",
                          start.elapsed(),
                          err);
                    // FIXME: use tokio Timeout insead.
                    thread::sleep(Duration::new(1, 0));
                }
            }

            ok((self, false)).boxed()
        }
    }

    fn get_resp(self) -> Option<Result<Resp>> {
        self.resp
    }

    pub fn retry(self) -> PdFuture<Resp> {
        let this = self;
        loop_fn(this, |this| {
                this.get()
                    .and_then(|this| this.check())
                    .and_then(|(this, done)| {
                        if done {
                            Ok(Loop::Break(this))
                        } else {
                            Ok(Loop::Continue(this))
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
}

struct GetClientRead<Req, Resp, F> {
    inner: Option<GetClient<Req, Resp, F>>,
}

// TODO: impl Stream instead.
impl<Req, Resp, F> Future for GetClientRead<Req, Resp, F> {
    type Item = (GetClient<Req, Resp, F>, Arc<PDAsyncClient>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = self.inner.take().expect("GetClientRead cannot poll twice");
        let ret = match inner.bundle.try_read() {
            Ok(bundle) => Ok(Async::Ready(bundle.client.clone())),
            Err(TryLockError::WouldBlock) => Ok(Async::NotReady),
            // TODO: handle `PoisonError`.
            Err(err) => panic!("{:?}", err),
        };

        ret.map(|async| async.map(|client| (inner, client)))
    }
}

// TODO: GetClientWrite

pub struct Request<C, Req, Resp, F> {
    retry_count: usize,
    client: Arc<C>,
    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

impl<C, Req, Resp, F> Request<C, Req, Resp, F>
    where C: PDAsync + Send + Sync + 'static,
          Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(&C, Req) -> PdFuture<Resp> + Send + 'static
{
    pub fn new(retry: usize, client: Arc<C>, req: Req, f: F) -> Request<C, Req, Resp, F> {
        Request {
            retry_count: retry,
            client: client,
            req: req,
            resp: None,
            func: f,
        }
    }

    fn send(mut self) -> PdFuture<Request<C, Req, Resp, F>> {
        debug!("Request retry remains: {}", self.retry_count);
        let r = self.req.clone();
        let req = (self.func)(self.client.as_ref(), r);
        req.then(|resp| {
                match resp {
                    Ok(resp) => self.resp = Some(Ok(resp)),
                    Err(err) => {
                        self.retry_count -= 1;
                        error!("request failed: {:?}", err);
                    }
                };
                ok(self)
            })
            .boxed()
    }

    fn receive(self) -> PdFuture<(Request<C, Req, Resp, F>, bool)> {
        let done = self.retry_count == 0 || self.resp.is_some();
        ok((self, done)).boxed()
    }

    fn get_resp(self) -> Option<Result<Resp>> {
        self.resp
    }

    pub fn retry(self) -> PdFuture<Resp> {
        let retry_req = self;
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
}
