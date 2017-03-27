use std::sync::Arc;

// use futures::Poll;
// use futures::Async;
use futures::Future;
use futures::future::ok;

use kvproto::pdpb::GetMembersResponse;
use kvproto::pdpb_grpc::PDAsync;

use super::super::PdFuture;
use super::super::Result;
// use super::super::Error;

// pub struct GetClient<C: PDAsync> {
//     client: Arc<C>,
// }

// impl<C: PDAsync> Future for GetClient<C> {
//     type Item = Arc<C>;
//     type Error = Error;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         // TODO: resolve it in future.
//         Ok(Async::Ready(self.client.clone()))
//     }
// }

pub struct LeaderClient<C> {
    members: GetMembersResponse,
    client: Arc<C>,
}

impl<C: PDAsync> LeaderClient<C> {
    pub fn new(client: C, members: GetMembersResponse) -> LeaderClient<C> {
        LeaderClient {
            client: Arc::new(client),
            members: members,
        }
    }

    // // TODO: resolve it in future.
    // pub fn client(&self) -> GetClient<C> {
    //     GetClient { client: self.client.clone() }
    // }

    pub fn clone_client(&self) -> Arc<C> {
        self.client.clone()
    }

    pub fn get_client(&self) -> &C {
        self.client.as_ref()
    }

    pub fn set_client(&mut self, client: C) {
        self.client = Arc::new(client);
    }

    pub fn get_members(&self) -> &GetMembersResponse {
        &self.members
    }

    pub fn set_members(&mut self, members: GetMembersResponse) {
        self.members = members;
    }
}

pub struct Request<C, R, F> {
    retry_count: usize,
    client: Arc<C>,
    resp: Option<Result<R>>,
    func: F,
}

impl<C, R, F> Request<C, R, F>
    where C: PDAsync + Send + Sync + 'static,
          R: Send + 'static,
          F: FnMut(&C) -> PdFuture<R> + Send + 'static
{
    pub fn new(retry: usize, client: Arc<C>, f: F) -> Request<C, R, F> {
        Request {
            retry_count: retry,
            client: client,
            resp: None,
            func: f,
        }
    }

    pub fn send(mut self) -> PdFuture<Request<C, R, F>> {
        debug!("request retry remains: {}", self.retry_count);
        let req = (self.func)(self.client.as_ref());
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

    pub fn receive(self) -> PdFuture<(Request<C, R, F>, bool)> {
        let done = self.retry_count == 0 || self.resp.is_some();
        ok((self, done)).boxed()
    }

    pub fn get_resp(self) -> Option<Result<R>> {
        self.resp
    }
}
