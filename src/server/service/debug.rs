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

use std::error;
use std::boxed::FnBox;
use std::sync::Arc;
use std::fmt::{self, Display};

use grpc::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink};
use futures::Future;
use futures::sync::oneshot;
use kvproto::debugpb_grpc;
use kvproto::debugpb::*;
use rocksdb::DB;

use util::worker::{Runnable, Scheduler};
use raftstore::store::keys;
use raftstore::store::engine::Peekable;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};

use super::make_callback;

pub type Callback = Box<FnBox(Result<Response, Error>) + Send>;

#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Request>,
}

impl Service {
    pub fn new(scheduler: Scheduler<Request>) -> Service {
        Service { scheduler }
    }

    fn handle_response<F, M, Q>(
        &self,
        ctx: RpcContext,
        sink: UnarySink<Q>,
        req: Request,
        resp: F,
        map: M,
        tag: &'static str,
    ) where
        Q: 'static,
        M: FnOnce(Response) -> Q + Send + 'static,
        F: Future<Item = Result<Response, Error>, Error = oneshot::Canceled> + Send + 'static,
    {
        let on_error = move |e| {
            error!("{} failed: {:?}", tag, e);
        };
        if self.scheduler.schedule(req).is_ok() {
            let future = resp.then(|v| match v {
                Ok(Ok(resp)) => sink.success(map(resp)).map_err(on_error),
                Ok(Err(Error::NotFound(msg))) => {
                    let status = RpcStatus::new(RpcStatusCode::NotFound, Some(msg));
                    sink.fail(status).map_err(on_error)
                }
                Ok(Err(Error::InvalidArgument(msg))) => {
                    let status = RpcStatus::new(RpcStatusCode::InvalidArgument, Some(msg));
                    sink.fail(status).map_err(on_error)
                }
                Ok(Err(Error::Other(e))) => {
                    let status = RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", e)));
                    sink.fail(status).map_err(on_error)
                }
                Err(canceled) => {
                    let status =
                        RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", canceled)));
                    sink.fail(status).map_err(on_error)
                }
            });
            ctx.spawn(future);
        } else {
            let status = RpcStatus::new(RpcStatusCode::Unavailable, None);
            ctx.spawn(sink.fail(status).map_err(on_error));
        }
    }
}

impl debugpb_grpc::Debug for Service {
    fn get(&self, ctx: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        const LABEL: &'static str = "debug_get";

        let (cb, future) = make_callback();
        let req = Request::Get {
            cf: req.get_cf(),
            key_encoded: req.take_key_encoded(),
            callback: cb,
        };

        let map = |response| match response {
            Response::Get { value } => {
                let mut resp = GetResponse::new();
                resp.set_value(value);
                resp
            }
        };
        self.handle_response(ctx, sink, req, future, map, LABEL);
    }

    fn mvcc(&self, _: RpcContext, _: MvccRequest, _: UnarySink<MvccResponse>) {
        unimplemented!()
    }

    fn raft_log(&self, _: RpcContext, _: RaftLogRequest, _: UnarySink<RaftLogResponse>) {
        unimplemented!()
    }

    fn region_info(&self, _: RpcContext, _: RegionInfoRequest, _: UnarySink<RegionInfoResponse>) {
        unimplemented!()
    }

    fn size(&self, _: RpcContext, _: SizeRequest, _: UnarySink<SizeResponse>) {
        unimplemented!()
    }

    fn scan(&self, _: RpcContext, _: ScanRequest, _: ServerStreamingSink<ScanResponse>) {
        unimplemented!()
    }
}

pub struct Runner {
    kv_db: Arc<DB>,
    raft_db: Arc<DB>,
}

impl Runner {
    pub fn new(kv_db: Arc<DB>, raft_db: Arc<DB>) -> Runner {
        Runner { kv_db, raft_db }
    }

    fn on_get(&self, cf: CF, key_encoded: Vec<u8>, cb: Callback) {
        let (cf, db) = match cf {
            CF::DEFAULT => (CF_DEFAULT, &self.kv_db),
            CF::WRITE => (CF_WRITE, &self.kv_db),
            CF::LOCK => (CF_LOCK, &self.kv_db),
            CF::RAFT => (CF_RAFT, &self.raft_db),
            _ => {
                cb(Err(Error::InvalidArgument("invalid cf".to_owned())));
                return;
            }
        };
        cb(match db.get_value_cf(
            cf,
            &keys::data_key(key_encoded.as_slice()),
        ) {
            Ok(Some(v)) => Ok(Response::Get {
                value: (&v).to_vec(),
            }),
            Ok(None) => Err(Error::NotFound(
                format!("get none value for encoded key {:?}", key_encoded,),
            )),
            Err(e) => Err(box_err!(e)),
        })
    }
}

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        InvalidArgument(msg: String) {
            description(msg)
            display("Invalid Argument {:?}", msg)
        }
        NotFound(msg: String) {
            description(msg)
            display("Not Found {:?}", msg)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

pub enum Request {
    Get {
        cf: CF,
        key_encoded: Vec<u8>,
        callback: Callback,
    },
}

impl Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Request::Get {
                ref cf,
                ref key_encoded,
                ..
            } => write!(f, "Request::Get key: {:?} at cf {:?}", key_encoded, cf),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Get { value: Vec<u8> },
}

impl Runnable<Request> for Runner {
    fn run(&mut self, req: Request) {
        match req {
            Request::Get {
                cf,
                key_encoded,
                callback,
            } => {
                self.on_get(cf, key_encoded, callback);
            }
        }
    }
}
