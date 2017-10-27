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

use std::boxed::FnBox;
use std::convert::From;
use std::fmt::Debug;
use std::sync::Arc;

use grpc::{ClientStreamingSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use futures::{Future, Stream};
use futures::sync::oneshot;
use futures_cpupool::{Builder, CpuPool};
use kvproto::errorpb;
use kvproto::importpb::*;
use kvproto::importpb_grpc;

use server::metrics::*;
use server::errors::Error;
use storage::{self, Storage};
use raftstore::store::{UploadDir, Uploader};

#[derive(Clone)]
pub struct Service {
    pool: CpuPool,
    storage: Storage,
    uploader: Arc<Uploader>,
}

impl Service {
    pub fn new(pool_size: usize, storage: Storage, upload_dir: Arc<UploadDir>) -> Service {
        let pool = Builder::new()
            .name_prefix(thd_name!("import"))
            .pool_size(pool_size)
            .create();
        Service {
            pool: pool,
            storage: storage,
            uploader: Arc::new(Uploader::new(upload_dir)),
        }
    }
}

impl importpb_grpc::Import for Service {
    fn ingest_sst(
        &self,
        ctx: RpcContext,
        mut req: IngestSSTRequest,
        sink: UnarySink<IngestSSTResponse>,
    ) {
        let label = "ingest_sst";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_cb();
        let res = self.storage
            .async_ingest_sst(req.take_context(), req.take_handles().to_vec(), cb);
        if let Err(e) = res {
            ctx.spawn(send_rpc_error!(sink, RpcStatusCode::ResourceExhausted, e));
            return;
        }

        ctx.spawn(
            future
                .map_err(Error::from)
                .map(move |res| {
                    let mut resp = IngestSSTResponse::new();
                    if let Some(e) = extract_error(&res) {
                        resp.set_error(e);
                    }
                    resp
                })
                .then(move |res| match res {
                    Ok(resp) => sink.success(resp),
                    Err(e) => sink.fail(make_rpc_error(RpcStatusCode::Unknown, e)),
                })
                .map(|_| timer.observe_duration())
                .map_err(move |e| {
                    warn!("send rpc response: {:?}", e);
                    GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
                }),
        )
    }

    fn upload_sst(
        &self,
        ctx: RpcContext,
        stream: RequestStream<UploadSSTRequest>,
        sink: ClientStreamingSink<UploadSSTResponse>,
    ) {
        let label = "upload_sst";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let pool = self.pool.clone();
        let up1 = self.uploader.clone();
        let up2 = self.uploader.clone();
        let token = self.uploader.token();
        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |chunk| {
                    let up1 = up1.clone();
                    pool.spawn_fn(move || {
                        if chunk.has_meta() {
                            up1.create(token, chunk.get_meta())?;
                        }
                        if !chunk.get_data().is_empty() {
                            up1.append(token, chunk.get_data())?;
                        }
                        Ok(())
                    })
                })
                .then(move |res| match res {
                    Ok(_) => up2.finish(token).map_err(Error::from),
                    Err(e) => {
                        if let Some(f) = up2.remove(token) {
                            error!("remove {}: {:?}", f, e);
                        }
                        Err(e)
                    }
                })
                .map(|_| UploadSSTResponse::new())
                .then(move |res| match res {
                    Ok(resp) => sink.success(resp),
                    Err(e) => {
                        GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
                        sink.fail(make_rpc_error(RpcStatusCode::Unknown, e))
                    }
                })
                .map(|_| timer.observe_duration())
                .map_err(|e| warn!("send rpc response: {:?}", e)),
        );
    }
}

fn make_cb<T: Debug + Send + 'static>() -> (Box<FnBox(T) + Send>, oneshot::Receiver<T>) {
    let (tx, rx) = oneshot::channel();
    let cb = move |resp| { tx.send(resp).unwrap(); };
    (box cb, rx)
}

fn extract_error<T>(res: &storage::Result<T>) -> Option<errorpb::Error> {
    super::kv::extract_region_error(res).or(match res {
        &Ok(_) => None,
        &Err(ref e) => {
            let mut err = errorpb::Error::new();
            err.set_message(format!("{:?}", e));
            Some(err)
        }
    })
}

fn make_rpc_error(code: RpcStatusCode, err: Error) -> RpcStatus {
    RpcStatus::new(code, Some(format!("{:?}", err)))
}
