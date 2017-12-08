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
use std::time::Instant;

use grpc::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use futures::{future, Future, Stream};
use futures::sync::mpsc;
use futures_cpupool::{Builder, CpuPool};

use kvproto::importpb::*;
use kvproto::importpb_grpc::*;

use storage::{Error as StorageError, Storage};
use storage::engine::Error as EngineError;
use util::time::duration_to_sec;

use super::service::*;
use super::metrics::*;
use super::{Config, Error, SSTImporter};

#[derive(Clone)]
pub struct ImportSSTService {
    cfg: Config,
    pool: CpuPool,
    storage: Storage,
    importer: Arc<SSTImporter>,
}

impl ImportSSTService {
    pub fn new(cfg: &Config, storage: Storage, importer: Arc<SSTImporter>) -> ImportSSTService {
        let pool = Builder::new()
            .name_prefix("import_sst")
            .pool_size(cfg.num_threads)
            .create();
        ImportSSTService {
            cfg: cfg.clone(),
            pool: pool,
            storage: storage,
            importer: importer,
        }
    }
}

impl ImportSst for ImportSSTService {
    fn upload(
        &self,
        ctx: RpcContext,
        stream: RequestStream<UploadRequest>,
        sink: ClientStreamingSink<UploadResponse>,
    ) {
        let label = "upload";
        let timer = Instant::now();

        let token = self.importer.token();
        let import1 = self.importer.clone();
        let import2 = self.importer.clone();
        let bounded_stream = mpsc::spawn(stream, &self.pool, self.cfg.stream_channel_size);

        ctx.spawn(
            bounded_stream
                .map_err(Error::from)
                .for_each(move |chunk| {
                    if chunk.has_meta() {
                        import1.create(token, chunk.get_meta())?;
                    }
                    if !chunk.get_data().is_empty() {
                        import1.append(token, chunk.get_data())?;
                    }
                    Ok(())
                })
                .then(move |res| match res {
                    Ok(_) => import2.finish(token),
                    Err(e) => {
                        if let Some(f) = import2.remove(token) {
                            error!("remove {}: {:?}", f, e);
                        }
                        Err(e)
                    }
                })
                .map(|_| UploadResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn ingest(&self, ctx: RpcContext, mut req: IngestRequest, sink: UnarySink<IngestResponse>) {
        let label = "ingest";
        let timer = Instant::now();

        let (cb, future) = make_cb();
        if let Err(e) = self.storage
            .async_ingest(req.take_context(), req.take_sst(), cb)
        {
            return send_rpc_error(ctx, sink, e);
        }

        ctx.spawn(
            future
                .map_err(Error::from)
                .then(|res| match res {
                    Ok(res) => {
                        let mut resp = IngestResponse::new();
                        match res {
                            Err(StorageError::Engine(EngineError::Request(e))) => {
                                resp.set_error(e.into());
                            }
                            Err(e) => {
                                resp.mut_error().set_message(format!("{:?}", e));
                            }
                            _ => {}
                        }
                        future::ok(resp)
                    }
                    Err(e) => future::err(e),
                })
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }
}
