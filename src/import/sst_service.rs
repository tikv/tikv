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
use futures_cpupool::{Builder, CpuPool};

use kvproto::importpb::*;
use kvproto::importpb_grpc::*;

use storage::{Error as StorageError, Storage};
use storage::engine::Error as EngineError;
use util::time::duration_to_sec;

use super::util::*;
use super::metrics::*;
use super::{Error, SSTImporter};

#[derive(Clone)]
pub struct ImportSSTService {
    threads: CpuPool,
    storage: Storage,
    importer: Arc<SSTImporter>,
}

impl ImportSSTService {
    pub fn new(
        num_threads: usize,
        storage: Storage,
        importer: Arc<SSTImporter>,
    ) -> ImportSSTService {
        let threads = Builder::new()
            .name_prefix("import_sst")
            .pool_size(num_threads)
            .create();
        ImportSSTService {
            threads: threads,
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
        let threads1 = self.threads.clone();
        let threads2 = self.threads.clone();

        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |chunk| {
                    let import1 = import1.clone();
                    threads1.spawn_fn(move || {
                        if chunk.has_meta() {
                            import1.create(token, chunk.get_meta())?;
                        }
                        if !chunk.get_data().is_empty() {
                            import1.append(token, chunk.get_data())?;
                        }
                        Ok(())
                    })
                })
                .then(move |res| {
                    let import2 = import2.clone();
                    threads2.spawn_fn(move || match res {
                        Ok(_) => import2.finish(token),
                        Err(e) => {
                            if let Some(f) = import2.remove(token) {
                                error!("remove {}: {:?}", f, e);
                            }
                            Err(e)
                        }
                    })
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
