// Copyright 2018 PingCAP, Inc.
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

use uuid::Uuid;
use grpc::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use futures::sync::mpsc;
use futures::{Future, Stream};
use futures_cpupool::{Builder, CpuPool};
use kvproto::importpb::*;
use kvproto::importpb_grpc::*;

use pd::RpcClient;
use util::time::Instant;

use super::client::*;
use super::service::*;
use super::metrics::*;
use super::{Config, Error, KVImporter};

#[derive(Clone)]
pub struct ImportKVService {
    cfg: Config,
    client: Client,
    threads: CpuPool,
    importer: Arc<KVImporter>,
}

impl ImportKVService {
    pub fn new(cfg: Config, client: Arc<RpcClient>, importer: Arc<KVImporter>) -> ImportKVService {
        let threads = Builder::new()
            .name_prefix("kv-importer")
            .pool_size(cfg.num_threads)
            .create();
        let client = Client::new(client, cfg.max_import_jobs);
        ImportKVService {
            cfg: cfg,
            client: client,
            threads: threads,
            importer: importer,
        }
    }
}

impl ImportKv for ImportKVService {
    fn open(&self, ctx: RpcContext, req: OpenRequest, sink: UnarySink<OpenResponse>) {
        let label = "open";
        let timer = Instant::now_coarse();

        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.open(uuid)
                })
                .map(|_| OpenResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn write(
        &self,
        ctx: RpcContext,
        stream: RequestStream<WriteRequest>,
        sink: ClientStreamingSink<WriteResponse>,
    ) {
        let label = "write";
        let timer = Instant::now_coarse();

        let token = self.importer.token();
        let import1 = Arc::clone(&self.importer);
        let import2 = Arc::clone(&self.importer);
        let bounded_stream = mpsc::spawn(stream, &self.threads, self.cfg.stream_channel_window);

        ctx.spawn(
            self.threads.spawn(
                bounded_stream
                    .map_err(Error::from)
                    .for_each(move |mut chunk| {
                        let start = Instant::now_coarse();
                        if chunk.has_head() {
                            let head = chunk.get_head();
                            let uuid = Uuid::from_bytes(head.get_uuid())?;
                            import1.attach(token, uuid)?;
                        }
                        if chunk.has_batch() {
                            let batch = chunk.take_batch();
                            let options = chunk.take_options();
                            let batch_size = import1.write(token, batch, options)?;
                            IMPORT_WRITE_CHUNK_BYTES.observe(batch_size as f64);
                        }
                        IMPORT_WRITE_CHUNK_DURATION.observe(start.elapsed_secs());
                        Ok(())
                    })
                    .then(move |res| match res {
                        Ok(_) => import2.detach(token),
                        Err(e) => {
                            if let Some(engine) = import2.remove(token) {
                                error!("remove {:?}: {:?}", engine, e);
                            }
                            Err(e)
                        }
                    })
                    .then(move |res| match res {
                        Ok(_) => Ok(WriteResponse::new()),
                        Err(Error::EngineNotFound(v)) => {
                            let mut resp = WriteResponse::new();
                            resp.mut_error()
                                .mut_engine_not_found()
                                .set_uuid(v.as_bytes().to_vec());
                            Ok(resp)
                        }
                        Err(e) => Err(e),
                    })
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
            ),
        )
    }

    fn close(&self, ctx: RpcContext, req: CloseRequest, sink: UnarySink<CloseResponse>) {
        let label = "close";
        let timer = Instant::now_coarse();

        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.close(uuid)
                })
                .then(move |res| match res {
                    Ok(_) => Ok(CloseResponse::new()),
                    Err(Error::EngineNotFound(v)) => {
                        let mut resp = CloseResponse::new();
                        resp.mut_error()
                            .mut_engine_not_found()
                            .set_uuid(v.as_bytes().to_vec());
                        Ok(resp)
                    }
                    Err(e) => Err(e),
                })
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn import(&self, ctx: RpcContext, req: ImportRequest, sink: UnarySink<ImportResponse>) {
        let label = "import";
        let timer = Instant::now_coarse();

        let client = self.client.clone();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.import(uuid, client)
                })
                .map(|_| ImportResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn cleanup(&self, ctx: RpcContext, req: CleanupRequest, sink: UnarySink<CleanupResponse>) {
        let label = "cleanup";
        let timer = Instant::now_coarse();

        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.cleanup(uuid)
                })
                .map(|_| CleanupResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn compact(&self, ctx: RpcContext, _: CompactRequest, sink: UnarySink<CompactResponse>) {
        let label = "compact";
        let timer = Instant::now_coarse();

        let client = self.client.clone();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || import.compact(client))
                .map(|_| CompactResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }
}
