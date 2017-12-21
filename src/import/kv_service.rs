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

use uuid::Uuid;
use grpc::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use futures::{Future, Stream};
use futures::sync::mpsc;
use futures_cpupool::{Builder, CpuPool};

use kvproto::importpb::*;
use kvproto::importpb_grpc::*;

use pd::RpcClient;
use util::time::duration_to_sec;

use super::service::*;
use super::metrics::*;
use super::{Client, Config, Error, KVImporter};

#[derive(Clone)]
pub struct ImportKVService {
    cfg: Config,
    pool: CpuPool,
    client: Arc<Client>,
    importer: Arc<KVImporter>,
}

impl ImportKVService {
    pub fn new(cfg: &Config, rpc: Arc<RpcClient>, importer: Arc<KVImporter>) -> ImportKVService {
        let pool = Builder::new()
            .name_prefix("import_kv")
            .pool_size(cfg.num_threads)
            .create();
        let client = Arc::new(Client::new(rpc, cfg.max_import_jobs));
        ImportKVService {
            cfg: cfg.clone(),
            pool: pool,
            client: client,
            importer: importer,
        }
    }
}

impl ImportKv for ImportKVService {
    fn write(
        &self,
        ctx: RpcContext,
        stream: RequestStream<WriteRequest>,
        sink: ClientStreamingSink<WriteResponse>,
    ) {
        let label = "write";
        let timer = Instant::now();
        let token = self.importer.token();
        let import1 = self.importer.clone();
        let import2 = self.importer.clone();
        let bounded_stream = mpsc::spawn(stream, &self.pool, self.cfg.stream_channel_size);

        ctx.spawn(
            bounded_stream
                .map_err(Error::from)
                .for_each(move |mut chunk| {
                    if chunk.has_head() {
                        let head = chunk.get_head();
                        let uuid = Uuid::from_bytes(head.get_uuid())?;
                        import1.attach(token, uuid)?;
                    }
                    if chunk.has_batch() {
                        let options = if chunk.has_options() {
                            chunk.take_options()
                        } else {
                            WriteOptions::new()
                        };
                        import1.write(token, chunk.take_batch(), options)?;
                    }
                    Ok(())
                })
                .then(move |res| match res {
                    Ok(_) => import2.detach(token),
                    Err(e) => {
                        if let Some(engine) = import2.remove(token) {
                            error!("remove {}: {:?}", engine, e);
                        }
                        Err(e)
                    }
                })
                .map(|_| WriteResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn close(&self, ctx: RpcContext, req: CloseRequest, sink: UnarySink<CloseResponse>) {
        let label = "close";
        let timer = Instant::now();
        let import = self.importer.clone();

        ctx.spawn(
            self.pool
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.close(uuid)
                })
                .map(|_| CloseResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn import(&self, ctx: RpcContext, req: ImportRequest, sink: UnarySink<ImportResponse>) {
        let label = "import";
        let timer = Instant::now();
        let client = self.client.clone();
        let import = self.importer.clone();

        ctx.spawn(
            self.pool
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.import(uuid, client)
                })
                .map(|_| ImportResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn delete(&self, ctx: RpcContext, req: DeleteRequest, sink: UnarySink<DeleteResponse>) {
        let label = "delete";
        let timer = Instant::now();
        let import = self.importer.clone();

        ctx.spawn(
            self.pool
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.delete(uuid)
                })
                .map(|_| DeleteResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }
}
