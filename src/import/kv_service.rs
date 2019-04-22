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

use futures::sync::mpsc;
use futures::{Future, Stream};
use futures_cpupool::{Builder, CpuPool};
use grpc::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use kvproto::import_kvpb::*;
use kvproto::import_kvpb_grpc::*;
use uuid::Uuid;

use raftstore::store::keys;
use storage::types::Key;
use util::time::Instant;

use super::client::*;
use super::metrics::*;
use super::service::*;
use super::{Config, Error, KVImporter};

#[derive(Clone)]
pub struct ImportKVService {
    cfg: Config,
    threads: CpuPool,
    importer: Arc<KVImporter>,
}

impl ImportKVService {
    pub fn new(cfg: Config, importer: Arc<KVImporter>) -> ImportKVService {
        let threads = Builder::new()
            .name_prefix("kv-importer")
            .pool_size(cfg.num_threads)
            .create();
        ImportKVService {
            cfg,
            threads,
            importer,
        }
    }
}

impl ImportKv for ImportKVService {
    fn switch_mode(
        &self,
        ctx: RpcContext,
        req: SwitchModeRequest,
        sink: UnarySink<SwitchModeResponse>,
    ) {
        let label = "switch_mode";
        let timer = Instant::now_coarse();
        let min_available_ratio = self.cfg.min_available_ratio;

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let client = Client::new(req.get_pd_addr(), 1, min_available_ratio)?;
                    match client.switch_cluster(req.get_request()) {
                        Ok(_) => {
                            info!("switch cluster {:?}", req.get_request());
                            Ok(())
                        }
                        Err(e) => {
                            error!("switch cluster {:?}: {:?}", req.get_request(), e);
                            Err(e)
                        }
                    }
                })
                .map(|_| SwitchModeResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn open_engine(
        &self,
        ctx: RpcContext,
        req: OpenEngineRequest,
        sink: UnarySink<OpenEngineResponse>,
    ) {
        let label = "open_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.open_engine(uuid)
                })
                .map(|_| OpenEngineResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn write_engine(
        &self,
        ctx: RpcContext,
        stream: RequestStream<WriteEngineRequest>,
        sink: ClientStreamingSink<WriteEngineResponse>,
    ) {
        let label = "write_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);
        let bounded_stream = mpsc::spawn(stream, &self.threads, self.cfg.stream_channel_window);

        ctx.spawn(
            self.threads.spawn(
                bounded_stream
                    .into_future()
                    .map_err(|(e, _)| Error::from(e))
                    .and_then(move |(chunk, stream)| {
                        // The first message of the stream specifies the uuid of
                        // the corresponding engine.
                        // The engine should be opened before any write.
                        let head = match chunk {
                            Some(ref chunk) if chunk.has_head() => chunk.get_head(),
                            _ => return Err(Error::InvalidChunk),
                        };
                        let uuid = Uuid::from_bytes(head.get_uuid())?;
                        let engine = import.bind_engine(uuid)?;
                        Ok((engine, stream))
                    })
                    .and_then(move |(engine, stream)| {
                        stream.map_err(Error::from).for_each(move |mut chunk| {
                            let start = Instant::now_coarse();
                            if !chunk.has_batch() {
                                return Err(Error::InvalidChunk);
                            }
                            let batch = chunk.take_batch();
                            let batch_size = engine.write(batch)?;
                            IMPORT_WRITE_CHUNK_BYTES.observe(batch_size as f64);
                            IMPORT_WRITE_CHUNK_DURATION.observe(start.elapsed_secs());
                            Ok(())
                        })
                    })
                    .then(move |res| match res {
                        Ok(_) => Ok(WriteEngineResponse::new()),
                        Err(Error::EngineNotFound(v)) => {
                            let mut resp = WriteEngineResponse::new();
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

    fn close_engine(
        &self,
        ctx: RpcContext,
        req: CloseEngineRequest,
        sink: UnarySink<CloseEngineResponse>,
    ) {
        let label = "close_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.close_engine(uuid)
                })
                .then(move |res| match res {
                    Ok(_) => Ok(CloseEngineResponse::new()),
                    Err(Error::EngineNotFound(v)) => {
                        let mut resp = CloseEngineResponse::new();
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

    fn import_engine(
        &self,
        ctx: RpcContext,
        req: ImportEngineRequest,
        sink: UnarySink<ImportEngineResponse>,
    ) {
        let label = "import_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.import_engine(uuid, req.get_pd_addr())
                })
                .map(|_| ImportEngineResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn cleanup_engine(
        &self,
        ctx: RpcContext,
        req: CleanupEngineRequest,
        sink: UnarySink<CleanupEngineResponse>,
    ) {
        let label = "cleanup_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let uuid = Uuid::from_bytes(req.get_uuid())?;
                    import.cleanup_engine(uuid)
                })
                .map(|_| CleanupEngineResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }

    fn compact_cluster(
        &self,
        ctx: RpcContext,
        req: CompactClusterRequest,
        sink: UnarySink<CompactClusterResponse>,
    ) {
        let label = "compact_cluster";
        let timer = Instant::now_coarse();
        let min_available_ratio = self.cfg.min_available_ratio;

        let mut compact = req.get_request().clone();
        if compact.has_range() {
            // Convert the range to a TiKV encoded data range.
            let start = Key::from_raw(compact.get_range().get_start());
            compact
                .mut_range()
                .set_start(keys::data_key(start.as_encoded()));
            let end = Key::from_raw(compact.get_range().get_end());
            compact
                .mut_range()
                .set_end(keys::data_end_key(end.as_encoded()));
        }

        ctx.spawn(
            self.threads
                .spawn_fn(move || {
                    let client = Client::new(req.get_pd_addr(), 1, min_available_ratio)?;
                    match client.compact_cluster(&compact) {
                        Ok(_) => {
                            info!("compact cluster {:?}", compact);
                            Ok(())
                        }
                        Err(e) => {
                            error!("compact cluster {:?}: {:?}", compact, e);
                            Err(e)
                        }
                    }
                })
                .map(|_| CompactClusterResponse::new())
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }
}
