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

use grpc::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use futures::{future, Future, Stream};
use futures::sync::mpsc;
use futures_cpupool::{Builder, CpuPool};
use kvproto::importpb::*;
use kvproto::importpb_grpc::*;
use kvproto::raft_cmdpb::*;

use util::future::paired_future_callback;
use util::time::Instant;
use raftstore::store::Callback;
use server::transport::RaftStoreRouter;

use super::service::*;
use super::metrics::*;
use super::{Config, Error, SSTImporter};

#[derive(Clone)]
pub struct ImportSSTService<Router> {
    cfg: Config,
    router: Router,
    threads: CpuPool,
    importer: Arc<SSTImporter>,
}

impl<Router: RaftStoreRouter> ImportSSTService<Router> {
    pub fn new(
        cfg: Config,
        router: Router,
        importer: Arc<SSTImporter>,
    ) -> ImportSSTService<Router> {
        let threads = Builder::new()
            .name_prefix("sst-importer")
            .pool_size(cfg.num_threads)
            .create();
        ImportSSTService {
            cfg: cfg,
            router: router,
            threads: threads,
            importer: importer,
        }
    }
}

impl<Router: RaftStoreRouter> ImportSst for ImportSSTService<Router> {
    fn upload(
        &self,
        ctx: RpcContext,
        stream: RequestStream<UploadRequest>,
        sink: ClientStreamingSink<UploadResponse>,
    ) {
        let label = "upload";
        let timer = Instant::now_coarse();

        let token = self.importer.token();
        let import1 = Arc::clone(&self.importer);
        let import2 = Arc::clone(&self.importer);
        let bounded_stream = mpsc::spawn(stream, &self.threads, self.cfg.stream_channel_window);

        ctx.spawn(
            self.threads.spawn(
                bounded_stream
                    .map_err(Error::from)
                    .for_each(move |chunk| {
                        let start = Instant::now_coarse();
                        if chunk.has_meta() {
                            import1.create(token, chunk.get_meta())?;
                        }
                        if !chunk.get_data().is_empty() {
                            let data = chunk.get_data();
                            import1.append(token, data)?;
                            IMPORT_UPLOAD_CHUNK_BYTES.observe(data.len() as f64);
                        }
                        IMPORT_UPLOAD_CHUNK_DURATION.observe(start.elapsed_secs());
                        Ok(())
                    })
                    .then(move |res| match res {
                        Ok(_) => import2.finish(token),
                        Err(e) => {
                            if let Some(f) = import2.remove(token) {
                                error!("remove {:?}: {:?}", f, e);
                            }
                            Err(e)
                        }
                    })
                    .map(|_| UploadResponse::new())
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
            ),
        )
    }

    fn ingest(&self, ctx: RpcContext, mut req: IngestRequest, sink: UnarySink<IngestResponse>) {
        let label = "ingest";
        let timer = Instant::now_coarse();

        // Make ingest command.
        let mut ingest = Request::new();
        ingest.set_cmd_type(CmdType::IngestSST);
        ingest.mut_ingest_sst().set_sst(req.take_sst());
        let mut context = req.take_context();
        let mut header = RaftRequestHeader::new();
        header.set_peer(context.take_peer());
        header.set_region_id(context.get_region_id());
        header.set_region_epoch(context.take_region_epoch());
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);

        let (cb, future) = paired_future_callback();
        if let Err(e) = self.router.send_command(cmd, Callback::Write(cb)) {
            return send_rpc_error(ctx, sink, e);
        }

        ctx.spawn(
            future
                .map_err(Error::from)
                .then(|res| match res {
                    Ok(mut res) => {
                        let mut resp = IngestResponse::new();
                        let mut header = res.response.take_header();
                        if header.has_error() {
                            resp.set_error(header.take_error());
                        }
                        future::ok(resp)
                    }
                    Err(e) => future::err(e),
                })
                .then(move |res| send_rpc_response!(res, sink, label, timer)),
        )
    }
}
