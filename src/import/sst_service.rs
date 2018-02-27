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
use futures::{Future, Stream};
use futures::sync::mpsc;
use futures_cpupool::{Builder, CpuPool};
use kvproto::importpb::*;
use kvproto::importpb_grpc::*;

use storage::Storage;
use util::time::Instant;

use super::metrics::*;
use super::{Config, Error, SSTImporter};

#[derive(Clone)]
pub struct ImportSSTService {
    cluster_id: u64,
    cfg: Config,
    threads: CpuPool,
    storage: Storage,
    importer: Arc<SSTImporter>,
}

impl ImportSSTService {
    pub fn new(
        cluster_id: u64,
        cfg: Config,
        storage: Storage,
        importer: Arc<SSTImporter>,
    ) -> ImportSSTService {
        let threads = Builder::new()
            .name_prefix("sst-importer")
            .pool_size(cfg.num_threads)
            .create();
        ImportSSTService {
            cluster_id: cluster_id,
            cfg: cfg,
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
        try_check_header!(ctx, sink, self.cluster_id, label);

        let timer = Instant::now_coarse();
        let token = self.importer.token();
        let thread1 = self.threads.clone();
        let thread2 = self.threads.clone();
        let import1 = Arc::clone(&self.importer);
        let import2 = Arc::clone(&self.importer);
        let bounded_stream = mpsc::spawn(stream, &self.threads, self.cfg.stream_channel_window);

        let result = bounded_stream
            .map_err(Error::from)
            .for_each(move |chunk| {
                let import1 = Arc::clone(&import1);
                thread1.spawn_fn(move || {
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
            })
            .then(move |res| {
                thread2.spawn_fn(move || match res {
                    Ok(_) => import2.finish(token),
                    Err(e) => {
                        if let Some(f) = import2.remove(token) {
                            error!("remove {:?}: {:?}", f, e);
                        }
                        Err(e)
                    }
                })
            })
            .then(move |res| match res {
                Ok(_) => {
                    IMPORT_RPC_DURATION
                        .with_label_values(&[label, "ok"])
                        .observe(timer.elapsed_secs());
                    Ok(UploadResponse::new())
                }
                Err(e) => {
                    IMPORT_RPC_DURATION
                        .with_label_values(&[label, "error"])
                        .observe(timer.elapsed_secs());
                    Err(e)
                }
            });
        send_response!(ctx, sink, result, Into::into, label);
    }

    fn ingest(&self, _: RpcContext, _: IngestRequest, _: UnarySink<IngestResponse>) {
        unimplemented!();
    }
}
