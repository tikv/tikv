// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use collections::HashSet;
use engine_traits::{KvEngine, CF_WRITE};
use file_system::{set_io_type, IOType};
use futures::{
    executor::{ThreadPool, ThreadPoolBuilder},
    future::join_all,
    sink::SinkExt,
    stream::TryStreamExt,
    TryFutureExt,
};
use grpcio::{
    ClientStreamingSink, RequestStream, RpcContext, ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::{
    encryptionpb::EncryptionMethod,
    errorpb,
    import_sstpb::{RawWriteRequest_oneof_chunk as RawChunk, WriteRequest_oneof_chunk as Chunk, *},
    kvrpcpb::Context,
    raft_cmdpb::*,
};
use protobuf::Message;
use raftstore::{
    router::RaftStoreRouter,
    store::{Callback, RaftCmdExtraOpts, RegionSnapshot},
};
use sst_importer::{error_inc, metrics::*, sst_meta_to_path, Config, Error, Result, SstImporter};
use tikv_util::{
    config::ReadableSize,
    future::{create_stream_with_buffer, paired_future_callback},
    time::{Instant, Limiter},
};
use txn_types::Key;

use super::make_rpc_error;
use crate::{import::duplicate_detect::DuplicateDetector, server::CONFIG_ROCKSDB_GAUGE};

/// ImportSstService provides tikv-server with the ability to ingest SST files.
///
/// It saves the SST sent from client to a file and then sends a command to
/// raftstore to trigger the ingest process.
#[derive(Clone)]
pub struct ImportSstService<E, Router>
where
    E: KvEngine,
{
    cfg: Config,
    engine: E,
    router: Router,
    threads: ThreadPool,
    importer: Arc<SstImporter>,
    limiter: Limiter,
    task_slots: Arc<Mutex<HashSet<PathBuf>>>,
    raft_entry_max_size: ReadableSize,
}

pub struct SnapshotResult<E: KvEngine> {
    snapshot: RegionSnapshot<E::Snapshot>,
    term: u64,
}

impl<E, Router> ImportSstService<E, Router>
where
    E: KvEngine,
    Router: 'static + RaftStoreRouter<E>,
{
    pub fn new(
        cfg: Config,
        raft_entry_max_size: ReadableSize,
        router: Router,
        engine: E,
        importer: Arc<SstImporter>,
    ) -> ImportSstService<E, Router> {
        let props = tikv_util::thread_group::current_properties();
        let threads = ThreadPoolBuilder::new()
            .pool_size(cfg.num_threads)
            .name_prefix("sst-importer")
            .after_start(move |_| {
                tikv_util::thread_group::set_properties(props.clone());
                tikv_alloc::add_thread_memory_accessor();
                set_io_type(IOType::Import);
            })
            .before_stop(move |_| tikv_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();
        importer.start_switch_mode_check(&threads, engine.clone());
        ImportSstService {
            cfg,
            engine,
            threads,
            router,
            importer,
            limiter: Limiter::new(f64::INFINITY),
            task_slots: Arc::new(Mutex::new(HashSet::default())),
            raft_entry_max_size,
        }
    }

    fn acquire_lock(task_slots: &Arc<Mutex<HashSet<PathBuf>>>, meta: &SstMeta) -> Result<bool> {
        let mut slots = task_slots.lock().unwrap();
        let p = sst_meta_to_path(meta)?;
        Ok(slots.insert(p))
    }

    fn release_lock(task_slots: &Arc<Mutex<HashSet<PathBuf>>>, meta: &SstMeta) -> Result<bool> {
        let mut slots = task_slots.lock().unwrap();
        let p = sst_meta_to_path(meta)?;
        Ok(slots.remove(&p))
    }

    async fn async_snapshot(
        router: Router,
        header: RaftRequestHeader,
    ) -> std::result::Result<SnapshotResult<E>, errorpb::Error> {
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        let (cb, future) = paired_future_callback();
        if let Err(e) = router.send_command(cmd, Callback::Read(cb), RaftCmdExtraOpts::default()) {
            return Err(e.into());
        }
        let mut res = future.await.map_err(|_| {
            let mut err = errorpb::Error::default();
            let err_str = "too many sst files are ingesting";
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(err_str.to_string());
            err.set_message(err_str.to_string());
            err.set_server_is_busy(server_is_busy_err);
            err
        })?;
        let mut header = res.response.take_header();
        if header.has_error() {
            return Err(header.take_error());
        }
        Ok(SnapshotResult {
            snapshot: res.snapshot.unwrap(),
            term: header.get_current_term(),
        })
    }

    fn check_write_stall(&self) -> Option<errorpb::Error> {
        if self.importer.get_mode() == SwitchMode::Normal
            && self
                .engine
                .ingest_maybe_slowdown_writes(CF_WRITE)
                .expect("cf")
        {
            let mut errorpb = errorpb::Error::default();
            let err = "too many sst files are ingesting";
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(err.to_string());
            errorpb.set_message(err.to_string());
            errorpb.set_server_is_busy(server_is_busy_err);
            return Some(errorpb);
        }
        None
    }

    // we need to remove duplicate keys in here, since
    // in https://github.com/tikv/tikv/blob/a401f78bc86f7e6ea6a55ad9f453ae31be835b55/components/resolved_ts/src/cmd.rs#L204
    // will panic if found duplicated entry during Vec<PutRequest>.
    fn build_apply_request<'a, 'b>(
        raft_size: u64,
        reqs: &'a mut HashMap<Vec<u8>, (Request, u64)>,
        cmd_reqs: &'a mut Vec<RaftCmdRequest>,
        is_delete: bool,
        cf: &'b str,
        context: Context,
    ) -> Box<dyn FnMut(Vec<u8>, Vec<u8>) + 'b>
    where
        'a: 'b,
    {
        let mut req_size = 0_u64;

        // use callback to collect kv data.
        if is_delete {
            Box::new(move |k: Vec<u8>, _v: Vec<u8>| {
                let mut req = Request::default();
                let mut del = DeleteRequest::default();

                let (encoded_key, ts) = Key::split_on_ts_for(&k).expect("key without ts");
                del.set_key(k.clone());
                del.set_cf(cf.to_string());
                req.set_cmd_type(CmdType::Delete);
                req.set_delete(del);
                req_size += req.compute_size() as u64;
                if reqs
                    .get(encoded_key)
                    .map(|(_, old_ts)| *old_ts < ts.into_inner())
                    .unwrap_or(true)
                {
                    reqs.insert(encoded_key.to_owned(), (req, ts.into_inner()));
                };
                // When the request size get grow to half of the max request size,
                // build the request and add it to a batch.
                if req_size > raft_size / 2 {
                    req_size = 0;
                    let cmd = make_request(reqs, context.clone());
                    cmd_reqs.push(cmd);
                }
            })
        } else {
            Box::new(move |k: Vec<u8>, v: Vec<u8>| {
                let mut req = Request::default();
                let mut put = PutRequest::default();

                let (encoded_key, ts) = Key::split_on_ts_for(&k).expect("key without ts");
                put.set_key(k.clone());
                put.set_value(v);
                put.set_cf(cf.to_string());
                req.set_cmd_type(CmdType::Put);
                req.set_put(put);
                req_size += req.compute_size() as u64;
                if reqs
                    .get(encoded_key)
                    .map(|(_, old_ts)| *old_ts < ts.into_inner())
                    .unwrap_or(true)
                {
                    reqs.insert(encoded_key.to_owned(), (req, ts.into_inner()));
                };
                if req_size > raft_size / 2 {
                    req_size = 0;
                    let cmd = make_request(reqs, context.clone());
                    cmd_reqs.push(cmd);
                }
            })
        }
    }

    fn ingest_files(
        &self,
        context: Context,
        label: &'static str,
        ssts: Vec<SstMeta>,
    ) -> impl Future<Output = Result<IngestResponse>> {
        let header = make_request_header(context);
        let snapshot_res = Self::async_snapshot(self.router.clone(), header.clone());
        let router = self.router.clone();
        let importer = self.importer.clone();
        async move {
            // check api version
            if !importer.as_ref().check_api_version(&ssts)? {
                return Err(Error::IncompatibleApiVersion);
            }

            let mut resp = IngestResponse::default();
            let res = match snapshot_res.await {
                Ok(snap) => snap,
                Err(e) => {
                    pb_error_inc(label, &e);
                    resp.set_error(e);
                    return Ok(resp);
                }
            };

            fail_point!("import::sst_service::ingest");
            // Make ingest command.
            let mut cmd = RaftCmdRequest::default();
            cmd.set_header(header);
            cmd.mut_header().set_term(res.term);
            for sst in ssts.iter() {
                let mut ingest = Request::default();
                ingest.set_cmd_type(CmdType::IngestSst);
                ingest.mut_ingest_sst().set_sst(sst.clone());
                cmd.mut_requests().push(ingest);
            }

            // Here we shall check whether the file has been ingested before. This operation
            // must execute after geting a snapshot from raftstore to make sure that the
            // current leader has applied to current term.
            for sst in ssts.iter() {
                if !importer.exist(sst) {
                    warn!(
                        "sst [{:?}] not exist. we may retry an operation that has already succeeded",
                        sst
                    );
                    let mut errorpb = errorpb::Error::default();
                    let err = "The file which would be ingested doest not exist.";
                    let stale_err = errorpb::StaleCommand::default();
                    errorpb.set_message(err.to_string());
                    errorpb.set_stale_command(stale_err);
                    resp.set_error(errorpb);
                    return Ok(resp);
                }
            }

            let (cb, future) = paired_future_callback();
            if let Err(e) =
                router.send_command(cmd, Callback::write(cb), RaftCmdExtraOpts::default())
            {
                resp.set_error(e.into());
                return Ok(resp);
            }

            let mut res = future.await.map_err(Error::from)?;
            let mut header = res.response.take_header();
            if header.has_error() {
                pb_error_inc(label, header.get_error());
                resp.set_error(header.take_error());
            }
            Ok(resp)
        }
    }
}

#[macro_export]
macro_rules! impl_write {
    ($fn:ident, $req_ty:ident, $resp_ty:ident, $chunk_ty:ident, $writer_fn:ident) => {
        fn $fn(
            &mut self,
            _ctx: RpcContext<'_>,
            stream: RequestStream<$req_ty>,
            sink: ClientStreamingSink<$resp_ty>,
        ) {
            let import = self.importer.clone();
            let engine = self.engine.clone();
            let (rx, buf_driver) =
                create_stream_with_buffer(stream, self.cfg.stream_channel_window);
            let mut rx = rx.map_err(Error::from);

            let timer = Instant::now_coarse();
            let label = stringify!($fn);
            let handle_task = async move {
                let res = async move {
                    let first_req = rx.try_next().await?;
                    let meta = match first_req {
                        Some(r) => match r.chunk {
                            Some($chunk_ty::Meta(m)) => m,
                            _ => return Err(Error::InvalidChunk),
                        },
                        _ => return Err(Error::InvalidChunk),
                    };

                    let writer = match import.$writer_fn(&engine, meta) {
                        Ok(w) => w,
                        Err(e) => {
                            error!("build writer failed {:?}", e);
                            return Err(Error::InvalidChunk);
                        }
                    };
                    let writer = rx
                        .try_fold(writer, |mut writer, req| async move {
                            let batch = match req.chunk {
                                Some($chunk_ty::Batch(b)) => b,
                                _ => return Err(Error::InvalidChunk),
                            };
                            writer.write(batch)?;
                            Ok(writer)
                        })
                        .await?;

                    let metas = writer.finish()?;
                    import.verify_checksum(&metas)?;
                    let mut resp = $resp_ty::default();
                    resp.set_metas(metas.into());
                    Ok(resp)
                }
                .await;
                crate::send_rpc_response!(res, sink, label, timer);
            };

            self.threads.spawn_ok(buf_driver);
            self.threads.spawn_ok(handle_task);
        }
    };
}

impl<E, Router> ImportSst for ImportSstService<E, Router>
where
    E: KvEngine,
    Router: 'static + RaftStoreRouter<E>,
{
    fn switch_mode(
        &mut self,
        ctx: RpcContext<'_>,
        req: SwitchModeRequest,
        sink: UnarySink<SwitchModeResponse>,
    ) {
        let label = "switch_mode";
        let timer = Instant::now_coarse();

        let res = {
            fn mf(cf: &str, name: &str, v: f64) {
                CONFIG_ROCKSDB_GAUGE.with_label_values(&[cf, name]).set(v);
            }

            match req.get_mode() {
                SwitchMode::Normal => self.importer.enter_normal_mode(self.engine.clone(), mf),
                SwitchMode::Import => self.importer.enter_import_mode(self.engine.clone(), mf),
            }
        };
        match res {
            Ok(_) => info!("switch mode"; "mode" => ?req.get_mode()),
            Err(ref e) => error!(%*e; "switch mode failed"; "mode" => ?req.get_mode(),),
        }

        let task = async move {
            let res = Ok(SwitchModeResponse::default());
            crate::send_rpc_response!(res, sink, label, timer);
        };
        ctx.spawn(task);
    }

    /// Receive SST from client and save the file for later ingesting.
    fn upload(
        &mut self,
        _ctx: RpcContext<'_>,
        stream: RequestStream<UploadRequest>,
        sink: ClientStreamingSink<UploadResponse>,
    ) {
        let label = "upload";
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let (rx, buf_driver) = create_stream_with_buffer(stream, self.cfg.stream_channel_window);
        let mut map_rx = rx.map_err(Error::from);

        let handle_task = async move {
            // So stream will not be dropped until response is sent.
            let rx = &mut map_rx;
            let res = async move {
                let first_chunk = rx.try_next().await?;
                let meta = match first_chunk {
                    Some(ref chunk) if chunk.has_meta() => chunk.get_meta(),
                    _ => return Err(Error::InvalidChunk),
                };
                let file = import.create(meta)?;
                let mut file = rx
                    .try_fold(file, |mut file, chunk| async move {
                        let start = Instant::now_coarse();
                        let data = chunk.get_data();
                        if data.is_empty() {
                            return Err(Error::InvalidChunk);
                        }
                        file.append(data)?;
                        IMPORT_UPLOAD_CHUNK_BYTES.observe(data.len() as f64);
                        IMPORT_UPLOAD_CHUNK_DURATION.observe(start.saturating_elapsed_secs());
                        Ok(file)
                    })
                    .await?;
                file.finish().map(|_| UploadResponse::default())
            }
            .await;
            crate::send_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(buf_driver);
        self.threads.spawn_ok(handle_task);
    }

    // clear_files the KV files after apply finished.
    // it will remove the direcotry in import path.
    fn clear_files(
        &mut self,
        _ctx: RpcContext<'_>,
        req: ClearRequest,
        sink: UnarySink<ClearResponse>,
    ) {
        let label = "clear_files";
        let timer = Instant::now_coarse();
        let importer = Arc::clone(&self.importer);
        let start = Instant::now();
        let mut resp = ClearResponse::default();

        let handle_task = async move {
            // Records how long the apply task waits to be scheduled.
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&["queue"])
                .observe(start.saturating_elapsed().as_secs_f64());

            if let Err(e) = importer.remove_dir(req.get_prefix()) {
                let mut import_err = kvproto::import_sstpb::Error::default();
                import_err.set_message(format!("failed to remove directory: {}", e));
                resp.set_error(import_err);
            }
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&[label])
                .observe(start.saturating_elapsed().as_secs_f64());

            let resp = Ok(resp);
            crate::send_rpc_response!(resp, sink, label, timer);
        };
        self.threads.spawn_ok(handle_task);
    }

    // Downloads KV file and performs key-rewrite then apply kv into this tikv store.
    fn apply(
        &mut self,
        _ctx: RpcContext<'_>,
        mut req: ApplyRequest,
        sink: UnarySink<ApplyResponse>,
    ) {
        let label = "apply";
        let timer = Instant::now_coarse();
        let importer = Arc::clone(&self.importer);
        let router = self.router.clone();
        let limiter = self.limiter.clone();
        let start = Instant::now();
        let raft_size = self.raft_entry_max_size;

        let handle_task = async move {
            // Records how long the apply task waits to be scheduled.
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&["queue"])
                .observe(start.saturating_elapsed().as_secs_f64());

            let mut futs = vec![];
            let mut apply_resp = ApplyResponse::default();
            let context = req.take_context();
            let meta = req.get_meta();

            let result = (|| -> Result<()> {
                let temp_file =
                    importer.do_download_kv_file(meta, req.get_storage_backend(), &limiter)?;
                let mut reqs = HashMap::<Vec<u8>, (Request, u64)>::default();
                let mut cmd_reqs = vec![];
                let mut build_req_fn = Self::build_apply_request(
                    raft_size.0,
                    &mut reqs,
                    cmd_reqs.as_mut(),
                    meta.get_is_delete(),
                    meta.get_cf(),
                    context.clone(),
                );
                let range = importer.do_apply_kv_file(
                    meta.get_start_key(),
                    meta.get_end_key(),
                    meta.get_restore_ts(),
                    temp_file,
                    req.get_rewrite_rule(),
                    &mut build_req_fn,
                )?;
                drop(build_req_fn);
                if !reqs.is_empty() {
                    let cmd = make_request(&mut reqs, context);
                    cmd_reqs.push(cmd);
                }
                for cmd in cmd_reqs {
                    let (cb, future) = paired_future_callback();
                    match router.send_command(cmd, Callback::write(cb), RaftCmdExtraOpts::default())
                    {
                        Ok(_) => futs.push(future),
                        Err(e) => {
                            let mut import_err = kvproto::import_sstpb::Error::default();
                            import_err.set_message(format!("failed to send raft command: {}", e));
                            apply_resp.set_error(import_err);
                        }
                    }
                }
                if let Some(r) = range {
                    apply_resp.set_range(r);
                }
                Ok(())
            })();
            if let Err(e) = result {
                apply_resp.set_error(e.into());
            }

            let resp = Ok(join_all(futs).await.iter().fold(apply_resp, |mut resp, x| {
                match x {
                    Err(e) => {
                        let mut import_err = kvproto::import_sstpb::Error::default();
                        import_err.set_message(format!("failed to complete raft command: {}", e));
                        resp.set_error(import_err);
                    }
                    Ok(r) => {
                        if r.response.get_header().has_error() {
                            let mut import_err = kvproto::import_sstpb::Error::default();
                            let err = r.response.get_header().get_error();
                            import_err
                                .set_message("failed to complete raft command".to_string());
                            // FIXME: if there are many errors, we may lose some of them here.
                            import_err
                                .set_store_error(err.clone());
                            warn!("failed to apply the file to the store"; "error" => ?err, "file" => %meta.get_name());
                            resp.set_error(import_err);
                        }
                    }
                }
                resp
            }));
            // Records how long the apply task waits to be scheduled.
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&["finish"])
                .observe(start.saturating_elapsed().as_secs_f64());
            debug!("finished apply kv file with {:?}", resp);
            crate::send_rpc_response!(resp, sink, label, timer);
        };
        self.threads.spawn_ok(handle_task);
    }

    /// Downloads the file and performs key-rewrite for later ingesting.
    fn download(
        &mut self,
        _ctx: RpcContext<'_>,
        req: DownloadRequest,
        sink: UnarySink<DownloadResponse>,
    ) {
        let label = "download";
        let timer = Instant::now_coarse();
        let importer = Arc::clone(&self.importer);
        let limiter = self.limiter.clone();
        let engine = self.engine.clone();
        let start = Instant::now();

        let handle_task = async move {
            // Records how long the download task waits to be scheduled.
            sst_importer::metrics::IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["queue"])
                .observe(start.saturating_elapsed().as_secs_f64());

            // FIXME: download() should be an async fn, to allow BR to cancel
            // a download task.
            // Unfortunately, this currently can't happen because the S3Storage
            // is not Send + Sync. See the documentation of S3Storage for reason.
            let cipher = req
                .cipher_info
                .to_owned()
                .into_option()
                .filter(|c| c.cipher_type != EncryptionMethod::Plaintext);

            let res = importer.download::<E>(
                req.get_sst(),
                req.get_storage_backend(),
                req.get_name(),
                req.get_rewrite_rule(),
                cipher,
                limiter,
                engine,
            );
            let mut resp = DownloadResponse::default();
            match res {
                Ok(range) => match range {
                    Some(r) => resp.set_range(r),
                    None => resp.set_is_empty(true),
                },
                Err(e) => resp.set_error(e.into()),
            }
            let resp = Ok(resp);
            crate::send_rpc_response!(resp, sink, label, timer);
        };

        self.threads.spawn_ok(handle_task);
    }

    /// Ingest the file by sending a raft command to raftstore.
    ///
    /// If the ingestion fails because the region is not found or the epoch does
    /// not match, the remaining files will eventually be cleaned up by
    /// CleanupSSTWorker.
    fn ingest(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: IngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        let label = "ingest";
        let timer = Instant::now_coarse();

        let mut resp = IngestResponse::default();
        if let Some(errorpb) = self.check_write_stall() {
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }

        let mut errorpb = errorpb::Error::default();
        if !Self::acquire_lock(&self.task_slots, req.get_sst()).unwrap_or(false) {
            errorpb.set_message(Error::FileConflict.to_string());
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }

        let task_slots = self.task_slots.clone();
        let meta = req.take_sst();
        let f = self.ingest_files(req.take_context(), label, vec![meta.clone()]);
        let handle_task = async move {
            let res = f.await;
            Self::release_lock(&task_slots, &meta).unwrap();
            crate::send_rpc_response!(res, sink, label, timer);
        };
        self.threads.spawn_ok(handle_task);
    }

    /// Ingest multiple files by sending a raft command to raftstore.
    ///
    fn multi_ingest(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: MultiIngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        let label = "multi-ingest";
        let timer = Instant::now_coarse();

        let mut resp = IngestResponse::default();
        if let Some(errorpb) = self.check_write_stall() {
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }

        let mut errorpb = errorpb::Error::default();
        let mut metas = vec![];
        for sst in req.get_ssts() {
            if Self::acquire_lock(&self.task_slots, sst).unwrap_or(false) {
                metas.push(sst.clone());
            }
        }
        if metas.len() < req.get_ssts().len() {
            for m in metas {
                Self::release_lock(&self.task_slots, &m).unwrap();
            }
            errorpb.set_message(Error::FileConflict.to_string());
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }
        let task_slots = self.task_slots.clone();
        let f = self.ingest_files(req.take_context(), label, req.take_ssts().into());
        let handle_task = async move {
            let res = f.await;
            for m in metas {
                Self::release_lock(&task_slots, &m).unwrap();
            }
            crate::send_rpc_response!(res, sink, label, timer);
        };
        self.threads.spawn_ok(handle_task);
    }

    fn compact(
        &mut self,
        _ctx: RpcContext<'_>,
        req: CompactRequest,
        sink: UnarySink<CompactResponse>,
    ) {
        let label = "compact";
        let timer = Instant::now_coarse();
        let engine = self.engine.clone();

        let handle_task = async move {
            let (start, end) = if !req.has_range() {
                (None, None)
            } else {
                (
                    Some(req.get_range().get_start()),
                    Some(req.get_range().get_end()),
                )
            };
            let output_level = if req.get_output_level() == -1 {
                None
            } else {
                Some(req.get_output_level())
            };

            let res = engine.compact_files_in_range(start, end, output_level);
            match res {
                Ok(_) => info!(
                    "compact files in range";
                    "start" => start.map(log_wrappers::Value::key),
                    "end" => end.map(log_wrappers::Value::key),
                    "output_level" => ?output_level, "takes" => ?timer.saturating_elapsed()
                ),
                Err(ref e) => error!(%*e;
                    "compact files in range failed";
                    "start" => start.map(log_wrappers::Value::key),
                    "end" => end.map(log_wrappers::Value::key),
                    "output_level" => ?output_level,
                ),
            }
            let res = res
                .map_err(|e| Error::Engine(box_err!(e)))
                .map(|_| CompactResponse::default());
            crate::send_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(handle_task);
    }

    fn set_download_speed_limit(
        &mut self,
        ctx: RpcContext<'_>,
        req: SetDownloadSpeedLimitRequest,
        sink: UnarySink<SetDownloadSpeedLimitResponse>,
    ) {
        let label = "set_download_speed_limit";
        let timer = Instant::now_coarse();

        let speed_limit = req.get_speed_limit();
        self.limiter.set_speed_limit(if speed_limit > 0 {
            speed_limit as f64
        } else {
            f64::INFINITY
        });

        let ctx_task = async move {
            let res = Ok(SetDownloadSpeedLimitResponse::default());
            crate::send_rpc_response!(res, sink, label, timer);
        };

        ctx.spawn(ctx_task);
    }

    fn duplicate_detect(
        &mut self,
        _ctx: RpcContext<'_>,
        mut request: DuplicateDetectRequest,
        mut sink: ServerStreamingSink<DuplicateDetectResponse>,
    ) {
        let label = "duplicate_detect";
        let timer = Instant::now_coarse();
        let context = request.take_context();
        let router = self.router.clone();
        let start_key = request.take_start_key();
        let min_commit_ts = request.get_min_commit_ts();
        let end_key = if request.get_end_key().is_empty() {
            None
        } else {
            Some(request.take_end_key())
        };
        let key_only = request.get_key_only();
        let snap_res = Self::async_snapshot(router, make_request_header(context));
        let handle_task = async move {
            let res = snap_res.await;
            let snapshot = match res {
                Ok(snap) => snap.snapshot,
                Err(e) => {
                    let mut resp = DuplicateDetectResponse::default();
                    pb_error_inc(label, &e);
                    resp.set_region_error(e);
                    match sink
                        .send((resp, WriteFlags::default().buffer_hint(true)))
                        .await
                    {
                        Ok(_) => {
                            IMPORT_RPC_DURATION
                                .with_label_values(&[label, "ok"])
                                .observe(timer.saturating_elapsed_secs());
                        }
                        Err(e) => {
                            warn!(
                                "connection send message fail";
                                "err" => %e
                            );
                        }
                    }
                    let _ = sink.close().await;
                    return;
                }
            };
            let detector =
                DuplicateDetector::new(snapshot, start_key, end_key, min_commit_ts, key_only)
                    .unwrap();
            for resp in detector {
                if let Err(e) = sink
                    .send((resp, WriteFlags::default().buffer_hint(true)))
                    .await
                {
                    warn!(
                        "connection send message fail";
                        "err" => %e
                    );
                    break;
                }
            }
            let _ = sink.close().await;
        };
        self.threads.spawn_ok(handle_task);
    }

    impl_write!(write, WriteRequest, WriteResponse, Chunk, new_txn_writer);

    impl_write!(
        raw_write,
        RawWriteRequest,
        RawWriteResponse,
        RawChunk,
        new_raw_writer
    );
}

// add error statistics from pb error response
fn pb_error_inc(type_: &str, e: &errorpb::Error) {
    let label = if e.has_not_leader() {
        "not_leader"
    } else if e.has_store_not_match() {
        "store_not_match"
    } else if e.has_region_not_found() {
        "region_not_found"
    } else if e.has_key_not_in_region() {
        "key_not_in_range"
    } else if e.has_epoch_not_match() {
        "epoch_not_match"
    } else if e.has_server_is_busy() {
        "server_is_busy"
    } else if e.has_stale_command() {
        "stale_command"
    } else if e.has_raft_entry_too_large() {
        "raft_entry_too_large"
    } else {
        "unknown"
    };

    IMPORTER_ERROR_VEC.with_label_values(&[type_, label]).inc();
}

fn make_request_header(mut context: Context) -> RaftRequestHeader {
    let region_id = context.get_region_id();
    let mut header = RaftRequestHeader::default();
    header.set_peer(context.take_peer());
    header.set_region_id(region_id);
    header.set_region_epoch(context.take_region_epoch());
    header
}

fn make_request(reqs: &mut HashMap<Vec<u8>, (Request, u64)>, context: Context) -> RaftCmdRequest {
    let mut cmd = RaftCmdRequest::default();
    let mut header = make_request_header(context);
    // Set the UUID of header to prevent raftstore batching our requests.
    // The current `resolved_ts` observer assumes that each batch of request doesn't has
    // two writes to the same key. (Even with 2 different TS). That was true for normal cases
    // because the latches reject concurrency write to keys. However we have bypassed the latch layer :(
    header.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    cmd.set_header(header);
    cmd.set_requests(
        std::mem::take(reqs)
            .into_values()
            .map(|(req, _)| req)
            .collect::<Vec<Request>>()
            .into(),
    );
    cmd
}
