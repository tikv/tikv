// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use collections::HashSet;
use engine_traits::{KvEngine, CF_DEFAULT, CF_WRITE};
use file_system::{set_io_type, IoType};
use futures::{future::join_all, sink::SinkExt, stream::TryStreamExt, TryFutureExt};
use futures_executor::{ThreadPool, ThreadPoolBuilder};
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
use sst_importer::{
    error_inc, metrics::*, sst_importer::DownloadExt, sst_meta_to_path, Config, Error, Result,
    SstImporter,
};
use tikv_util::{
    config::ReadableSize,
    future::{create_stream_with_buffer, paired_future_callback},
    sys::thread::ThreadBuildWrapper,
    time::{Instant, Limiter},
};
use tokio::{runtime::Runtime, time::sleep};
use txn_types::{Key, WriteRef, WriteType};

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
    threads: Arc<Runtime>,
    // For now, PiTR cannot be executed in the tokio runtime because it is synchronous and may
    // blocks. (tokio is so strict... it panics if we do insane things like blocking in an async
    // context.)
    // We need to execute these code in a context which allows blocking.
    // FIXME: Make PiTR restore asynchronous. Get rid of this pool.
    block_threads: Arc<ThreadPool>,
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
        let threads = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.num_threads)
            .enable_all()
            .thread_name("sst-importer")
            .after_start_wrapper(move || {
                tikv_util::thread_group::set_properties(props.clone());
                tikv_alloc::add_thread_memory_accessor();
                set_io_type(IoType::Import);
            })
            .before_stop_wrapper(move || tikv_alloc::remove_thread_memory_accessor())
            .build()
            .unwrap();
        let props = tikv_util::thread_group::current_properties();
        let block_threads = ThreadPoolBuilder::new()
            .pool_size(cfg.num_threads)
            .name_prefix("sst-importer")
            .after_start_wrapper(move || {
                tikv_util::thread_group::set_properties(props.clone());
                tikv_alloc::add_thread_memory_accessor();
                set_io_type(IoType::Import);
            })
            .before_stop_wrapper(move || tikv_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();
        importer.start_switch_mode_check(threads.handle(), engine.clone());
        threads.spawn(Self::tick(importer.clone()));

        ImportSstService {
            cfg,
            engine,
            threads: Arc::new(threads),
            block_threads: Arc::new(block_threads),
            router,
            importer,
            limiter: Limiter::new(f64::INFINITY),
            task_slots: Arc::new(Mutex::new(HashSet::default())),
            raft_entry_max_size,
        }
    }

    async fn tick(importer: Arc<SstImporter>) {
        loop {
            sleep(Duration::from_secs(10)).await;
            importer.shrink_by_tick();
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
        if let Err(e) = router.send_command(cmd, Callback::read(cb), RaftCmdExtraOpts::default()) {
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
            match self.engine.get_sst_key_ranges(CF_WRITE, 0) {
                Ok(l0_sst_ranges) => {
                    warn!(
                        "sst ingest is too slow";
                        "sst_ranges" => ?l0_sst_ranges,
                    );
                }
                Err(e) => {
                    error!("get sst key ranges failed"; "err" => ?e);
                }
            }
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
                $crate::send_rpc_response!(res, sink, label, timer);
            };

            self.threads.spawn(buf_driver);
            self.threads.spawn(handle_task);
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

        self.threads.spawn(buf_driver);
        self.threads.spawn(handle_task);
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
        self.threads.spawn(handle_task);
    }

    // Downloads KV file and performs key-rewrite then apply kv into this tikv
    // store.
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
            let mut start_apply = Instant::now();
            let mut futs = vec![];
            let mut apply_resp = ApplyResponse::default();
            let context = req.take_context();
            let mut rules = req.take_rewrite_rules();
            let mut metas = req.take_metas();
            // For compatibility with old requests.
            if req.has_meta() {
                metas.push(req.take_meta());
                rules.push(req.take_rewrite_rule());
            }

            let result = (|| -> Result<()> {
                let mut cmd_reqs = vec![];
                let mut reqs_default = RequestCollector::from_cf(CF_DEFAULT);
                let mut reqs_write = RequestCollector::from_cf(CF_WRITE);
                let mut req_default_size = 0_u64;
                let mut req_write_size = 0_u64;
                let mut range: Option<Range> = None;
                let ext_storage = {
                    let inner = importer.wrap_kms(
                        importer.external_storage_or_cache(
                            req.get_storage_backend(),
                            req.get_storage_cache_id(),
                        )?,
                        false,
                    );
                    inner
                };

                for (i, meta) in metas.iter().enumerate() {
                    let (reqs, req_size) = if meta.get_cf() == CF_DEFAULT {
                        (&mut reqs_default, &mut req_default_size)
                    } else {
                        (&mut reqs_write, &mut req_write_size)
                    };

                    let mut build_req_fn = build_apply_request(
                        req_size,
                        raft_size.0,
                        reqs,
                        cmd_reqs.as_mut(),
                        meta.get_is_delete(),
                        meta.get_cf(),
                        context.clone(),
                    );

                    let buff = importer.read_from_kv_file(
                        meta,
                        &rules[i],
                        Arc::clone(&ext_storage),
                        req.get_storage_backend(),
                        &limiter,
                    )?;
                    let r: Option<Range> = importer.do_apply_kv_file(
                        meta.get_start_key(),
                        meta.get_end_key(),
                        meta.get_start_ts(),
                        meta.get_restore_ts(),
                        buff,
                        &mut build_req_fn,
                    )?;

                    if let Some(mut r) = r {
                        range = match range {
                            Some(mut v) => {
                                let s = v.take_start().min(r.take_start());
                                let e = v.take_end().max(r.take_end());
                                Some(Range {
                                    start: s,
                                    end: e,
                                    ..Default::default()
                                })
                            }
                            None => Some(r),
                        };
                    }
                }

                if !reqs_default.is_empty() {
                    let cmd = make_request(&mut reqs_default, context.clone());
                    cmd_reqs.push(cmd);
                    IMPORTER_APPLY_BYTES.observe(req_default_size as _);
                }
                if !reqs_write.is_empty() {
                    let cmd = make_request(&mut reqs_write, context);
                    cmd_reqs.push(cmd);
                    IMPORTER_APPLY_BYTES.observe(req_write_size as _);
                }

                start_apply = Instant::now();
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
                            import_err.set_message("failed to complete raft command".to_string());
                            // FIXME: if there are many errors, we may lose some of them here.
                            import_err.set_store_error(err.clone());
                            warn!("failed to apply the file to the store"; "error" => ?err);
                            resp.set_error(import_err);
                        }
                    }
                }
                resp
            }));

            // Records how long the apply task waits to be scheduled.
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&["apply"])
                .observe(start_apply.saturating_elapsed().as_secs_f64());
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&["finish"])
                .observe(start.saturating_elapsed().as_secs_f64());
            debug!("finished apply kv file with {:?}", resp);
            crate::send_rpc_response!(resp, sink, label, timer);
        };
        self.block_threads.spawn_ok(handle_task);
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

            let res = importer.download_ext::<E>(
                req.get_sst(),
                req.get_storage_backend(),
                req.get_name(),
                req.get_rewrite_rule(),
                cipher,
                limiter,
                engine,
                DownloadExt::default().cache_key(req.get_storage_cache_id()),
            );
            let mut resp = DownloadResponse::default();
            match res.await {
                Ok(range) => match range {
                    Some(r) => resp.set_range(r),
                    None => resp.set_is_empty(true),
                },
                Err(e) => resp.set_error(e.into()),
            }
            let resp = Ok(resp);
            crate::send_rpc_response!(resp, sink, label, timer);
        };

        self.threads.spawn(handle_task);
    }

    /// Ingest the file by sending a raft command to raftstore.
    ///
    /// If the ingestion fails because the region is not found or the epoch does
    /// not match, the remaining files will eventually be cleaned up by
    /// CleanupSstWorker.
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
        self.threads.spawn(handle_task);
    }

    /// Ingest multiple files by sending a raft command to raftstore.
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
        self.threads.spawn(handle_task);
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

        self.threads.spawn(handle_task);
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
        self.threads.spawn(handle_task);
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

enum RequestCollector {
    /// Retain the last ts of each key in each request.
    /// This is used for write CF because resolved ts observer hates duplicated
    /// key in the same request.
    RetainLastTs(HashMap<Vec<u8>, (Request, u64)>),
    /// Collector favor that simple collect all items, and it do not contains
    /// duplicated key-value. This is used for default CF.
    KeepAll(HashMap<Vec<u8>, Request>),
}

impl RequestCollector {
    fn from_cf(cf: &str) -> Self {
        match cf {
            CF_DEFAULT | "" => Self::KeepAll(Default::default()),
            CF_WRITE => Self::RetainLastTs(Default::default()),
            _ => {
                warn!("unknown cf name, using default request collector"; "cf" => %cf);
                Self::RetainLastTs(Default::default())
            }
        }
    }

    fn accept(&mut self, req: Request) {
        let k = key_from_request(&req);
        match self {
            RequestCollector::RetainLastTs(ref mut reqs) => {
                let (encoded_key, ts) = match Key::split_on_ts_for(k) {
                    Ok(k) => k,
                    Err(err) => {
                        warn!("key without ts, skipping"; "key" => %log_wrappers::Value::key(k), "err" => %err);
                        return;
                    }
                };
                if reqs
                    .get(encoded_key)
                    .map(|(_, old_ts)| *old_ts < ts.into_inner())
                    .unwrap_or(true)
                {
                    reqs.insert(encoded_key.to_owned(), (req, ts.into_inner()));
                }
            }
            RequestCollector::KeepAll(ref mut reqs) => {
                reqs.insert(k.to_owned(), req);
            }
        }
    }

    fn drain(&mut self) -> Vec<Request> {
        match self {
            RequestCollector::RetainLastTs(ref mut reqs) => {
                reqs.drain().map(|(_, (req, _))| req).collect()
            }
            RequestCollector::KeepAll(ref mut reqs) => reqs.drain().map(|(_, req)| req).collect(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            RequestCollector::RetainLastTs(reqs) => reqs.is_empty(),
            RequestCollector::KeepAll(reqs) => reqs.is_empty(),
        }
    }
}

fn key_from_request(req: &Request) -> &[u8] {
    if req.has_put() {
        return req.get_put().get_key();
    }
    if req.has_delete() {
        return req.get_delete().get_key();
    }
    warn!("trying to extract key from request is neither put nor delete.");
    b""
}

fn make_request_header(mut context: Context) -> RaftRequestHeader {
    let region_id = context.get_region_id();
    let mut header = RaftRequestHeader::default();
    header.set_peer(context.take_peer());
    header.set_region_id(region_id);
    header.set_region_epoch(context.take_region_epoch());
    header
}

fn make_request(reqs: &mut RequestCollector, context: Context) -> RaftCmdRequest {
    let mut cmd = RaftCmdRequest::default();
    let mut header = make_request_header(context);
    // Set the UUID of header to prevent raftstore batching our requests.
    // The current `resolved_ts` observer assumes that each batch of request doesn't
    // has two writes to the same key. (Even with 2 different TS). That was true
    // for normal cases because the latches reject concurrency write to keys.
    // However we have bypassed the latch layer :(
    header.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    cmd.set_header(header);
    cmd.set_requests(reqs.drain().into());
    cmd
}

// we need to remove duplicate keys in here, since
// in https://github.com/tikv/tikv/blob/a401f78bc86f7e6ea6a55ad9f453ae31be835b55/components/resolved_ts/src/cmd.rs#L204
// will panic if found duplicated entry during Vec<PutRequest>.
fn build_apply_request<'a, 'b>(
    req_size: &'a mut u64,
    raft_size: u64,
    reqs: &'a mut RequestCollector,
    cmd_reqs: &'a mut Vec<RaftCmdRequest>,
    is_delete: bool,
    cf: &'b str,
    context: Context,
) -> Box<dyn FnMut(Vec<u8>, Vec<u8>) + 'b>
where
    'a: 'b,
{
    // use callback to collect kv data.
    Box::new(move |k: Vec<u8>, v: Vec<u8>| {
        // Need to skip the empty key/value that could break the transaction or cause
        // data corruption. see details at https://github.com/pingcap/tiflow/issues/5468.
        if k.is_empty() || (!is_delete && v.is_empty()) {
            return;
        }

        let mut req = Request::default();
        if is_delete {
            let mut del = DeleteRequest::default();
            del.set_key(k);
            del.set_cf(cf.to_string());
            req.set_cmd_type(CmdType::Delete);
            req.set_delete(del);
        } else {
            if cf == CF_WRITE && !write_needs_restore(&v) {
                return;
            }

            let mut put = PutRequest::default();
            put.set_key(k);
            put.set_value(v);
            put.set_cf(cf.to_string());
            req.set_cmd_type(CmdType::Put);
            req.set_put(put);
        }

        // When the request size get grow to max request size,
        // build the request and add it to a batch.
        if *req_size + req.compute_size() as u64 > raft_size * 7 / 8 {
            IMPORTER_APPLY_BYTES.observe(*req_size as _);
            *req_size = 0;
            let cmd = make_request(reqs, context.clone());
            cmd_reqs.push(cmd);
        }

        *req_size += req.compute_size() as u64;
        reqs.accept(req);
    })
}

fn write_needs_restore(write: &[u8]) -> bool {
    let w = WriteRef::parse(write);
    match w {
        Ok(w)
            if matches!(
                w.write_type,
                // We only keep the last put / delete write CF,
                // other write type may shadow the real data and cause data loss.
                WriteType::Put | WriteType::Delete
            ) =>
        {
            true
        }
        Ok(w) => {
            debug!("skip unnecessary write."; "type" => ?w.write_type);
            false
        }
        Err(err) => {
            warn!("write cannot be parsed, skipping"; "err" => %err, 
                        "write" => %log_wrappers::Value::key(write));
            false
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use engine_traits::{CF_DEFAULT, CF_WRITE};
    use kvproto::{kvrpcpb::Context, raft_cmdpb::*};
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use crate::import::sst_service::{
        build_apply_request, key_from_request, make_request, RequestCollector,
    };

    fn write(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> (Vec<u8>, Vec<u8>) {
        let k = Key::from_raw(key).append_ts(TimeStamp::new(commit_ts));
        let v = Write::new(ty, TimeStamp::new(start_ts), None);
        (k.into_encoded(), v.as_ref().to_bytes())
    }

    fn default(key: &[u8], val: &[u8], start_ts: u64) -> (Vec<u8>, Vec<u8>) {
        let k = Key::from_raw(key).append_ts(TimeStamp::new(start_ts));
        (k.into_encoded(), val.to_owned())
    }

    fn default_req(key: &[u8], val: &[u8], start_ts: u64) -> Request {
        let (k, v) = default(key, val, start_ts);
        req(k, v, CF_DEFAULT, CmdType::Put)
    }

    fn write_req(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> Request {
        let (k, v) = write(key, ty, commit_ts, start_ts);
        let cmd_type = if ty == WriteType::Delete {
            CmdType::Delete
        } else {
            CmdType::Put
        };

        req(k, v, CF_WRITE, cmd_type)
    }

    fn req(k: Vec<u8>, v: Vec<u8>, cf: &str, cmd_type: CmdType) -> Request {
        let mut req = Request::default();
        req.set_cmd_type(cmd_type);

        match cmd_type {
            CmdType::Put => {
                let mut put = PutRequest::default();
                put.set_key(k);
                put.set_value(v);
                put.set_cf(cf.to_string());

                req.set_put(put)
            }
            CmdType::Delete => {
                let mut del = DeleteRequest::default();
                del.set_cf(cf.to_string());
                del.set_key(k);

                req.set_delete(del);
            }
            _ => panic!("invalid input cmd_type"),
        }
        req
    }

    #[test]
    fn test_build_request() {
        #[derive(Debug)]
        struct Case {
            cf: &'static str,
            is_delete: bool,
            mutations: Vec<(Vec<u8>, Vec<u8>)>,
            expected_reqs: Vec<Request>,
        }

        fn run_case(c: &Case) {
            let mut cmds = vec![];
            let mut reqs = RequestCollector::from_cf(c.cf);
            let mut req_size = 0_u64;

            let mut builder = build_apply_request(
                &mut req_size,
                1024,
                &mut reqs,
                &mut cmds,
                c.is_delete,
                c.cf,
                Context::new(),
            );

            for (k, v) in c.mutations.clone() {
                builder(k, v);
            }
            drop(builder);
            if !reqs.is_empty() {
                let cmd = make_request(&mut reqs, Context::new());
                cmds.push(cmd);
            }

            let mut req1: HashMap<_, _> = cmds
                .into_iter()
                .flat_map(|mut x| x.take_requests().into_iter())
                .map(|req| {
                    let key = key_from_request(&req).to_owned();
                    (key, req)
                })
                .collect();
            for req in c.expected_reqs.iter() {
                let r = req1.remove(key_from_request(req));
                assert_eq!(r.as_ref(), Some(req), "{:?}", c);
            }
            assert!(req1.is_empty(), "{:?}\ncase = {:?}", req1, c);
        }

        use WriteType::*;
        let cases = vec![
            Case {
                cf: CF_WRITE,
                is_delete: false,
                mutations: vec![
                    write(b"foo", Lock, 42, 41),
                    write(b"foo", Put, 40, 39),
                    write(b"bar", Put, 38, 37),
                    write(b"baz", Put, 34, 31),
                    write(b"bar", Put, 28, 17),
                    (Vec::default(), Vec::default()),
                ],
                expected_reqs: vec![
                    write_req(b"foo", Put, 40, 39),
                    write_req(b"bar", Put, 38, 37),
                    write_req(b"baz", Put, 34, 31),
                ],
            },
            Case {
                cf: CF_WRITE,
                is_delete: true,
                mutations: vec![
                    write(b"foo", Delete, 40, 39),
                    write(b"bar", Delete, 38, 37),
                    write(b"baz", Delete, 34, 31),
                    write(b"bar", Delete, 28, 17),
                ],
                expected_reqs: vec![
                    write_req(b"foo", Delete, 40, 39),
                    write_req(b"bar", Delete, 38, 37),
                    write_req(b"baz", Delete, 34, 31),
                ],
            },
            Case {
                cf: CF_DEFAULT,
                is_delete: false,
                mutations: vec![
                    default(b"aria", b"The planet where flowers bloom.", 123),
                    default(
                        b"aria",
                        b"Even a small breeze can still bring small happiness.",
                        178,
                    ),
                    default(b"beyond", b"Calling your name.", 278),
                    default(b"beyond", b"Calling your name.", 278),
                    default(b"PingCap", b"", 300),
                ],
                expected_reqs: vec![
                    default_req(b"aria", b"The planet where flowers bloom.", 123),
                    default_req(
                        b"aria",
                        b"Even a small breeze can still bring small happiness.",
                        178,
                    ),
                    default_req(b"beyond", b"Calling your name.", 278),
                ],
            },
        ];

        for case in cases {
            run_case(&case);
        }
    }

    #[test]
    fn test_request_collector_with_write_cf() {
        let mut request_collector = RequestCollector::from_cf(CF_WRITE);
        assert_eq!(request_collector.is_empty(), true);
        let reqs = vec![
            write_req(b"foo", WriteType::Put, 40, 39),
            write_req(b"aar", WriteType::Put, 38, 37),
            write_req(b"foo", WriteType::Put, 34, 31),
            write_req(b"zzz", WriteType::Put, 41, 40),
        ];
        let reqs_result = vec![
            write_req(b"aar", WriteType::Put, 38, 37),
            write_req(b"foo", WriteType::Put, 40, 39),
            write_req(b"zzz", WriteType::Put, 41, 40),
        ];

        for req in reqs {
            request_collector.accept(req);
        }
        assert_eq!(request_collector.is_empty(), false);
        let mut reqs = request_collector.drain();
        reqs.sort_by(|r1, r2| {
            let k1 = key_from_request(r1);
            let k2 = key_from_request(r2);
            k1.cmp(k2)
        });
        assert_eq!(reqs, reqs_result);
        assert_eq!(request_collector.is_empty(), true);
    }

    #[test]
    fn test_request_collector_with_default_cf() {
        let mut request_collector = RequestCollector::from_cf(CF_DEFAULT);
        assert_eq!(request_collector.is_empty(), true);
        let reqs = vec![
            default_req(b"foo", b"", 39),
            default_req(b"zzz", b"", 40),
            default_req(b"foo", b"", 37),
            default_req(b"foo", b"", 39),
        ];
        let reqs_result = vec![
            default_req(b"foo", b"", 37),
            default_req(b"foo", b"", 39),
            default_req(b"zzz", b"", 40),
        ];

        for req in reqs {
            request_collector.accept(req);
        }
        assert_eq!(request_collector.is_empty(), false);
        let mut reqs = request_collector.drain();
        reqs.sort_by(|r1, r2| {
            let k1 = key_from_request(r1);
            let (k1, ts1) = Key::split_on_ts_for(k1).unwrap();
            let k2 = key_from_request(r2);
            let (k2, ts2) = Key::split_on_ts_for(k2).unwrap();

            k1.cmp(k2).then(ts1.cmp(&ts2))
        });
        assert_eq!(reqs, reqs_result);
        assert_eq!(request_collector.is_empty(), true);
    }
}
