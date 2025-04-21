// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    convert::identity,
    sync::{Arc, Mutex},
    time::Duration,
};

use engine_traits::{CompactExt, CF_DEFAULT, CF_WRITE};
use file_system::{set_io_type, IoType};
use futures::{sink::SinkExt, stream::TryStreamExt, FutureExt, TryFutureExt};
use grpcio::{
    ClientStreamingSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink,
    UnarySink, WriteFlags,
};
use kvproto::{
    encryptionpb::EncryptionMethod,
    import_sstpb::{
        Error as ImportPbError, ImportSst, Range, RawWriteRequest_oneof_chunk as RawChunk,
        SuspendImportRpcRequest, SuspendImportRpcResponse, SwitchMode,
        WriteRequest_oneof_chunk as Chunk, *,
    },
    metapb::RegionEpoch,
};
use raftstore::{
    coprocessor::{RegionInfo, RegionInfoProvider},
    store::util::is_epoch_stale,
    RegionInfoAccessor,
};
use raftstore_v2::StoreMeta;
use rand::Rng;
use resource_control::{with_resource_limiter, ResourceGroupManager};
use sst_importer::{
    error_inc, metrics::*, sst_importer::DownloadExt, Config, ConfigManager, Error, Result,
    SstImporter,
};
use tikv_kv::{Engine, LocalTablets, Modify, WriteData};
use tikv_util::{
    config::ReadableSize,
    future::{create_stream_with_buffer, paired_future_callback},
    resizable_threadpool::{DeamonRuntimeHandle, ResizableRuntime},
    sys::{
        disk::{get_disk_status, DiskUsage},
        get_global_memory_usage, SysQuota,
    },
    time::{Instant, Limiter},
    HandyRwLock,
};
use tokio::time::sleep;
use txn_types::{Key, WriteRef, WriteType};

use super::{
    ingest::{async_snapshot, ingest, IngestLatch, SuspendDeadline},
    make_rpc_error, pb_error_inc, raft_writer,
};
use crate::{
    import::duplicate_detect::DuplicateDetector,
    send_rpc_response,
    server::CONFIG_ROCKSDB_GAUGE,
    storage::{self, errors::extract_region_error_from_error},
    tikv_util::sys::thread::ThreadBuildWrapper,
};

/// The concurrency of sending raft request for every `apply` requests.
/// This value `16` would mainly influence the speed of applying a huge file:
/// when we downloading the files into disk, loading all of them into memory may
/// lead to OOM. This would be able to back-pressure them.
/// (only log files greater than 16 * 7M = 112M would be throttled by this.)
/// NOTE: Perhaps add a memory quota for download to disk mode and get rid of
/// this value?
const REQUEST_WRITE_CONCURRENCY: usize = 16;
/// The extra bytes required by the wire encoding.
/// Generally, a field (and a embedded message) would introduce some extra
/// bytes. In detail, they are:
/// - 2 bytes for the request type (Tag+Value).
/// - 2 bytes for every string or bytes field (Tag+Length), they are:
/// .  + the key field
/// .  + the value field
/// .  + the CF field (None for CF_DEFAULT)
/// - 2 bytes for the embedded message field `PutRequest` (Tag+Length).
/// - 2 bytes for the request itself (which would be embedded into a
///   [`RaftCmdRequest`].)
/// In fact, the length field is encoded by varint, which may grow when the
/// content length is greater than 128, however when the length is greater than
/// 128, the extra 1~4 bytes can be ignored.
const WIRE_EXTRA_BYTES: usize = 12;
/// The interval of running the GC for
/// [`raft_writer::ThrottledTlsEngineWriter`]. There aren't too many items held
/// in the writer. So we can run the GC less frequently.
const WRITER_GC_INTERVAL: Duration = Duration::from_secs(300);
/// The max time of suspending requests.
/// This may save us from some client sending insane value to the server.
const SUSPEND_REQUEST_MAX_SECS: u64 = // 6h
    6 * 60 * 60;

const REJECT_SERVE_MEMORY_USAGE: u64 = 1024 * 1024 * 1024; //1G
// consider block cache and raft store. the memory usage will be
const HIGH_IMPORT_MEMORY_WATER_RATIO: f64 = 0.95;

/// Check if the system has enough resources for import tasks
async fn check_import_resources(mem_limit: u64) -> Result<()> {
    #[cfg(feature = "failpoints")]
    let mem_limit = (|| {
        fail_point!("mock_memory_limit", |t| {
            t.unwrap().parse::<u64>().unwrap()
        });
        mem_limit
    })();
    #[cfg(not(feature = "failpoints"))]
    let mem_limit = mem_limit;

    // these error(memory or disk) cannot be recover at a short time,
    // in case client retry immediately, sleep for a while
    async fn sleep_with_jitter() {
        let jitter = rand::thread_rng().gen_range(1000, 2000);
        tokio::time::sleep(Duration::from_millis(jitter)).await;
    }
    // Check disk space first
    if get_disk_status(0) != DiskUsage::Normal {
        sleep_with_jitter().await;
        return Err(Error::DiskSpaceNotEnough);
    }

    let usage = get_global_memory_usage();
    if mem_limit == 0 || mem_limit < usage {
        // make it through when cannot get correct memory
        warn!(
            "Memory limit isn't correct. skip next check, limit is {} bytes, usage is {} bytes",
            mem_limit, usage
        );
        return Ok(());
    }

    let available_memory = mem_limit - usage;
    let min_required_memory = std::cmp::min(
        REJECT_SERVE_MEMORY_USAGE,
        ((1.0 - HIGH_IMPORT_MEMORY_WATER_RATIO) * mem_limit as f64) as u64,
    );

    // Reject ONLY if BOTH:
    // - Available memory is below REJECT_SERVE_MEMORY_USAGE
    // - Memory usage ratio is 95%+
    if available_memory < min_required_memory {
        sleep_with_jitter().await;
        return Err(Error::ResourceNotEnough(format!(
            "Memory usage too high, usage: {} bytes, mem limit {} bytes",
            usage, mem_limit
        )));
    }
    Ok(())
}

fn transfer_error(err: storage::Error) -> ImportPbError {
    let mut e = ImportPbError::default();
    if let Some(region_error) = extract_region_error_from_error(&err) {
        e.set_store_error(region_error);
    }
    e.set_message(format!("failed to complete raft command: {:?}", err));
    e
}

fn convert_join_error(err: tokio::task::JoinError) -> ImportPbError {
    let mut e = ImportPbError::default();
    if err.is_cancelled() {
        e.set_message("task canceled, probably runtime is shutting down.".to_owned());
    }
    if err.is_panic() {
        e.set_message(format!("panicked! {}", err))
    }
    e
}

/// ImportSstService provides tikv-server with the ability to ingest SST files.
///
/// It saves the SST sent from client to a file and then sends a command to
/// raftstore to trigger the ingest process.
#[derive(Clone)]
pub struct ImportSstService<E: Engine> {
    cfg: ConfigManager,
    tablets: LocalTablets<E::Local>,
    engine: E,
    threads: DeamonRuntimeHandle,
    // threads_ref is for safely cleanning
    #[allow(dead_code)]
    threads_ref: Arc<Mutex<ResizableRuntime>>,
    importer: Arc<SstImporter<E::Local>>,
    limiter: Limiter,
    ingest_latch: Arc<IngestLatch>,
    raft_entry_max_size: ReadableSize,
    region_info_accessor: Arc<RegionInfoAccessor>,

    writer: raft_writer::ThrottledTlsEngineWriter,

    // it's some iff multi-rocksdb is enabled
    store_meta: Option<Arc<Mutex<StoreMeta<E::Local>>>>,
    resource_manager: Option<Arc<ResourceGroupManager>>,

    // When less than now, don't accept any requests.
    suspend: Arc<SuspendDeadline>,

    mem_limit: u64,
}

struct RequestCollector {
    max_raft_req_size: usize,

    /// Retain the last ts of each key in each request.
    /// This is used for write CF because resolved ts observer hates duplicated
    /// key in the same request.
    write_reqs: HashMap<Vec<u8>, (Modify, u64)>,
    /// Collector favor that simple collect all items, and it do not contains
    /// duplicated key-value. This is used for default CF.
    default_reqs: HashMap<Vec<u8>, Modify>,
    /// Size of all `Request`s.
    unpacked_size: usize,

    pending_writes: Vec<WriteData>,
}

impl RequestCollector {
    fn record_size_of_message(&mut self, size: usize) {
        // We make a raft command entry when we unpacked size grows to 7/8 of the max
        // raft entry size.
        //
        // Which means, if we don't add the extra bytes, when the amplification by the
        // extra bytes is greater than 8/7 (i.e. the average size of entry is
        // less than 70B), we may encounter the "raft entry is too large" error.
        self.unpacked_size += size + WIRE_EXTRA_BYTES;
    }

    fn release_message_of_size(&mut self, size: usize) {
        self.unpacked_size -= size + WIRE_EXTRA_BYTES;
    }

    fn new(max_raft_req_size: usize) -> Self {
        Self {
            max_raft_req_size,
            write_reqs: HashMap::default(),
            default_reqs: HashMap::default(),
            unpacked_size: 0,
            pending_writes: Vec::new(),
        }
    }

    fn accept_kv(&mut self, cf: &str, is_delete: bool, k: Vec<u8>, v: Vec<u8>) {
        debug!("Accepting KV."; "cf" => %cf,
            "key" => %log_wrappers::Value::key(&k),
            "value" => %log_wrappers::Value::key(&v));
        // Need to skip the empty key/value that could break the transaction or cause
        // data corruption. see details at https://github.com/pingcap/tiflow/issues/5468.
        if k.is_empty() || (!is_delete && v.is_empty()) {
            return;
        }
        // Filter out not supported CF.
        let cf = match cf {
            CF_WRITE => CF_WRITE,
            CF_DEFAULT => CF_DEFAULT,
            _ => return,
        };
        let m = if is_delete {
            Modify::Delete(cf, Key::from_encoded(k))
        } else {
            if cf == CF_WRITE && !write_needs_restore(&v) {
                return;
            }

            Modify::Put(cf, Key::from_encoded(k), v)
        };
        self.accept(cf, m);
    }

    /// check whether the unpacked size would exceed the max_raft_req_size after
    /// accepting the modify.
    fn should_send_batch_before_adding(&self, m: &Modify) -> bool {
        let message_size = m.size() + WIRE_EXTRA_BYTES;
        // If there isn't any records in the collector, and there is a huge modify, we
        // should give it a change to enter the collector. Or we may generate empty
        // batch.
        self.unpacked_size != 0 /* batched */
        && message_size + self.unpacked_size > self.max_raft_req_size /* exceed the max_raft_req_size */
    }

    // we need to remove duplicate keys in here, since
    // in https://github.com/tikv/tikv/blob/a401f78bc86f7e6ea6a55ad9f453ae31be835b55/components/resolved_ts/src/cmd.rs#L204
    // will panic if found duplicated entry during Vec<PutRequest>.
    fn accept(&mut self, cf: &str, m: Modify) {
        if self.should_send_batch_before_adding(&m) {
            self.pack_all();
        }

        let k = m.key();
        match cf {
            CF_WRITE => {
                let (encoded_key, ts) = match Key::split_on_ts_for(k.as_encoded()) {
                    Ok(k) => k,
                    Err(err) => {
                        warn!(
                            "key without ts, skipping";
                            "key" => %k,
                            "err" => %err
                        );
                        return;
                    }
                };
                if self
                    .write_reqs
                    .get(encoded_key)
                    .map(|(_, old_ts)| *old_ts < ts.into_inner())
                    .unwrap_or(true)
                {
                    self.record_size_of_message(m.size());
                    if let Some((v, _)) = self
                        .write_reqs
                        .insert(encoded_key.to_owned(), (m, ts.into_inner()))
                    {
                        self.release_message_of_size(v.size())
                    }
                }
            }
            CF_DEFAULT => {
                self.record_size_of_message(m.size());
                if let Some(v) = self.default_reqs.insert(k.as_encoded().clone(), m) {
                    self.release_message_of_size(v.size());
                }
            }
            _ => unreachable!(),
        }
    }

    #[cfg(test)]
    fn drain_unpacked_reqs(&mut self, cf: &str) -> Vec<Modify> {
        let res: Vec<Modify> = if cf == CF_DEFAULT {
            self.default_reqs.drain().map(|(_, m)| m).collect()
        } else {
            self.write_reqs.drain().map(|(_, (m, _))| m).collect()
        };
        for r in &res {
            self.release_message_of_size(r.size());
        }
        res
    }

    #[inline]
    fn drain_pending_writes(&mut self, take_unpacked: bool) -> std::vec::Drain<'_, WriteData> {
        if take_unpacked {
            self.pack_all();
        }
        self.pending_writes.drain(..)
    }

    fn pack_all(&mut self) {
        if self.unpacked_size == 0 {
            return;
        }
        // Set the UUID of header to prevent raftstore batching our requests.
        // The current `resolved_ts` observer assumes that each batch of request doesn't
        // has two writes to the same key. (Even with 2 different TS). That was true
        // for normal cases because the latches reject concurrency write to keys.
        // However we have bypassed the latch layer :(
        let mut reqs: Vec<_> = self.write_reqs.drain().map(|(_, (req, _))| req).collect();
        reqs.append(&mut self.default_reqs.drain().map(|(_, req)| req).collect());
        if reqs.is_empty() {
            debug_assert!(false, "attempt to pack an empty request");
            return;
        }
        let mut data = WriteData::from_modifies(reqs);
        data.set_avoid_batch(true);
        self.pending_writes.push(data);
        self.unpacked_size = 0;
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.pending_writes.is_empty() && self.unpacked_size == 0
    }
}

impl<E: Engine> ImportSstService<E> {
    pub fn new(
        cfg: Config,
        raft_entry_max_size: ReadableSize,
        engine: E,
        tablets: LocalTablets<E::Local>,
        importer: Arc<SstImporter<E::Local>>,
        store_meta: Option<Arc<Mutex<StoreMeta<E::Local>>>>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        region_info_accessor: Arc<RegionInfoAccessor>,
    ) -> Self {
        let eng = Arc::new(Mutex::new(engine.clone()));
        let create_tokio_runtime = move |thread_count: usize, thread_name: &str| {
            let props = tikv_util::thread_group::current_properties();
            let eng = eng.clone();
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(thread_count)
                .enable_all()
                .thread_name(thread_name)
                .with_sys_and_custom_hooks(
                    move || {
                        tikv_util::thread_group::set_properties(props.clone());
                        set_io_type(IoType::Import);
                        tikv_kv::set_tls_engine(eng.lock().unwrap().clone());
                    },
                    move || {
                        // SAFETY: we have set the engine at some lines above with type `E`.
                        unsafe { tikv_kv::destroy_tls_engine::<E>() };
                    },
                )
                .build()
        };

        let threads = ResizableRuntime::new(
            4,
            "impwkr",
            Box::new(create_tokio_runtime),
            Box::new(|_| ()),
        );

        let handle = threads.handle();
        let threads_clone = Arc::new(Mutex::new(threads));
        if let LocalTablets::Singleton(tablet) = &tablets {
            importer.start_switch_mode_check(&handle.clone(), Some(tablet.clone()));
        } else {
            importer.start_switch_mode_check(&handle.clone(), None);
        }
        let writer = raft_writer::ThrottledTlsEngineWriter::default();
        let gc_handle = writer.clone();
        handle.spawn(async move {
            while gc_handle.try_gc() {
                tokio::time::sleep(WRITER_GC_INTERVAL).await;
            }
        });
        let num_threads = cfg.num_threads;
        let cfg_mgr = ConfigManager::new(cfg, Arc::downgrade(&threads_clone));
        handle.spawn(Self::tick(importer.clone(), cfg_mgr.clone()));
        // Drop the initial pool to accept new tasks
        threads_clone.lock().unwrap().adjust_with(num_threads);

        let mem_limit = SysQuota::memory_limit_in_bytes();
        ImportSstService {
            cfg: cfg_mgr,
            tablets,
            threads: handle.clone(),
            threads_ref: threads_clone,
            engine,
            importer,
            limiter: Limiter::new(f64::INFINITY),
            ingest_latch: Arc::default(),
            raft_entry_max_size,
            region_info_accessor,
            writer,
            store_meta,
            resource_manager,
            suspend: Arc::default(),
            mem_limit,
        }
    }

    pub fn get_config_manager(&self) -> ConfigManager {
        self.cfg.clone()
    }

    async fn tick(importer: Arc<SstImporter<E::Local>>, cfg: ConfigManager) {
        loop {
            sleep(Duration::from_secs(10)).await;

            importer.update_config_memory_use_ratio(&cfg);
            importer.shrink_by_tick();
        }
    }

    async fn do_apply(
        mut req: ApplyRequest,
        importer: Arc<SstImporter<E::Local>>,
        writer: raft_writer::ThrottledTlsEngineWriter,
        limiter: Limiter,
        max_raft_size: usize,
    ) -> std::result::Result<Option<Range>, ImportPbError> {
        let mut range: Option<Range> = None;

        let mut collector = RequestCollector::new(max_raft_size / 2);
        let context = req.take_context();
        let mut metas = req.take_metas();
        let mut rules = req.take_rewrite_rules();
        // For compatibility with old requests.
        if req.has_meta() {
            metas.push(req.take_meta());
            rules.push(req.take_rewrite_rule());
        }
        let ext_storage = importer.auto_encrypt_local_file_if_needed(
            importer
                .external_storage_or_cache(req.get_storage_backend(), req.get_storage_cache_id())?,
        );

        let mut inflight_futures = VecDeque::new();

        let mut tasks = metas.iter().zip(rules.iter()).peekable();
        while let Some((meta, rule)) = tasks.next() {
            let buff = importer
                .download_kv_file(
                    meta,
                    ext_storage.clone(),
                    req.get_storage_backend(),
                    &limiter,
                    req.cipher_info.clone().take(),
                    req.master_keys.clone().to_vec(),
                )
                .await?;
            if let Some(mut r) = importer.do_apply_kv_file(
                meta.get_start_key(),
                meta.get_end_key(),
                meta.get_start_ts(),
                meta.get_restore_ts(),
                buff,
                rule,
                |k, v| collector.accept_kv(meta.get_cf(), meta.get_is_delete(), k, v),
            )? {
                if let Some(range) = range.as_mut() {
                    range.start = range.take_start().min(r.take_start());
                    range.end = range.take_end().max(r.take_end());
                } else {
                    range = Some(r);
                }
            }

            let is_last_task = tasks.peek().is_none();
            for w in collector.drain_pending_writes(is_last_task) {
                // Record the start of a task would greatly help us to inspect pending
                // tasks.
                APPLIER_EVENT.with_label_values(&["begin_req"]).inc();
                // SAFETY: we have registered the thread local storage engine into the thread
                // when creating them.
                let task = unsafe {
                    writer
                        .write::<E>(w, context.clone())
                        .map_err(transfer_error)
                };
                inflight_futures.push_back(
                    tokio::spawn(task)
                        .map_err(convert_join_error)
                        .map(|x| x.and_then(identity)),
                );
                if inflight_futures.len() >= REQUEST_WRITE_CONCURRENCY {
                    inflight_futures.pop_front().unwrap().await?;
                }
            }
        }
        assert!(collector.is_empty());
        futures::future::try_join_all(inflight_futures).await?;

        Ok(range)
    }
}

fn check_local_region_stale(
    region_id: u64,
    epoch: &RegionEpoch,
    local_region_info: Option<RegionInfo>,
) -> Result<()> {
    match local_region_info {
        Some(local_region_info) => {
            let local_region_epoch = local_region_info.region.region_epoch.unwrap();

            // when local region epoch is stale, client can retry write later
            if is_epoch_stale(&local_region_epoch, epoch) {
                return Err(Error::RequestTooNew(format!(
                    "request region {} is ahead of local region, local epoch {:?}, request epoch {:?}, please retry write later",
                    region_id, local_region_epoch, epoch
                )));
            }
            // when local region epoch is ahead, client need to rescan region from PD to get
            // latest region later
            if is_epoch_stale(epoch, &local_region_epoch) {
                return Err(Error::RequestTooOld(format!(
                    "request region {} is staler than local region, local epoch {:?}, request epoch {:?}",
                    region_id, local_region_epoch, epoch
                )));
            }

            // not match means to rescan
            Ok(())
        }
        None => {
            // when region not found, we can't tell whether it's stale or ahead, so we just
            // return the safest case
            Err(Error::RequestTooOld(format!(
                "region {} is not found",
                region_id
            )))
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
            let tablets = self.tablets.clone();
            let mem_limit = self.mem_limit;
            let region_info_accessor = self.region_info_accessor.clone();
            let (rx, buf_driver) =
                create_stream_with_buffer(stream, self.cfg.rl().stream_channel_window);
            let mut rx = rx.map_err(Error::from);

            let timer = Instant::now_coarse();
            let label = stringify!($fn);
            let resource_manager = self.resource_manager.clone();
            let handle_task = async move {
                let (res, rx) = async move {
                    let first_req = match rx.try_next().await {
                        Ok(r) => r,
                        Err(e) => return (Err(e), Some(rx)),
                    };
                    let (meta, resource_limiter, txn_source) = match first_req {
                        Some(r) => {
                            let limiter = resource_manager.as_ref().and_then(|m| {
                                m.get_background_resource_limiter(
                                    r.get_context()
                                        .get_resource_control_context()
                                        .get_resource_group_name(),
                                    r.get_context().get_request_source(),
                                )
                            });
                            let txn_source = r.get_context().get_txn_source();
                            match r.chunk {
                                Some($chunk_ty::Meta(m)) => (m, limiter, txn_source),
                                _ => return (Err(Error::InvalidChunk), Some(rx)),
                            }
                        }
                        _ => return (Err(Error::InvalidChunk), Some(rx)),
                    };
                    // wait the region epoch on this TiKV to catch up with the epoch
                    // in request, which comes from PD and represents the majority
                    // peers' status.
                    let region_id = meta.get_region_id();
                    let (cb, f) = paired_future_callback();
                    if let Err(e) = region_info_accessor
                        .find_region_by_id(region_id, cb)
                        .map_err(|e| {
                            // when region not found, we can't tell whether it's stale or ahead, so
                            // we just return the safest case
                            Error::RequestTooOld(format!(
                                "failed to find region {} err {:?}",
                                region_id, e
                            ))
                        })
                    {
                        return (Err(e), Some(rx));
                    };
                    let res = match f.await {
                        Ok(r) => r,
                        Err(e) => return (Err(From::from(e)), Some(rx)),
                    };
                    if let Err(e) =
                        check_local_region_stale(region_id, meta.get_region_epoch(), res)
                    {
                        return (Err(e), Some(rx));
                    };

                    let tablet = match tablets.get(region_id) {
                        Some(t) => t,
                        None => {
                            return (
                                Err(Error::RequestTooOld(format!(
                                    "region {} not found",
                                    region_id
                                ))),
                                Some(rx),
                            );
                        }
                    };
                    if let Err(e) = check_import_resources(mem_limit).await {
                        warn!("Write failed due to not enough resource {:?}", e);
                        return (Err(e), Some(rx));
                    }

                    let writer = match import.$writer_fn(&*tablet, meta, txn_source) {
                        Ok(w) => w,
                        Err(e) => {
                            error!("build writer failed {:?}", e);
                            return (Err(Error::InvalidChunk), Some(rx));
                        }
                    };
                    let result = rx
                        .try_fold(
                            (writer, resource_limiter),
                            |(mut writer, limiter), req| async move {
                                let batch = match req.chunk {
                                    Some($chunk_ty::Batch(b)) => b,
                                    _ => return Err(Error::InvalidChunk),
                                };
                                let f = async {
                                    writer.write(batch)?;
                                    Ok(writer)
                                };
                                with_resource_limiter(f, limiter.clone())
                                    .await
                                    .map(|w| (w, limiter))
                            },
                        )
                        .await;
                    let (writer, resource_limiter) = match result {
                        Ok(r) => r,
                        Err(e) => return (Err(e), None),
                    };

                    let finish_fn = async {
                        let metas = writer.finish()?;
                        import.verify_checksum(&metas)?;
                        Ok(metas)
                    };

                    let metas: Result<_> = with_resource_limiter(finish_fn, resource_limiter).await;
                    let metas = match metas {
                        Ok(r) => r,
                        Err(e) => return (Err(e), None),
                    };
                    let mut resp = $resp_ty::default();
                    resp.set_metas(metas.into());
                    (Ok(resp), None)
                }
                .await;
                $crate::send_rpc_response!(res, sink, label, timer);
                // don't drop rx before send response
                _ = rx;
            };

            self.threads.spawn(buf_driver);
            self.threads.spawn(handle_task);
        }
    };
}

impl<E: Engine> ImportSst for ImportSstService<E> {
    // Switch mode for v1 and v2 is quite different.
    //
    // For v1, once it enters import mode, all regions are in import mode as there's
    // only one kv rocksdb.
    //
    // V2 is different. The switch mode with import mode request carries a range
    // where only regions overlapped with the range can enter import mode.
    // And unlike v1, where some rocksdb configs will be changed when entering
    // import mode, the config of the rocksdb will not change when entering import
    // mode due to implementation complexity (a region's rocksdb can change
    // overtime due to snapshot, split, and merge, which brings some
    // implemention complexities). If it really needs, we will implement it in the
    // future.
    fn switch_mode(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SwitchModeRequest,
        sink: UnarySink<SwitchModeResponse>,
    ) {
        let label = "switch_mode";
        IMPORT_RPC_COUNT.with_label_values(&[label]).inc();
        let timer = Instant::now_coarse();

        let res = {
            fn mf(cf: &str, name: &str, v: f64) {
                CONFIG_ROCKSDB_GAUGE.with_label_values(&[cf, name]).set(v);
            }

            match &self.tablets {
                LocalTablets::Singleton(tablet) => match req.get_mode() {
                    SwitchMode::Normal => self.importer.enter_normal_mode(tablet.clone(), mf),
                    SwitchMode::Import => self.importer.enter_import_mode(tablet.clone(), mf),
                },
                LocalTablets::Registry(_) => {
                    if req.get_mode() == SwitchMode::Import {
                        if !req.get_ranges().is_empty() {
                            let ranges = req.take_ranges().to_vec();
                            self.importer.ranges_enter_import_mode(ranges);
                            Ok(true)
                        } else {
                            Err(sst_importer::Error::Engine(
                                "partitioned-raft-kv only support switch mode with range set"
                                    .into(),
                            ))
                        }
                    } else {
                        // case SwitchMode::Normal
                        if !req.get_ranges().is_empty() {
                            let ranges = req.take_ranges().to_vec();
                            self.importer.clear_import_mode_regions(ranges);
                            Ok(true)
                        } else {
                            Err(sst_importer::Error::Engine(
                                "partitioned-raft-kv only support switch mode with range set"
                                    .into(),
                            ))
                        }
                    }
                }
            }
        };
        match res {
            Ok(_) => info!("switch mode"; "mode" => ?req.get_mode()),
            Err(ref e) => error!(%*e; "switch mode failed"; "mode" => ?req.get_mode(),),
        }

        let task = async move {
            defer! { IMPORT_RPC_COUNT.with_label_values(&[label]).dec() }
            crate::send_rpc_response!(Ok(SwitchModeResponse::default()), sink, label, timer);
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
        let mem_limit = self.mem_limit;
        let (rx, buf_driver) =
            create_stream_with_buffer(stream, self.cfg.rl().stream_channel_window);
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
                if let Err(e) = check_import_resources(mem_limit).await {
                    warn!("Upload failed due to not enough resource {:?}", e);
                    return Err(e);
                }
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
                let mut import_err = ImportPbError::default();
                import_err.set_message(format!("failed to remove directory: {}", e));
                resp.set_error(import_err);
            }
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&[label])
                .observe(start.saturating_elapsed().as_secs_f64());
            crate::send_rpc_response!(Ok(resp), sink, label, timer);
        };
        self.threads.spawn(handle_task);
    }

    // Downloads KV file and performs key-rewrite then apply kv into this tikv
    // store.
    fn apply(&mut self, _ctx: RpcContext<'_>, req: ApplyRequest, sink: UnarySink<ApplyResponse>) {
        let label = "apply";
        IMPORT_RPC_COUNT.with_label_values(&[label]).inc();
        let start = Instant::now();
        let importer = self.importer.clone();
        let limiter = self.limiter.clone();
        let mem_limit = self.mem_limit;
        let max_raft_size = self.raft_entry_max_size.0 as usize;
        let applier = self.writer.clone();

        let handle_task = async move {
            defer! { IMPORT_RPC_COUNT.with_label_values(&[label]).dec() }
            // Records how long the apply task waits to be scheduled.
            sst_importer::metrics::IMPORTER_APPLY_DURATION
                .with_label_values(&["queue"])
                .observe(start.saturating_elapsed().as_secs_f64());

            let mut resp = ApplyResponse::default();
            match check_import_resources(mem_limit).await {
                Ok(()) => (),
                Err(e) => {
                    resp.set_error(e.into());
                    crate::send_rpc_response!(Ok(resp), sink, label, start);
                    return;
                }
            }

            match Self::do_apply(req, importer, applier, limiter, max_raft_size).await {
                Ok(Some(r)) => resp.set_range(r),
                Err(e) => resp.set_error(e),
                _ => {}
            }

            debug!("finished apply kv file with {:?}", resp);
            send_rpc_response!(Ok(resp), sink, label, start);
        };
        self.threads.spawn(handle_task);
    }

    /// Downloads the file and performs key-rewrite for later ingesting.
    fn download(
        &mut self,
        _ctx: RpcContext<'_>,
        req: DownloadRequest,
        sink: UnarySink<DownloadResponse>,
    ) {
        let label = "download";
        IMPORT_RPC_COUNT.with_label_values(&[label]).inc();
        let timer = Instant::now_coarse();
        let importer = Arc::clone(&self.importer);
        let limiter = self.limiter.clone();
        let mem_limit = self.mem_limit;
        let region_id = req.get_sst().get_region_id();
        let tablets = self.tablets.clone();
        let start = Instant::now();
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_background_resource_limiter(
                req.get_context()
                    .get_resource_control_context()
                    .get_resource_group_name(),
                req.get_context().get_request_source(),
            )
        });

        let handle_task = async move {
            defer! { IMPORT_RPC_COUNT.with_label_values(&[label]).dec() }
            // Records how long the download task waits to be scheduled.
            sst_importer::metrics::IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["queue"])
                .observe(start.saturating_elapsed().as_secs_f64());

            let mut resp = DownloadResponse::default();
            match check_import_resources(mem_limit).await {
                Ok(()) => (),
                Err(e) => {
                    resp.set_error(e.into());
                    crate::send_rpc_response!(Ok(resp), sink, label, timer);
                    return;
                }
            }

            // FIXME: download() should be an async fn, to allow BR to cancel
            // a download task.
            // Unfortunately, this currently can't happen because the S3Storage
            // is not Send + Sync. See the documentation of S3Storage for reason.
            let cipher = req
                .cipher_info
                .to_owned()
                .into_option()
                .filter(|c| c.cipher_type != EncryptionMethod::Plaintext);

            let tablet = match tablets.get(region_id) {
                Some(tablet) => tablet,
                None => {
                    let error = sst_importer::Error::Engine(box_err!(
                        "region {} not found, maybe it's not a replica of this store",
                        region_id
                    ));
                    let mut resp = DownloadResponse::default();
                    resp.set_error(error.into());
                    crate::send_rpc_response!(Ok(resp), sink, label, timer);
                    return;
                }
            };

            let res = with_resource_limiter(
                importer.download_ext(
                    req.get_sst(),
                    req.get_storage_backend(),
                    req.get_name(),
                    req.get_rewrite_rule(),
                    cipher,
                    limiter,
                    tablet.into_owned(),
                    DownloadExt::default()
                        .cache_key(req.get_storage_cache_id())
                        .req_type(req.get_request_type()),
                ),
                resource_limiter,
            );
            let mut resp = DownloadResponse::default();
            match res.await {
                Ok(range) => match range {
                    Some(r) => resp.set_range(r),
                    None => resp.set_is_empty(true),
                },
                Err(e) => resp.set_error(e.into()),
            }
            crate::send_rpc_response!(Ok(resp), sink, label, timer);
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
        _: RpcContext<'_>,
        mut req: IngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        let label = "ingest";
        IMPORT_RPC_COUNT.with_label_values(&[label]).inc();
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let engine = self.engine.clone();
        let suspend = self.suspend.clone();
        let tablets = self.tablets.clone();
        let store_meta = self.store_meta.clone();
        let ingest_latch = self.ingest_latch.clone();

        let handle_task = async move {
            defer! { IMPORT_RPC_COUNT.with_label_values(&[label]).dec() }
            let mut multi_ingest = MultiIngestRequest::default();
            multi_ingest.set_context(req.take_context());
            multi_ingest.mut_ssts().push(req.take_sst());
            let res = ingest(
                multi_ingest,
                engine,
                &suspend,
                &tablets,
                &store_meta,
                &import,
                &ingest_latch,
                label,
            )
            .await;
            crate::send_rpc_response!(res, sink, label, timer);
        };
        self.threads.spawn(handle_task);
    }

    /// Ingest multiple files by sending a raft command to raftstore.
    fn multi_ingest(
        &mut self,
        _: RpcContext<'_>,
        req: MultiIngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        let label = "multi-ingest";
        IMPORT_RPC_COUNT.with_label_values(&[label]).inc();
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let engine = self.engine.clone();
        let suspend = self.suspend.clone();
        let tablets = self.tablets.clone();
        let store_meta = self.store_meta.clone();
        let ingest_latch = self.ingest_latch.clone();

        let handle_task = async move {
            defer! { IMPORT_RPC_COUNT.with_label_values(&[label]).dec() }
            let res = ingest(
                req,
                engine,
                &suspend,
                &tablets,
                &store_meta,
                &import,
                &ingest_latch,
                label,
            )
            .await;
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
        let tablets = self.tablets.clone();

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

            let region_id = req.get_context().get_region_id();
            let tablet = match tablets.get(region_id) {
                Some(tablet) => tablet,
                None => {
                    let e = Error::Engine(format!("region {} not found", region_id).into());
                    crate::send_rpc_response!(Err(e), sink, label, timer);
                    return;
                }
            };

            let res = tablet.compact_files_in_range(start, end, output_level);
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
            crate::send_rpc_response!(
                Ok(SetDownloadSpeedLimitResponse::default()),
                sink,
                label,
                timer
            );
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
        let start_key = request.take_start_key();
        let min_commit_ts = request.get_min_commit_ts();
        let end_key = if request.get_end_key().is_empty() {
            None
        } else {
            Some(request.take_end_key())
        };
        let key_only = request.get_key_only();
        let snap_res = async_snapshot(&mut self.engine, &context);
        let handle_task = async move {
            let res = snap_res.await;
            let snapshot = match res {
                Ok(snap) => snap,
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
                            let _ = sink.close().await;
                        }
                        Err(e) => {
                            warn!(
                                "connection send message fail";
                                "err" => %e
                            );
                            let status =
                                RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", e));
                            let _ = sink.fail(status).await;
                        }
                    }
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
                    let status =
                        RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", e));
                    let _ = sink.fail(status).await;
                    return;
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

    fn suspend_import_rpc(
        &mut self,
        ctx: RpcContext<'_>,
        req: SuspendImportRpcRequest,
        sink: UnarySink<SuspendImportRpcResponse>,
    ) {
        let label = "suspend_import_rpc";
        let timer = Instant::now_coarse();

        if req.should_suspend_imports && req.get_duration_in_secs() > SUSPEND_REQUEST_MAX_SECS {
            ctx.spawn(async move {
                send_rpc_response!(Err(Error::Io(
                    std::io::Error::new(std::io::ErrorKind::InvalidInput,
                        format!("you are going to suspend the import RPCs too long. (for {} seconds, max acceptable duration is {} seconds)",
                        req.get_duration_in_secs(), SUSPEND_REQUEST_MAX_SECS)))), sink, label, timer);
            });
            return;
        }

        let suspended = if req.should_suspend_imports {
            info!("suspend incoming import RPCs."; "for_second" => req.get_duration_in_secs(), "caller" => req.get_caller());
            self.suspend
                .suspend_requests(Duration::from_secs(req.get_duration_in_secs()))
        } else {
            info!("allow incoming import RPCs."; "caller" => req.get_caller());
            self.suspend.allow_requests()
        };
        let mut resp = SuspendImportRpcResponse::default();
        resp.set_already_suspended(suspended);
        ctx.spawn(async move { send_rpc_response!(Ok(resp), sink, label, timer) });
    }
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
    use kvproto::{
        kvrpcpb::Context,
        metapb::{Region, RegionEpoch},
        raft_cmdpb::{RaftCmdRequest, Request},
    };
    use protobuf::{Message, SingularPtrField};
    use raft::StateRole::Follower;
    use raftstore::RegionInfo;
    use tikv_kv::{Modify, WriteData};
    use txn_types::{Key, TimeStamp, Write, WriteBatchFlags, WriteType};

    use crate::{
        import::sst_service::{check_local_region_stale, RequestCollector},
        server::raftkv,
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

    fn default_req(key: &[u8], val: &[u8], start_ts: u64) -> Modify {
        let (k, v) = default(key, val, start_ts);
        Modify::Put(CF_DEFAULT, Key::from_encoded(k), v)
    }

    fn write_req(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> Modify {
        let (k, v) = write(key, ty, commit_ts, start_ts);
        if ty == WriteType::Delete {
            Modify::Delete(CF_WRITE, Key::from_encoded(k))
        } else {
            Modify::Put(CF_WRITE, Key::from_encoded(k), v)
        }
    }

    #[test]
    fn test_build_request() {
        #[derive(Debug)]
        struct Case {
            cf: &'static str,
            is_delete: bool,
            mutations: Vec<(Vec<u8>, Vec<u8>)>,
            expected_reqs: Vec<Modify>,
        }

        fn run_case(c: &Case) {
            let mut collector = RequestCollector::new(1024);

            for (k, v) in c.mutations.clone() {
                collector.accept_kv(c.cf, c.is_delete, k, v);
            }
            let reqs = collector.drain_pending_writes(true);

            let mut req1: HashMap<_, _> = reqs
                .into_iter()
                .flat_map(|x| {
                    assert!(x.avoid_batch);
                    x.modifies.into_iter()
                })
                .map(|req| {
                    let key = req.key().to_owned();
                    (key, req)
                })
                .collect();
            for req in c.expected_reqs.iter() {
                let r = req1.remove(req.key());
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
        let mut request_collector = RequestCollector::new(102400);
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
            request_collector.accept(CF_WRITE, req);
        }
        let mut reqs: Vec<_> = request_collector.drain_unpacked_reqs(CF_WRITE);
        reqs.sort_by(|r1, r2| r1.key().cmp(r2.key()));
        assert_eq!(reqs, reqs_result);
        assert!(request_collector.is_empty());
    }

    #[test]
    fn test_request_collector_with_default_cf() {
        let mut request_collector = RequestCollector::new(102400);
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
            request_collector.accept(CF_DEFAULT, req);
        }
        let mut reqs: Vec<_> = request_collector.drain_unpacked_reqs(CF_DEFAULT);
        reqs.sort_by(|r1, r2| {
            let (k1, ts1) = Key::split_on_ts_for(r1.key().as_encoded()).unwrap();
            let (k2, ts2) = Key::split_on_ts_for(r2.key().as_encoded()).unwrap();

            k1.cmp(k2).then(ts1.cmp(&ts2))
        });
        assert_eq!(reqs, reqs_result);
        assert!(request_collector.is_empty());
    }

    fn convert_write_batch_to_request_raftkv1(ctx: &Context, batch: WriteData) -> RaftCmdRequest {
        let reqs: Vec<Request> = batch.modifies.into_iter().map(Into::into).collect();
        let txn_extra = batch.extra;
        let mut header = raftkv::new_request_header(ctx);
        if batch.avoid_batch {
            header.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
        }
        let mut flags = 0;
        if txn_extra.one_pc {
            flags |= WriteBatchFlags::ONE_PC.bits();
        }
        if txn_extra.allowed_in_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());
        cmd
    }

    fn fake_ctx() -> Context {
        let mut fake_ctx = Context::new();
        fake_ctx.set_region_id(42);
        fake_ctx.set_region_epoch({
            let mut e = RegionEpoch::new();
            e.set_version(1024);
            e.set_conf_ver(56);
            e
        });
        fake_ctx
    }

    #[test]
    fn test_collector_size() {
        let mut request_collector = RequestCollector::new(1024);

        for i in 0..100u8 {
            request_collector.accept(CF_DEFAULT, default_req(&i.to_ne_bytes(), b"egg", i as _));
        }

        let pws = request_collector.drain_pending_writes(true);
        for w in pws {
            let req_size = convert_write_batch_to_request_raftkv1(&fake_ctx(), w).compute_size();
            assert!(req_size < 1024, "{}", req_size);
        }
    }

    #[test]
    fn test_collector_huge_write_liveness() {
        let mut request_collector = RequestCollector::new(1024);
        for i in 0..100u8 {
            if i % 10 == 2 {
                // Inject some huge requests.
                request_collector.accept(
                    CF_DEFAULT,
                    default_req(&i.to_ne_bytes(), &[42u8; 1025], i as _),
                );
            } else {
                request_collector.accept(CF_DEFAULT, default_req(&i.to_ne_bytes(), b"egg", i as _));
            }
        }
        let pws = request_collector.drain_pending_writes(true);
        let mut total = 0;
        for w in pws {
            let req = convert_write_batch_to_request_raftkv1(&fake_ctx(), w);
            let req_size = req.compute_size();
            total += req.get_requests().len();
            assert!(req_size < 2048, "{}", req_size);
        }
        assert_eq!(total, 100);
    }

    #[test]
    fn test_collector_mid_size_write_no_exceed_max() {
        let mut request_collector = RequestCollector::new(1024);
        for i in 0..100u8 {
            if i % 10 == 2 {
                let huge_req = default_req(&i.to_ne_bytes(), &[42u8; 960], i as _);
                // Inject some huge requests.
                request_collector.accept(CF_DEFAULT, huge_req);
            } else {
                request_collector.accept(
                    CF_DEFAULT,
                    default_req(
                        &i.to_ne_bytes(),
                        b"noodles with beef, egg, bacon and spinach; in chicken soup",
                        i as _,
                    ),
                );
            }
        }
        let pws = request_collector.drain_pending_writes(true);
        let mut total = 0;
        for w in pws {
            let req = convert_write_batch_to_request_raftkv1(&fake_ctx(), w);
            let req_size = req.compute_size();
            total += req.get_requests().len();
            assert!(req_size < 1024, "{}", req_size);
        }
        assert_eq!(total, 100);
    }

    #[test]
    fn test_write_rpc_check_region_epoch() {
        let mut req_epoch = RegionEpoch {
            conf_ver: 10,
            version: 10,
            ..Default::default()
        };
        // test for region not found
        let result = check_local_region_stale(1, &req_epoch, None);
        assert!(result.is_err());
        // check error message contains "rescan region later", client will match this
        // string pattern
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("rescan region later")
        );

        let mut local_region_info = RegionInfo {
            region: Region {
                id: 1,
                region_epoch: SingularPtrField::some(req_epoch.clone()),
                ..Default::default()
            },
            role: Follower,
            buckets: 1,
        };
        // test the local region epoch is same as request
        let result = check_local_region_stale(1, &req_epoch, Some(local_region_info.clone()));
        result.unwrap();

        // test the local region epoch is ahead of request
        local_region_info
            .region
            .region_epoch
            .as_mut()
            .unwrap()
            .conf_ver = 11;
        let result = check_local_region_stale(1, &req_epoch, Some(local_region_info.clone()));
        assert!(result.is_err());
        // check error message contains "rescan region later", client will match this
        // string pattern
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("rescan region later")
        );

        req_epoch.conf_ver = 11;
        let result = check_local_region_stale(1, &req_epoch, Some(local_region_info.clone()));
        result.unwrap();

        // test the local region epoch is staler than request
        req_epoch.version = 12;
        let result = check_local_region_stale(1, &req_epoch, Some(local_region_info));
        assert!(result.is_err());
        // check error message contains "retry write later", client will match this
        // string pattern
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("retry write later")
        );
    }
}
