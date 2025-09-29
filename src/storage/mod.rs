// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]

//! This module contains TiKV's transaction layer. It lowers high-level,
//! transactional commands to low-level (raw key-value) interactions with
//! persistent storage.
//!
//! This module is further split into layers: [`txn`](txn) lowers transactional
//! commands to key-value operations on an MVCC abstraction. [`mvcc`](mvcc) is
//! our MVCC implementation. [`kv`](kv) is an abstraction layer over persistent
//! storage.
//!
//! Other responsibilities of this module are managing latches (see
//! [`latch`](txn::latch)), deadlock and wait handling (see
//! [`lock_manager`](lock_manager)), sche duling command execution (see
//! [`txn::scheduler`](txn::scheduler)), and handling commands from the raw and
//! versioned APIs (in the [`Storage`](Storage) struct).
//!
//! For more information about TiKV's transactions, see the [sig-txn docs](https://github.com/tikv/sig-transaction/tree/master/doc).
//!
//! Some important types are:
//!
//! * the [`Engine`](kv::Engine) trait and related traits, which abstracts over
//!   underlying storage,
//! * the [`MvccTxn`](mvcc::txn::MvccTxn) struct, which is the primary object in
//!   the MVCC implementation,
//! * the commands in the [`commands`](txn::commands) module, which are how each
//!   command is implemented,
//! * the [`Storage`](Storage) struct, which is the primary entry point for this
//!   module.
//!
//! Related code:
//!
//! * the [`kv`](crate::server::service::kv) module, which is the interface for
//!   TiKV's APIs,
//! * the [`lock_manager](crate::server::lock_manager), which takes part in lock
//!   and deadlock management,
//! * [`gc_worker`](crate::server::gc_worker), which drives garbage collection
//!   of old values,
//! * the [`txn_types](::txn_types) crate, some important types for this
//!   module's interface,
//! * the [`kvproto`](::kvproto) crate, which defines TiKV's protobuf API and
//!   includes some documentation of the commands implemented here,
//! * the [`test_storage`](::test_storage) crate, integration tests for this
//!   module,
//! * the [`engine_traits`](::engine_traits) crate, more detail of the engine
//!   abstraction.

pub mod config;
pub mod config_manager;
pub mod errors;
pub mod kv;
pub mod lock_manager;
pub(crate) mod metrics;
pub mod mvcc;
pub mod raw;
pub mod txn;

mod read_pool;
mod types;

use std::{
    assert_matches::assert_matches,
    borrow::Cow,
    iter,
    marker::PhantomData,
    mem,
    sync::{
        Arc,
        atomic::{self, AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use api_version::{ApiV1, ApiV2, KeyMode, KvFormat, RawValue};
use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use engine_traits::{
    CF_DEFAULT, CF_LOCK, CF_WRITE, CfName, DATA_CFS, DATA_CFS_LEN, raw_ttl::ttl_to_expire_ts,
};
use futures::{future::Either, prelude::*};
use kvproto::{
    kvrpcpb,
    kvrpcpb::{
        ApiVersion, ChecksumAlgorithm, CommandPri, Context, GetRequest, IsolationLevel, KeyRange,
        LockInfo, RawGetRequest,
    },
    pdpb::QueryKind,
};
use pd_client::FeatureGate;
use protobuf::Message;
use raftstore::store::{ReadStats, TxnExt, WriteStats, util::build_key_range};
use rand::prelude::*;
use resource_control::{ResourceController, ResourceGroupManager, ResourceLimiter, TaskMetadata};
use resource_metering::{
    FutureExt, ResourceTagFactory, record_logical_read_bytes, record_network_in_bytes,
    record_network_out_bytes,
};
use tikv_kv::{OnAppliedCb, SnapshotExt};
use tikv_util::{
    deadline::Deadline,
    future::try_poll,
    quota_limiter::QuotaLimiter,
    time::{Instant, InstantExt, ThreadReadId, duration_to_ms, duration_to_sec},
};
use tracker::{
    TlsTrackedFuture, TrackerToken, clear_tls_tracker_token, set_tls_tracker_token,
    with_tls_tracker,
};
use txn_types::{Key, KvPair, Lock, LockType, TimeStamp, TsSet, Value};

use self::kv::SnapContext;
pub use self::{
    errors::{Error, ErrorHeaderKind, ErrorInner, get_error_kind_from_header, get_tag_from_header},
    kv::{
        CfStatistics, Cursor, CursorBuilder, Engine, FlowStatistics, FlowStatsReporter, Iterator,
        RocksEngine, ScanMode, Snapshot, StageLatencyStats, Statistics, TestEngineBuilder,
    },
    raw::RawStore,
    read_pool::{build_read_pool, build_read_pool_for_test},
    txn::{Latches, Lock as LatchLock, ProcessResult, Scanner, SnapshotStore, Store},
    types::{
        PessimisticLockKeyResult, PessimisticLockResults, PrewriteResult, SecondaryLocksStatus,
        StorageCallback, TxnStatus,
    },
};
use crate::{
    read_pool::{ReadPool, ReadPoolHandle},
    server::{lock_manager::waiter_manager, metrics::ResourcePriority},
    storage::{
        config::Config,
        kv::{Modify, WriteData, with_tls_engine},
        lock_manager::{LockManager, MockLockManager},
        metrics::{CommandKind, *},
        mvcc::{MvccReader, PointGetterBuilder, metrics::ScanLockReadTimeSource::resolve_lock},
        test_util::latest_feature_gate,
        txn::{
            Command, Error as TxnError, ErrorInner as TxnErrorInner,
            commands::{RawAtomicStore, RawCompareAndSwap, TypedCommand},
            flow_controller::{EngineFlowController, FlowController},
            scheduler::TxnScheduler,
            txn_status_cache::{TxnState, TxnStatusCache},
        },
        types::StorageCallbackType,
    },
};

pub type Result<T> = std::result::Result<T, Error>;
pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

macro_rules! check_key_size {
    ($key_iter:expr, $max_key_size:expr, $callback:ident) => {
        for k in $key_iter {
            let key_size = k.len();
            if key_size > $max_key_size {
                $callback(Err(Error::from(ErrorInner::KeyTooLarge {
                    size: key_size,
                    limit: $max_key_size,
                })));
                return Ok(());
            }
        }
    };
}

/// Storage for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type StorageApiV1<E, L> = Storage<E, L, ApiV1>;

/// [`Storage`](Storage) implements transactional KV APIs and raw KV APIs on a
/// given [`Engine`]. An [`Engine`] provides low level KV functionality.
/// [`Engine`] has multiple implementations. When a TiKV server is running, a
/// [`RaftKv`](crate::server::raftkv::RaftKv) will be the underlying [`Engine`]
/// of [`Storage`]. The other two types of engines are for test purpose.
///
/// [`Storage`] is reference counted and cloning [`Storage`] will just increase
/// the reference counter. Storage resources (i.e. threads, engine) will be
/// released when all references are dropped.
///
/// Notice that read and write methods may not be performed over full data in
/// most cases, i.e. when underlying engine is
/// [`RaftKv`](crate::server::raftkv::RaftKv), which limits data access in the
/// range of a single region according to specified `ctx` parameter. However,
/// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::
/// UnsafeDestroyRange) is the only exception. It's always performed on the
/// whole TiKV.
///
/// Operations of [`Storage`](Storage) can be divided into two types: MVCC
/// operations and raw operations. MVCC operations uses MVCC keys, which usually
/// consist of several physical keys in different CFs. In default CF and write
/// CF, the key will be memcomparable-encoded and append the timestamp to it, so
/// that multiple versions can be saved at the same time. Raw operations use raw
/// keys, which are saved directly to the engine without memcomparable- encoding
/// and appending timestamp.
pub struct Storage<E: Engine, L: LockManager, F: KvFormat> {
    // TODO: Too many Arcs, would be slow when clone.
    engine: E,

    sched: TxnScheduler<E, L>,

    /// The thread pool used to run most read operations.
    read_pool: ReadPoolHandle,

    concurrency_manager: ConcurrencyManager,

    /// How many strong references. Thread pool and workers will be stopped
    /// once there are no more references.
    // TODO: This should be implemented in thread pool and worker.
    refs: Arc<atomic::AtomicUsize>,

    // Fields below are storage configurations.
    max_key_size: usize,

    resource_tag_factory: ResourceTagFactory,

    api_version: ApiVersion, // TODO: remove this. Use `Api` instead.

    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    quota_limiter: Arc<QuotaLimiter>,
    resource_manager: Option<Arc<ResourceGroupManager>>,

    _phantom: PhantomData<F>,
}

impl<E: Engine, L: LockManager, F: KvFormat> Clone for Storage<E, L, F> {
    #[inline]
    fn clone(&self) -> Self {
        let refs = self.refs.fetch_add(1, Ordering::SeqCst);

        trace!(
            "Storage referenced"; "original_ref" => refs
        );

        Self {
            engine: self.engine.clone(),
            sched: self.sched.clone(),
            read_pool: self.read_pool.clone(),
            refs: self.refs.clone(),
            max_key_size: self.max_key_size,
            concurrency_manager: self.concurrency_manager.clone(),
            api_version: self.api_version,
            causal_ts_provider: self.causal_ts_provider.clone(),
            resource_tag_factory: self.resource_tag_factory.clone(),
            quota_limiter: self.quota_limiter.clone(),
            resource_manager: self.resource_manager.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<E: Engine, L: LockManager, F: KvFormat> Drop for Storage<E, L, F> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, Ordering::SeqCst);

        trace!(
            "Storage de-referenced"; "original_ref" => refs
        );

        if refs != 1 {
            return;
        }

        info!("Storage stopped.");
    }
}

impl<E: Engine, L: LockManager, F: KvFormat> Storage<E, L, F> {
    /// Create a `Storage` from given engine.
    pub fn from_engine<R: FlowStatsReporter>(
        engine: E,
        config: &Config,
        read_pool: ReadPoolHandle,
        lock_mgr: L,
        concurrency_manager: ConcurrencyManager,
        dynamic_switches: DynamicConfigs,
        flow_controller: Arc<FlowController>,
        reporter: R,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
        feature_gate: FeatureGate,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
        resource_ctl: Option<Arc<ResourceController>>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        txn_status_cache: Arc<TxnStatusCache>,
    ) -> Result<Self> {
        assert_eq!(config.api_version(), F::TAG, "Api version not match");

        let sched = TxnScheduler::new(
            engine.clone(),
            lock_mgr,
            concurrency_manager.clone(),
            config,
            dynamic_switches,
            flow_controller,
            causal_ts_provider.clone(),
            reporter,
            resource_tag_factory.clone(),
            Arc::clone(&quota_limiter),
            feature_gate,
            resource_ctl,
            resource_manager.clone(),
            txn_status_cache,
        );

        info!("Storage started.");

        Ok(Storage {
            engine,
            sched,
            read_pool,
            concurrency_manager,
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            max_key_size: config.max_key_size,
            api_version: config.api_version(),
            causal_ts_provider,
            resource_tag_factory,
            quota_limiter,
            resource_manager,
            _phantom: PhantomData,
        })
    }

    /// Get the underlying `Engine` of the `Storage`.
    pub fn get_engine(&self) -> E {
        self.engine.clone()
    }

    pub fn get_scheduler(&self) -> TxnScheduler<E, L> {
        self.sched.clone()
    }

    pub fn get_concurrency_manager(&self) -> ConcurrencyManager {
        self.concurrency_manager.clone()
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.sched.dump_wait_for_entries(cb);
    }

    /// Get a snapshot of `engine`.
    fn snapshot(
        engine: &mut E,
        ctx: SnapContext<'_>,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        kv::snapshot(engine, ctx)
            .map_err(txn::Error::from)
            .map_err(Error::from)
    }

    #[cfg(test)]
    pub fn get_snapshot(&mut self) -> E::Snap {
        self.engine.snapshot(Default::default()).unwrap()
    }

    pub fn release_snapshot(&mut self) {
        self.engine.release_snapshot();
    }

    pub fn get_readpool_queue_per_worker(&self) -> usize {
        self.read_pool.get_queue_size_per_worker()
    }

    pub fn get_normal_pool_size(&self) -> usize {
        self.read_pool.get_normal_pool_size()
    }

    fn with_perf_context<Fn, T>(cmd: CommandKind, f: Fn) -> T
    where
        Fn: FnOnce() -> T,
    {
        // Safety: the read pools ensure that a TLS engine exists.
        unsafe { with_perf_context::<E, _, _>(cmd, f) }
    }

    #[inline]
    fn with_tls_engine<R>(f: impl FnOnce(&mut E) -> R) -> R {
        // Safety: the read pools ensure that a TLS engine exists.
        unsafe { with_tls_engine(f) }
    }

    /// Check the given raw kv CF name. If the given cf is empty, CF_DEFAULT
    /// will be returned.
    // TODO: refactor to use `Api` parameter.
    fn rawkv_cf(cf: &str, api_version: ApiVersion) -> Result<CfName> {
        match api_version {
            ApiVersion::V1 | ApiVersion::V1ttl => {
                // In API V1, the possible cfs are CF_DEFAULT, CF_LOCK and CF_WRITE.
                if cf.is_empty() {
                    return Ok(CF_DEFAULT);
                }
                for c in [CF_DEFAULT, CF_LOCK, CF_WRITE] {
                    if cf == c {
                        return Ok(c);
                    }
                }
                Err(Error::from(ErrorInner::InvalidCf(cf.to_owned())))
            }
            ApiVersion::V2 => {
                // API V2 doesn't allow raw requests from explicitly specifying a `cf`.
                if cf.is_empty() {
                    return Ok(CF_DEFAULT);
                }
                Err(Error::from(ErrorInner::CfDeprecated(cf.to_owned())))
            }
        }
    }

    /// Check if key range is valid
    ///
    /// - If `reverse` is true, `end_key` is less than `start_key`. `end_key` is
    ///   the lower bound.
    /// - If `reverse` is false, `end_key` is greater than `start_key`.
    ///   `end_key` is the upper bound.
    fn check_key_ranges(ranges: &[KeyRange], reverse: bool) -> bool {
        let ranges_len = ranges.len();
        for i in 0..ranges_len {
            let start_key = ranges[i].get_start_key();
            let mut end_key = ranges[i].get_end_key();
            if end_key.is_empty() && i + 1 != ranges_len {
                end_key = ranges[i + 1].get_start_key();
            }
            if !end_key.is_empty()
                && (!reverse && start_key >= end_key || reverse && start_key <= end_key)
            {
                return false;
            }
        }
        true
    }

    /// Check whether a raw kv command or not.
    #[inline]
    fn is_raw_command(cmd: CommandKind) -> bool {
        matches!(
            cmd,
            CommandKind::raw_batch_get_command
                | CommandKind::raw_get
                | CommandKind::raw_batch_get
                | CommandKind::raw_scan
                | CommandKind::raw_batch_scan
                | CommandKind::raw_put
                | CommandKind::raw_batch_put
                | CommandKind::raw_delete
                | CommandKind::raw_delete_range
                | CommandKind::raw_batch_delete
                | CommandKind::raw_get_key_ttl
                | CommandKind::raw_compare_and_swap
                | CommandKind::raw_atomic_store
                | CommandKind::raw_checksum
        )
    }

    /// Check whether a trancsation kv command or not.
    #[inline]
    fn is_txn_command(cmd: CommandKind) -> bool {
        !Self::is_raw_command(cmd)
    }

    /// Check api version.
    ///
    /// When config.api_version = V1: accept request of V1 only.
    /// When config.api_version = V2: accept the following:
    ///   * Request of V1 from TiDB, for compatibility.
    ///   * Request of V2 with legal prefix.
    ///
    /// See the following for detail:
    ///   * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
    ///   * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto,
    ///     enum APIVersion.
    // TODO: refactor to use `Api` parameter.
    fn check_api_version(
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
        cmd: CommandKind,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<()> {
        match (storage_api_version, req_api_version) {
            (ApiVersion::V1, ApiVersion::V1) => {}
            (ApiVersion::V1ttl, ApiVersion::V1) if Self::is_raw_command(cmd) => {
                // storage api_version = V1ttl, allow RawKV request only.
            }
            (ApiVersion::V2, ApiVersion::V1) if Self::is_txn_command(cmd) => {
                // For compatibility, accept TiDB request only.
                for key in keys {
                    if ApiV2::parse_key_mode(key.as_ref()) != KeyMode::Tidb {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_raw_command(cmd) => {
                for key in keys {
                    if ApiV2::parse_key_mode(key.as_ref()) != KeyMode::Raw {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_txn_command(cmd) => {
                for key in keys {
                    if ApiV2::parse_key_mode(key.as_ref()) != KeyMode::Txn {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(Error::from(ErrorInner::ApiVersionNotMatched {
                    cmd,
                    storage_api_version,
                    req_api_version,
                }));
            }
        }
        Ok(())
    }

    // TODO: refactor to use `Api` parameter.
    fn check_api_version_ranges(
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
        cmd: CommandKind,
        ranges: impl IntoIterator<Item = (Option<impl AsRef<[u8]>>, Option<impl AsRef<[u8]>>)>,
    ) -> Result<()> {
        match (storage_api_version, req_api_version) {
            (ApiVersion::V1, ApiVersion::V1) => {}
            (ApiVersion::V1ttl, ApiVersion::V1) if Self::is_raw_command(cmd) => {
                // storage api_version = V1ttl, allow RawKV request only.
            }
            (ApiVersion::V2, ApiVersion::V1) if Self::is_txn_command(cmd) => {
                // For compatibility, accept TiDB request only.
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if ApiV2::parse_range_mode(range) != KeyMode::Tidb {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_raw_command(cmd) => {
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if ApiV2::parse_range_mode(range) != KeyMode::Raw {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_txn_command(cmd) => {
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if ApiV2::parse_range_mode(range) != KeyMode::Txn {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(Error::from(ErrorInner::ApiVersionNotMatched {
                    cmd,
                    storage_api_version,
                    req_api_version,
                }));
            }
        }
        Ok(())
    }

    /// Get value of the given key from a snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn get(
        &self,
        mut ctx: Context,
        key: Key,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Option<Value>, KvGetStatistics)>> {
        let deadline = Self::get_deadline(&ctx);
        const CMD: CommandKind = CommandKind::get;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag_with_key_ranges(
            &ctx,
            vec![(key.as_encoded().to_vec(), key.as_encoded().to_vec())],
        );
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        let quota_limiter = self.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);
        let mut process_nanos = 0u64;
        with_tls_tracker(|tracker| {
            tracker.metrics.grpc_process_nanos =
                tracker.req_info.begin.saturating_elapsed().as_nanos() as u64;
            process_nanos = tracker.metrics.grpc_process_nanos;
        });

        let stage_begin_ts = Instant::now();
        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                let stage_scheduled_ts = Instant::now();
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key.as_encoded(),
                    key.as_encoded(),
                    false,
                    QueryKind::Get,
                );
                with_tls_tracker(|tracker| {
                    record_network_in_bytes(tracker.metrics.grpc_req_size);
                });

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                deadline.check()?;

                Self::check_api_version(api_version, ctx.api_version, CMD, [key.as_encoded()])?;

                let command_duration = Instant::now();

                // The bypass_locks and access_locks set will be checked at most once.
                // `TsSet::vec` is more efficient here.
                let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());
                let access_locks = TsSet::vec_from_u64s(ctx.take_committed_locks());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    iter::once(&key),
                    start_ts,
                    &bypass_locks,
                    &concurrency_manager,
                    CMD,
                )?;
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;

                {
                    deadline.check()?;
                    let begin_instant = Instant::now();
                    let stage_snap_recv_ts = begin_instant;
                    let buckets = snapshot.ext().get_buckets();
                    let mut statistics = Statistics::default();
                    let result = Self::with_perf_context(CMD, || {
                        let _guard = sample.observe_cpu();
                        let snap_store = SnapshotStore::new(
                            snapshot,
                            start_ts,
                            ctx.get_isolation_level(),
                            !ctx.get_not_fill_cache(),
                            bypass_locks,
                            access_locks,
                            false,
                        );
                        snap_store
                            .get(&key, &mut statistics)
                            // map storage::txn::Error -> storage::Error
                            .map_err(Error::from)
                            .inspect(|_r| {
                                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                            })
                    });
                    if let Err(
                        e @ Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::DefaultNotFound { .. }),
                        )))),
                    ) = &result
                    {
                        error!("default not found in storage get";
                            "err" => ?e,
                            "RpcContext" => ?&ctx,
                        );
                    }
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(key.as_encoded()),
                        Some(key.as_encoded()),
                        &statistics,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    let result_len = result
                        .as_ref()
                        .unwrap_or(&None)
                        .as_ref()
                        .map_or(0, |v| v.len());
                    record_network_out_bytes(result_len as u64);
                    let read_bytes = key.len() + result_len;
                    sample.add_read_bytes(read_bytes);
                    let quota_delay = quota_limiter.consume_sample(sample, true).await;
                    if !quota_delay.is_zero() {
                        TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                            .get(CMD)
                            .inc_by(quota_delay.as_micros() as u64);
                    }

                    let stage_finished_ts = Instant::now();
                    let schedule_wait_time =
                        stage_scheduled_ts.saturating_duration_since(stage_begin_ts);
                    let snapshot_wait_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_scheduled_ts);
                    let wait_wall_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                    let process_wall_time =
                        stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                    let latency_stats = StageLatencyStats {
                        schedule_wait_time_ns: schedule_wait_time.as_nanos() as u64,
                        snapshot_wait_time_ns: snapshot_wait_time.as_nanos() as u64,
                        wait_wall_time_ns: wait_wall_time.as_nanos() as u64,
                        process_wall_time_ns: process_wall_time.as_nanos() as u64,
                    };
                    with_tls_tracker(|tracker| {
                        tracker.metrics.read_pool_schedule_wait_nanos =
                            schedule_wait_time.as_nanos() as u64;
                    });
                    record_logical_read_bytes(statistics.processed_size as u64);
                    Ok((
                        result?,
                        KvGetStatistics {
                            stats: statistics,
                            latency_stats,
                        },
                    ))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    /// Get values of a set of keys with separate context from a snapshot,
    /// return a list of `Result`s.
    ///
    /// Only writes that are committed before their respective `start_ts` are
    /// visible.
    pub fn batch_get_command<P: 'static + ResponseBatchConsumer<(Option<Vec<u8>>, Statistics)>>(
        &self,
        requests: Vec<GetRequest>,
        ids: Vec<u64>,
        trackers: Vec<TrackerToken>,
        consumer: P,
        begin_instant: Instant,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = requests[0].get_context().get_priority();
        let metadata =
            TaskMetadata::from_ctx(requests[0].get_context().get_resource_control_context());
        let resource_group_name = requests[0]
            .get_context()
            .get_resource_control_context()
            .get_resource_group_name();
        let group_priority = requests[0]
            .get_context()
            .get_resource_control_context()
            .get_override_priority();
        let resource_priority = ResourcePriority::from(group_priority);
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                resource_group_name,
                requests[0].get_context().get_request_source(),
                group_priority,
            )
        });
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        let busy_threshold =
            Duration::from_millis(requests[0].get_context().busy_threshold_ms as u64);

        // The resource tags of these batched requests are not the same, and it is quite
        // expensive to distinguish them, so we can find random one of them as a
        // representative.
        let rand_index = rand::thread_rng().gen_range(0, requests.len());
        let rand_ctx = requests[rand_index].get_context();
        let rand_key = requests[rand_index].get_key().to_vec();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(rand_ctx, vec![(rand_key.clone(), rand_key)]);
        // Unset the TLS tracker because the future below does not belong to any
        // specific request
        clear_tls_tracker_token();
        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(requests.len() as f64);
                let command_duration = Instant::now();
                let read_id = Some(ThreadReadId::new());
                let mut statistics = Statistics::default();
                let mut req_snaps = vec![];

                for ((mut req, id), tracker) in requests.into_iter().zip(ids).zip(trackers) {
                    set_tls_tracker_token(tracker);
                    let mut ctx = req.take_context();
                    let deadline = Self::get_deadline(&ctx);
                    let source = ctx.take_request_source();
                    let region_id = ctx.get_region_id();
                    let peer = ctx.get_peer();

                    let key = Key::from_raw(req.get_key());
                    tls_collect_query(
                        region_id,
                        peer,
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );
                    record_network_in_bytes(req.get_key().len() as u64);

                    Self::check_api_version(api_version, ctx.api_version, CMD, [key.as_encoded()])?;

                    let start_ts = req.get_version().into();
                    let isolation_level = ctx.get_isolation_level();
                    let fill_cache = !ctx.get_not_fill_cache();
                    let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());
                    let access_locks = TsSet::vec_from_u64s(ctx.take_committed_locks());
                    let region_id = ctx.get_region_id();

                    let snap_ctx = match prepare_snap_ctx(
                        &ctx,
                        iter::once(&key),
                        start_ts,
                        &bypass_locks,
                        &concurrency_manager,
                        CMD,
                    ) {
                        Ok(mut snap_ctx) => {
                            snap_ctx.read_id = if ctx.get_stale_read() {
                                None
                            } else {
                                read_id.clone()
                            };
                            snap_ctx
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant, source, resource_priority);
                            continue;
                        }
                    };

                    let snap = Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx));
                    req_snaps.push((
                        TlsTrackedFuture::new(snap),
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_locks,
                        access_locks,
                        region_id,
                        id,
                        source,
                        tracker,
                        deadline,
                    ));
                }
                Self::with_tls_engine(|engine| engine.release_snapshot());
                for req_snap in req_snaps {
                    let (
                        snap,
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_locks,
                        access_locks,
                        region_id,
                        id,
                        source,
                        tracker,
                        deadline,
                    ) = req_snap;
                    let snap_res = snap.await;
                    if let Err(e) = deadline.check() {
                        consumer.consume(
                            id,
                            Err(Error::from(e)),
                            begin_instant,
                            source,
                            resource_priority,
                        );
                        continue;
                    }

                    set_tls_tracker_token(tracker);
                    match snap_res {
                        Ok(snapshot) => Self::with_perf_context(CMD, || {
                            let buckets = snapshot.ext().get_buckets();
                            match PointGetterBuilder::new(snapshot, start_ts)
                                .fill_cache(fill_cache)
                                .isolation_level(isolation_level)
                                .bypass_locks(bypass_locks)
                                .access_locks(access_locks)
                                .build()
                            {
                                Ok(mut point_getter) => {
                                    let v = point_getter.get(&key);
                                    let stat = point_getter.take_statistics();
                                    metrics::tls_collect_read_flow(
                                        region_id,
                                        Some(key.as_encoded()),
                                        Some(key.as_encoded()),
                                        &stat,
                                        buckets.as_ref(),
                                    );
                                    statistics.add(&stat);
                                    let value_size = v
                                        .as_ref()
                                        .map_or(0, |v| v.as_ref().map_or(0, |v1| v1.len()) as u64);
                                    record_network_out_bytes(value_size);
                                    record_logical_read_bytes(statistics.processed_size as u64);
                                    consumer.consume(
                                        id,
                                        v.map_err(|e| Error::from(txn::Error::from(e)))
                                            .map(|v| (v, stat)),
                                        begin_instant,
                                        source,
                                        resource_priority,
                                    );
                                }
                                Err(e) => {
                                    consumer.consume(
                                        id,
                                        Err(Error::from(txn::Error::from(e))),
                                        begin_instant,
                                        source,
                                        resource_priority,
                                    );
                                }
                            }
                        }),
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant, source, resource_priority);
                        }
                    }
                }
                metrics::tls_collect_scan_details(CMD, &statistics);
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.saturating_elapsed_secs());

                Ok(())
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    pub fn buffer_batch_get(
        &self,
        ctx: Context,
        mut keys: Vec<Key>,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Vec<Result<KvPair>>, KvGetStatistics)>> {
        let deadline = Self::get_deadline(&ctx);
        const CMD: CommandKind = CommandKind::buffer_batch_get;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        keys.sort();
        keys.dedup();
        let key_ranges = keys
            .iter()
            .map(|k| (k.as_encoded().to_vec(), k.as_encoded().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, key_ranges);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);
        let quota_limiter = self.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);
        with_tls_tracker(|tracker| {
            tracker.metrics.grpc_process_nanos =
                tracker.req_info.begin.saturating_elapsed().as_nanos() as u64;
        });
        let stage_begin_ts = Instant::now();
        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                let stage_scheduled_ts = Instant::now();
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key.as_encoded(), key.as_encoded(), false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );
                with_tls_tracker(|tracker| {
                    record_network_in_bytes(tracker.metrics.grpc_req_size);
                });

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                deadline.check()?;

                Self::check_api_version(
                    api_version,
                    ctx.api_version,
                    CMD,
                    keys.iter().map(Key::as_encoded),
                )?;

                let command_duration = Instant::now();

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    &keys,
                    start_ts,
                    // buffer_batch_get doesn't read locks from other txns
                    &TsSet::Empty,
                    &concurrency_manager,
                    CMD,
                )?;
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    deadline.check()?;
                    let begin_instant = Instant::now();

                    let stage_snap_recv_ts = begin_instant;
                    let (result, stats) = Self::with_perf_context(CMD, || {
                        let _guard = sample.observe_cpu();
                        let mut reader = MvccReader::new(
                            snapshot,
                            // TODO: compare the performance of Forward scan and multi get operations.
                            None,
                            !ctx.get_not_fill_cache(),
                        );
                        // TODO: metrics
                        // TODO: refactor: reuse functions in PointGetter
                        let result: Vec<Result<KvPair>> = keys
                            .into_iter()
                            .filter_map(|k| {
                                let pair: Option<std::result::Result<KvPair, _>> =
                                    match reader.load_lock(&k) {
                                        Ok(None) => None,
                                        Ok(Some(lock)) => {
                                            if matches!(lock.lock_type, LockType::Pessimistic) {
                                                assert_ne!(lock.ts, start_ts);
                                            }
                                            if lock.ts != start_ts {
                                                None
                                            } else {
                                                match lock.lock_type {
                                                    // Deletions are returned in the result pairs.
                                                    // This is the same behavior as TiDB's memdb.
                                                    // This is different from a normal batch_get.
                                                    LockType::Delete => Some(Ok((k.into_raw().unwrap(), vec![]))),
                                                    LockType::Lock => unreachable!("Unexpected LockType::Lock. pipelined-dml only supports optimistic transactions"),
                                                    LockType::Pessimistic => unreachable!("Unexpected LockType::Pessimistic. pipelined-dml only supports optimistic transactions"),
                                                    LockType::Put => {
                                                        match lock.short_value {
                                                            Some(v) => Some(Ok((k.into_raw().unwrap(), v))),
                                                            None => match reader.get_value(&k, start_ts) {
                                                                Ok(Some(data)) => {
                                                                    Some(Ok((k.into_raw().unwrap(), data)))
                                                                }
                                                                Ok(None) => None,
                                                                Err(e) => Some(Err(e)),
                                                            },
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => Some(Err(e)),
                                    };
                                pair.map(|r| r.map_err(|e| Error::from(TxnError::from(e))))
                            })
                            .collect();
                        record_network_out_bytes(
                            result.iter().fold(0u64, |acc, r| {
                                acc + r.as_ref().map_or(0, |(k, v)| k.len() + v.len()) as u64
                            })
                        );
                        record_logical_read_bytes(reader.statistics.processed_size as u64);
                        (result, reader.statistics)
                    });
                    metrics::tls_collect_scan_details(CMD, &stats);
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    let read_bytes = stats.cf_statistics(CF_DEFAULT).flow_stats.read_bytes
                        + stats.cf_statistics(CF_LOCK).flow_stats.read_bytes
                        + stats.cf_statistics(CF_WRITE).flow_stats.read_bytes;
                    sample.add_read_bytes(read_bytes);
                    let quota_delay = quota_limiter.consume_sample(sample, true).await;
                    if !quota_delay.is_zero() {
                        TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                            .get(CMD)
                            .inc_by(quota_delay.as_micros() as u64);
                    }

                    let stage_finished_ts = Instant::now();
                    let schedule_wait_time =
                        stage_scheduled_ts.saturating_duration_since(stage_begin_ts);
                    let snapshot_wait_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_scheduled_ts);
                    let wait_wall_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                    let process_wall_time =
                        stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                    with_tls_tracker(|tracker| {
                        tracker.metrics.read_pool_schedule_wait_nanos =
                            schedule_wait_time.as_nanos() as u64;
                    });
                    let latency_stats = StageLatencyStats {
                        schedule_wait_time_ns: duration_to_ms(schedule_wait_time),
                        snapshot_wait_time_ns: duration_to_ms(snapshot_wait_time),
                        wait_wall_time_ns: duration_to_ms(wait_wall_time),
                        process_wall_time_ns: duration_to_ms(process_wall_time),
                    };
                    Ok((
                        result,
                        KvGetStatistics {
                            stats,
                            latency_stats,
                        },
                    ))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }
    /// Get values of a set of keys in a batch from the snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn batch_get(
        &self,
        mut ctx: Context,
        keys: Vec<Key>,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Vec<Result<KvPair>>, KvGetStatistics)>> {
        let deadline = Self::get_deadline(&ctx);
        const CMD: CommandKind = CommandKind::batch_get;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let key_ranges = keys
            .iter()
            .map(|k| (k.as_encoded().to_vec(), k.as_encoded().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, key_ranges);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);
        let quota_limiter = self.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);
        with_tls_tracker(|tracker| {
            tracker.metrics.grpc_process_nanos =
                tracker.req_info.begin.saturating_elapsed().as_nanos() as u64;
        });
        let stage_begin_ts = Instant::now();
        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                let stage_scheduled_ts = Instant::now();
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key.as_encoded(), key.as_encoded(), false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );
                with_tls_tracker(|tracker| {
                    record_network_in_bytes(tracker.metrics.grpc_req_size);
                });

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                deadline.check()?;

                Self::check_api_version(
                    api_version,
                    ctx.api_version,
                    CMD,
                    keys.iter().map(Key::as_encoded),
                )?;

                let command_duration = Instant::now();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());
                let access_locks = TsSet::from_u64s(ctx.take_committed_locks());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    &keys,
                    start_ts,
                    &bypass_locks,
                    &concurrency_manager,
                    CMD,
                )?;
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    deadline.check()?;
                    let begin_instant = Instant::now();

                    let stage_snap_recv_ts = begin_instant;
                    let mut statistics = Vec::with_capacity(keys.len());
                    let buckets = snapshot.ext().get_buckets();
                    let (result, stats) = Self::with_perf_context(CMD, || {
                        let _guard = sample.observe_cpu();
                        let snap_store = SnapshotStore::new(
                            snapshot,
                            start_ts,
                            ctx.get_isolation_level(),
                            !ctx.get_not_fill_cache(),
                            bypass_locks,
                            access_locks,
                            false,
                        );
                        let mut stats = Statistics::default();
                        let result = snap_store
                            .batch_get(&keys, &mut statistics)
                            .map_err(Error::from)
                            .map(|v| {
                                let kv_pairs: Vec<_> = v
                                    .into_iter()
                                    .zip(keys)
                                    .enumerate()
                                    .filter(|&(i, (ref v, ref k))| {
                                        metrics::tls_collect_read_flow(
                                            ctx.get_region_id(),
                                            Some(k.as_encoded()),
                                            Some(k.as_encoded()),
                                            &statistics[i],
                                            buckets.as_ref(),
                                        );
                                        stats.add(&statistics[i]);
                                        !(v.is_ok() && v.as_ref().unwrap().is_none())
                                    })
                                    .map(|(_, (v, k))| match v {
                                        Ok(Some(x)) => Ok((k.into_raw().unwrap(), x)),
                                        Err(e) => Err(Error::from(e)),
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                                    .get(CMD)
                                    .observe(kv_pairs.len() as f64);
                                record_network_out_bytes(kv_pairs.iter().fold(0u64, |acc, r| {
                                    acc + r.as_ref().map_or(0, |(k, v)| k.len() + v.len()) as u64
                                }));
                                kv_pairs
                            });
                        (result, stats)
                    });
                    if let Err(
                        e @ Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::DefaultNotFound { .. }),
                        )))),
                    ) = &result
                    {
                        error!("default not found in storage batch_get";
                            "err" => ?e,
                            "RpcContext" => ?&ctx,
                        );
                    }
                    metrics::tls_collect_scan_details(CMD, &stats);
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    let read_bytes = stats.cf_statistics(CF_DEFAULT).flow_stats.read_bytes
                        + stats.cf_statistics(CF_LOCK).flow_stats.read_bytes
                        + stats.cf_statistics(CF_WRITE).flow_stats.read_bytes;
                    sample.add_read_bytes(read_bytes);
                    let quota_delay = quota_limiter.consume_sample(sample, true).await;
                    if !quota_delay.is_zero() {
                        TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                            .get(CMD)
                            .inc_by(quota_delay.as_micros() as u64);
                    }

                    let stage_finished_ts = Instant::now();
                    let schedule_wait_time =
                        stage_scheduled_ts.saturating_duration_since(stage_begin_ts);
                    let snapshot_wait_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_scheduled_ts);
                    let wait_wall_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                    let process_wall_time =
                        stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                    with_tls_tracker(|tracker| {
                        tracker.metrics.read_pool_schedule_wait_nanos =
                            schedule_wait_time.as_nanos() as u64;
                    });
                    let latency_stats = StageLatencyStats {
                        schedule_wait_time_ns: duration_to_ms(schedule_wait_time),
                        snapshot_wait_time_ns: duration_to_ms(snapshot_wait_time),
                        wait_wall_time_ns: duration_to_ms(wait_wall_time),
                        process_wall_time_ns: duration_to_ms(process_wall_time),
                    };
                    record_logical_read_bytes(stats.processed_size as u64);
                    Ok((
                        result?,
                        KvGetStatistics {
                            stats,
                            latency_stats,
                        },
                    ))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    /// Scan keys in [`start_key`, `end_key`) up to `limit` keys from the
    /// snapshot. If `reverse_scan` is true, it scans [`end_key`,
    /// `start_key`) in descending order. If `end_key` is `None`, it means
    /// the upper bound or the lower bound if reverse scan is unbounded.
    ///
    /// Only writes committed before `start_ts` are visible.
    pub fn scan(
        &self,
        mut ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        sample_step: usize,
        start_ts: TimeStamp,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::scan;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag_with_key_ranges(
            &ctx,
            vec![(
                start_key.as_encoded().to_vec(),
                match &end_key {
                    Some(k) => k.as_encoded().to_vec(),
                    None => vec![],
                },
            )],
        );
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        reverse_scan,
                        QueryKind::Scan,
                    );
                    with_tls_tracker(|tracker| {
                        record_network_in_bytes(tracker.metrics.grpc_req_size);
                    });
                }
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    [(
                        Some(start_key.as_encoded()),
                        end_key.as_ref().map(Key::as_encoded),
                    )],
                )?;

                let (mut start_key, mut end_key) = (Some(start_key), end_key);
                if reverse_scan {
                    std::mem::swap(&mut start_key, &mut end_key);
                }
                let command_duration = Instant::now();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());
                let access_locks = TsSet::from_u64s(ctx.take_committed_locks());

                // Update max_ts and check the in-memory lock table before getting the snapshot
                if !ctx.get_stale_read() {
                    concurrency_manager
                        .update_max_ts(start_ts, "scan")
                        .map_err(txn::Error::from)?;
                }
                if need_check_locks(ctx.get_isolation_level()) {
                    let begin_instant = Instant::now();
                    concurrency_manager
                        .read_range_check(start_key.as_ref(), end_key.as_ref(), |key, lock| {
                            Lock::check_ts_conflict(
                                Cow::Borrowed(lock),
                                key,
                                start_ts,
                                &bypass_locks,
                                ctx.get_isolation_level(),
                            )
                        })
                        .map_err(|e| {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .locked
                                .observe(begin_instant.saturating_elapsed().as_secs_f64());
                            txn::Error::from_mvcc(e)
                        })?;
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(CMD)
                        .unlocked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                }

                let mut snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    start_ts: Some(start_ts),
                    ..Default::default()
                };
                let mut key_range = KeyRange::default();
                if let Some(start_key) = &start_key {
                    key_range.set_start_key(start_key.as_encoded().to_vec());
                }
                if let Some(end_key) = &end_key {
                    key_range.set_end_key(end_key.as_encoded().to_vec());
                }
                if need_check_locks_in_replica_read(&ctx) {
                    snap_ctx.key_ranges = vec![key_range.clone()];
                }

                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                Self::with_perf_context(CMD, || {
                    let begin_instant = Instant::now();
                    let buckets = snapshot.ext().get_buckets();

                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_locks,
                        access_locks,
                        false,
                    );

                    let mut scanner =
                        snap_store.scanner(reverse_scan, key_only, false, start_key, end_key)?;
                    let res = scanner.scan(limit, sample_step);

                    let statistics = scanner.take_statistics();
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(key_range.get_start_key()),
                        Some(key_range.get_end_key()),
                        &statistics,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    res.map_err(Error::from).map(|results| {
                        KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                            .get(CMD)
                            .observe(results.len() as f64);
                        record_network_out_bytes(results.iter().fold(0u64, |acc, r| {
                            acc + r.as_ref().map_or(0, |(k, v)| k.len() + v.len()) as u64
                        }));
                        record_logical_read_bytes(statistics.processed_size as u64);
                        results
                            .into_iter()
                            .map(|x| x.map_err(Error::from))
                            .collect()
                    })
                })
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    pub fn scan_lock(
        &self,
        mut ctx: Context,
        max_ts: TimeStamp,
        start_key: Option<Key>,
        end_key: Option<Key>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<LockInfo>>> {
        const CMD: CommandKind = CommandKind::scan_lock;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag_with_key_ranges(
            &ctx,
            vec![(
                match &start_key {
                    Some(k) => k.as_encoded().to_vec(),
                    None => vec![],
                },
                match &end_key {
                    Some(k) => k.as_encoded().to_vec(),
                    None => vec![],
                },
            )],
        );
        let concurrency_manager = self.concurrency_manager.clone();
        // Do not allow replica read for scan_lock.
        ctx.set_replica_read(false);

        let res = self.read_pool.spawn_handle(
            async move {
                if let Some(start_key) = &start_key {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        false,
                        QueryKind::Scan,
                    );
                    with_tls_tracker(|tracker| {
                        record_network_in_bytes(tracker.metrics.grpc_req_size);
                    });
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                // Do not check_api_version in scan_lock, to be compatible with TiDB gc-worker,
                // which resolves locks on regions, and boundary of regions will be out of range
                // of TiDB keys.

                let command_duration = Instant::now();

                concurrency_manager
                    .update_max_ts(max_ts, "scan_lock")
                    .map_err(txn::Error::from)?;
                let begin_instant = Instant::now();
                // TODO: Though it's very unlikely to find a conflicting memory lock here, it's
                // not a good idea to return an error to the client, making the GC fail. A
                // better approach is to wait for these locks to be unlocked.
                concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        // `Lock::check_ts_conflict` can't be used here, because LockType::Lock
                        // can't be ignored in this case.
                        if lock.ts <= max_ts {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .locked
                                .observe(begin_instant.saturating_elapsed().as_secs_f64());
                            Err(txn::Error::from_mvcc(mvcc::ErrorInner::KeyIsLocked(
                                lock.clone().into_lock_info(key.to_raw()?),
                            )))
                        } else {
                            Ok(())
                        }
                    },
                )?;
                CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                    .get(CMD)
                    .unlocked
                    .observe(begin_instant.saturating_elapsed().as_secs_f64());

                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };

                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                Self::with_perf_context(CMD, || {
                    let begin_instant = Instant::now();
                    let mut statistics = Statistics::default();
                    let buckets = snapshot.ext().get_buckets();
                    let mut reader = MvccReader::new(
                        snapshot,
                        Some(ScanMode::Forward),
                        !ctx.get_not_fill_cache(),
                    );
                    let read_res = reader
                        .scan_locks(
                            start_key.as_ref(),
                            end_key.as_ref(),
                            |_, lock| lock.get_start_ts() <= max_ts,
                            limit,
                            resolve_lock,
                        )
                        .map_err(txn::Error::from);
                    statistics.add(&reader.statistics);
                    let (read_locks, _) = read_res?;
                    let mut locks = Vec::with_capacity(read_locks.len());
                    for (key, lock) in read_locks.into_iter() {
                        let lock_info =
                            lock.into_lock_info(key.into_raw().map_err(txn::Error::from)?);
                        locks.push(lock_info);
                    }

                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(
                        ctx.get_region_id(),
                        start_key.as_ref().map(|key| key.as_encoded().as_slice()),
                        end_key.as_ref().map(|key| key.as_encoded().as_slice()),
                        &statistics,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));
                    record_network_out_bytes(
                        locks.iter().map(|l| l.compute_size()).sum::<u32>() as u64
                    );
                    record_logical_read_bytes(statistics.processed_size as u64);
                    Ok(locks)
                })
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    // The entry point of the storage scheduler. Not only transaction commands need
    // to access keys serially.
    pub fn sched_txn_command<T: StorageCallbackType>(
        &self,
        cmd: TypedCommand<T>,
        callback: Callback<T>,
    ) -> Result<()> {
        use crate::storage::txn::commands::{
            AcquirePessimisticLock, AcquirePessimisticLockResumed, Flush, Prewrite,
            PrewritePessimistic,
        };

        let cmd: Command = cmd.into();

        match &cmd {
            Command::Prewrite(Prewrite { mutations, .. }) => {
                let keys = mutations.iter().map(|m| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::PrewritePessimistic(PrewritePessimistic { mutations, .. }) => {
                let keys = mutations.iter().map(|(m, _)| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::Flush(Flush { mutations, .. }) => {
                let keys = mutations.iter().map(|m| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::flush,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::AcquirePessimisticLock(AcquirePessimisticLock { keys, .. }) => {
                let keys = keys.iter().map(|k| k.0.as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::acquire_pessimistic_lock,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::AcquirePessimisticLockResumed(AcquirePessimisticLockResumed {
                items, ..
            }) => {
                let keys = items.iter().map(|item| item.key.as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::acquire_pessimistic_lock_resumed,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            _ => {}
        }
        with_tls_tracker(|tracker| {
            tracker.req_info.start_ts = cmd.ts().into_inner();
            tracker.req_info.request_type = cmd.request_type();
        });

        fail_point!("storage_drop_message", |_| Ok(()));
        self.sched.run_cmd(cmd, T::callback(callback));

        Ok(())
    }

    // The entry point of the raw atomic command scheduler.
    pub fn sched_raw_atomic_command<T: StorageCallbackType>(
        sched: TxnScheduler<E, L>,
        cmd: TypedCommand<T>,
        callback: Callback<T>,
    ) {
        let cmd: Command = cmd.into();
        sched.run_cmd(cmd, T::callback(callback));
    }

    // Schedule raw modify commands, which reuse the scheduler worker pool.
    // TODO: separate the txn and raw commands if needed in the future.
    fn sched_raw_command<T>(
        &self,
        metadata: TaskMetadata<'_>,
        pri: CommandPri,
        tag: CommandKind,
        future: T,
    ) -> Result<()>
    where
        T: Future<Output = ()> + Send + 'static,
    {
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        self.sched
            .get_sched_pool()
            // NOTE: we don't support background resource control for raw api.
            .spawn("", metadata, pri, future)
            .map_err(|_| Error::from(ErrorInner::SchedTooBusy))
    }

    fn get_deadline(ctx: &Context) -> Deadline {
        let execution_duration_limit = if ctx.max_execution_duration_ms == 0 {
            crate::storage::txn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT
        } else {
            ::std::time::Duration::from_millis(ctx.max_execution_duration_ms)
        };
        Deadline::from_now(execution_duration_limit)
    }

    /// Delete all keys in the range [`start_key`, `end_key`).
    ///
    /// All keys in the range will be deleted permanently regardless of their
    /// timestamps. This means that deleted keys will not be retrievable by
    /// specifying an older timestamp. If `notify_only` is set, the data will
    /// not be immediately deleted, but the operation will still be replicated
    /// via Raft. This is used to notify that the data will be deleted by
    /// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::
    /// UnsafeDestroyRange) soon.
    pub fn delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        notify_only: bool,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version_ranges(
            self.api_version,
            ctx.api_version,
            CommandKind::delete_range,
            [(Some(start_key.as_encoded()), Some(end_key.as_encoded()))],
        )?;

        let mut modifies = Vec::with_capacity(DATA_CFS_LEN);
        for cf in DATA_CFS {
            modifies.push(Modify::DeleteRange(
                cf,
                start_key.clone(),
                end_key.clone(),
                notify_only,
            ));
        }

        let mut batch = WriteData::from_modifies(modifies);
        batch.set_allowed_on_disk_almost_full();
        let res = kv::write(
            &self.engine,
            &ctx,
            batch,
            Some(Box::new(|res| {
                callback(mem::replace(res, Ok(())).map_err(Error::from))
            })),
        );
        // TODO: perhaps change delete_range API to return future.
        if let Some(Some(Err(e))) = try_poll(res) {
            return Err(Error::from(e));
        }
        KV_COMMAND_COUNTER_VEC_STATIC.delete_range.inc();
        Ok(())
    }

    /// Get the value of a raw key.
    pub fn raw_get(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Option<Vec<u8>>>> {
        const CMD: CommandKind = CommandKind::raw_get;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, vec![(key.clone(), key.clone())]);
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;

                let command_duration = Instant::now();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let buckets = snapshot.ext().get_buckets();
                let store = RawStore::new(snapshot, api_version);
                let cf = Self::rawkv_cf(&cf, api_version)?;
                {
                    let begin_instant = Instant::now();
                    let mut stats = Statistics::default();
                    let key = F::encode_raw_key_owned(key, None);
                    // Keys pass to `tls_collect_query` should be encoded, to get correct keys for
                    // region split.
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );
                    let r = store
                        .raw_get_key_value(cf, &key, &mut stats)
                        .map_err(Error::from);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(key.as_encoded()),
                        Some(key.as_encoded()),
                        &stats,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));
                    r
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    /// Get the values of a set of raw keys, return a list of `Result`s.
    pub fn raw_batch_get_command<P: 'static + ResponseBatchConsumer<Option<Vec<u8>>>>(
        &self,
        gets: Vec<RawGetRequest>,
        ids: Vec<u64>,
        consumer: P,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::raw_batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = gets[0].get_context().get_priority();
        let metadata = TaskMetadata::from_ctx(gets[0].get_context().get_resource_control_context());
        let resource_group_name = gets[0]
            .get_context()
            .get_resource_control_context()
            .get_resource_group_name();
        let group_priority = gets[0]
            .get_context()
            .get_resource_control_context()
            .get_override_priority();
        let resource_priority = ResourcePriority::from(group_priority);
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                resource_group_name,
                gets[0].get_context().get_request_source(),
                group_priority,
            )
        });
        let priority_tag = get_priority_tag(priority);
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(gets[0].get_context().busy_threshold_ms as u64);

        // The resource tags of these batched requests are not the same, and it is quite
        // expensive to distinguish them, so we can find random one of them as a
        // representative.
        let rand_index = rand::thread_rng().gen_range(0, gets.len());
        let rand_ctx = gets[rand_index].get_context();
        let rand_key = gets[rand_index].get_key().to_vec();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(rand_ctx, vec![(rand_key.clone(), rand_key)]);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(gets.len() as f64);

                for get in &gets {
                    Self::check_api_version(
                        api_version,
                        get.get_context().api_version,
                        CMD,
                        [get.get_key()],
                    )?;
                }

                let command_duration = Instant::now();
                let read_id = Some(ThreadReadId::new());
                let mut snaps = vec![];
                for (mut req, id) in gets.into_iter().zip(ids) {
                    let ctx = req.take_context();
                    let key = F::encode_raw_key_owned(req.take_key(), None);
                    // Keys pass to `tls_collect_query` should be encoded, to get correct keys for
                    // region split. Don't place in loop of `snaps`, otherwise `snap.wait` may run
                    // in another thread, and cause the `thread-local` statistics unstable for test.
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );

                    let snap_ctx = SnapContext {
                        pb_ctx: &ctx,
                        read_id: read_id.clone(),
                        ..Default::default()
                    };
                    let snap = Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx));
                    snaps.push((id, key, ctx, req, snap));
                }
                Self::with_tls_engine(|engine| engine.release_snapshot());
                let begin_instant = Instant::now();
                for (id, key, mut ctx, mut req, snap) in snaps {
                    let cf = req.take_cf();
                    match snap.await {
                        Ok(snapshot) => {
                            let mut stats = Statistics::default();
                            let buckets = snapshot.ext().get_buckets();
                            let store = RawStore::new(snapshot, api_version);
                            match Self::rawkv_cf(&cf, api_version) {
                                Ok(cf) => {
                                    consumer.consume(
                                        id,
                                        store
                                            .raw_get_key_value(cf, &key, &mut stats)
                                            .map_err(Error::from),
                                        begin_instant,
                                        ctx.take_request_source(),
                                        resource_priority,
                                    );
                                    tls_collect_read_flow(
                                        ctx.get_region_id(),
                                        Some(key.as_encoded()),
                                        Some(key.as_encoded()),
                                        &stats,
                                        buckets.as_ref(),
                                    );
                                }
                                Err(e) => {
                                    consumer.consume(
                                        id,
                                        Err(e),
                                        begin_instant,
                                        ctx.take_request_source(),
                                        resource_priority,
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            consumer.consume(
                                id,
                                Err(e),
                                begin_instant,
                                ctx.take_request_source(),
                                resource_priority,
                            );
                        }
                    }
                }

                let now = Instant::now();
                SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(duration_to_sec(
                        now.saturating_duration_since(begin_instant),
                    ));
                SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                    now.saturating_duration_since(command_duration),
                ));
                Ok(())
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    /// Get the values of some raw keys in a batch.
    pub fn raw_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::raw_batch_get;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let key_ranges = keys.iter().map(|k| (k.clone(), k.clone())).collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, key_ranges);
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                let mut key_ranges = vec![];
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, &keys)?;

                let command_duration = Instant::now();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let buckets = snapshot.ext().get_buckets();
                let store = RawStore::new(snapshot, api_version);
                {
                    let begin_instant = Instant::now();

                    let cf = Self::rawkv_cf(&cf, api_version)?;
                    // no scan_count for this kind of op.
                    let mut stats = Statistics::default();
                    let result: Vec<Result<KvPair>> = keys
                        .into_iter()
                        .map(|k| {
                            let k = F::encode_raw_key_owned(k, None);
                            let mut s = Statistics::default();
                            let v = store.raw_get_key_value(cf, &k, &mut s).map_err(Error::from);
                            tls_collect_read_flow(
                                ctx.get_region_id(),
                                Some(k.as_encoded()),
                                Some(k.as_encoded()),
                                &s,
                                buckets.as_ref(),
                            );
                            stats.add(&s);
                            key_ranges.push(build_key_range(k.as_encoded(), k.as_encoded(), false));
                            (k, v)
                        })
                        .filter(|(_, v)| !(v.is_ok() && v.as_ref().unwrap().is_none()))
                        .map(|(k, v)| match v {
                            Ok(v) => {
                                let (user_key, _) = F::decode_raw_key_owned(k, false).unwrap();
                                Ok((user_key, v.unwrap()))
                            }
                            Err(v) => Err(v),
                        })
                        .collect();

                    tls_collect_query_batch(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key_ranges,
                        QueryKind::Get,
                    );
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(stats.data.flow_stats.read_keys as f64);
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));
                    Ok(result)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    async fn check_causal_ts_flushed(ctx: &mut Context, tag: CommandKind) -> Result<()> {
        if F::TAG == ApiVersion::V2 {
            let snap_ctx = SnapContext {
                pb_ctx: ctx,
                ..Default::default()
            };
            match Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await {
                Ok(snapshot) => {
                    SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();
                    if !snapshot.ext().is_max_ts_synced() {
                        return Err(Error::from(txn::Error::from(
                            TxnErrorInner::RawKvMaxTimestampNotSynced {
                                region_id: ctx.get_region_id(),
                            },
                        )));
                    }
                    let term = snapshot.ext().get_term();
                    if let Some(term) = term {
                        ctx.set_term(term.get());
                    }
                }
                Err(err) => {
                    SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();
                    info!("get snapshot failed"; "tag" => ?tag, "err" => ?err);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Write a raw key to the storage.
    pub fn raw_put(
        &self,
        mut ctx: Context,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_put;
        let api_version = self.api_version;

        Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;

        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);

        if !F::IS_TTL_ENABLED && ttl != 0 {
            return Err(Error::from(ErrorInner::TtlNotEnabled));
        }
        let deadline = Self::get_deadline(&ctx);
        let cf = Self::rawkv_cf(&cf, self.api_version)?;
        let provider = self.causal_ts_provider.clone();
        let engine = self.engine.clone();
        let concurrency_manager = self.concurrency_manager.clone();

        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            if let Err(e) = deadline.check() {
                return callback(Err(Error::from(e)));
            }
            let command_duration = Instant::now();

            if let Err(e) = Self::check_causal_ts_flushed(&mut ctx, CMD).await {
                return callback(Err(e));
            }

            let key_guard = get_raw_key_guard(&provider, concurrency_manager).await;
            if let Err(e) = key_guard {
                return callback(Err(e));
            }
            let ts = get_causal_ts(&provider).await;
            if let Err(e) = ts {
                return callback(Err(e));
            }
            let raw_value = RawValue {
                user_value: value,
                expire_ts: ttl_to_expire_ts(ttl),
                is_delete: false,
            };
            let m = Modify::Put(
                cf,
                F::encode_raw_key_owned(key, ts.unwrap()),
                F::encode_raw_value_owned(raw_value),
            );

            let mut batch = WriteData::from_modifies(vec![m]);
            batch.set_allowed_on_disk_almost_full();
            let res = kv::write(&engine, &ctx, batch, None);
            callback(
                res.await
                    .unwrap_or_else(|| Err(box_err!("stale command")))
                    .map_err(Error::from),
            );
            KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
            SCHED_STAGE_COUNTER_VEC.get(CMD).write_finish.inc();
            SCHED_HISTOGRAM_VEC_STATIC
                .get(CMD)
                .observe(command_duration.saturating_elapsed().as_secs_f64());
        })
    }

    fn check_ttl_valid(key_cnt: usize, ttls: &[u64]) -> Result<()> {
        if !F::IS_TTL_ENABLED {
            if ttls.iter().any(|&x| x != 0) {
                return Err(Error::from(ErrorInner::TtlNotEnabled));
            }
        } else if ttls.len() != key_cnt {
            return Err(Error::from(ErrorInner::TtlLenNotEqualsToPairs));
        }
        Ok(())
    }

    fn raw_batch_put_requests_to_modifies(
        cf: CfName,
        pairs: Vec<KvPair>,
        ttls: Vec<u64>,
        ts: Option<TimeStamp>,
    ) -> Vec<Modify> {
        pairs
            .into_iter()
            .zip(ttls)
            .map(|((k, v), ttl)| {
                let raw_value = RawValue {
                    user_value: v,
                    expire_ts: ttl_to_expire_ts(ttl),
                    is_delete: false,
                };
                Modify::Put(
                    cf,
                    F::encode_raw_key_owned(k, ts),
                    F::encode_raw_value_owned(raw_value),
                )
            })
            .collect()
    }

    /// Write some keys to the storage in a batch.
    pub fn raw_batch_put(
        &self,
        mut ctx: Context,
        cf: String,
        pairs: Vec<KvPair>,
        ttls: Vec<u64>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_batch_put;
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CMD,
            pairs.iter().map(|(ref k, _)| k),
        )?;

        let cf = Self::rawkv_cf(&cf, self.api_version)?;

        check_key_size!(
            pairs.iter().map(|(ref k, _)| k),
            self.max_key_size,
            callback
        );
        Self::check_ttl_valid(pairs.len(), &ttls)?;

        let provider = self.causal_ts_provider.clone();
        let engine = self.engine.clone();
        let concurrency_manager = self.concurrency_manager.clone();
        let deadline = Self::get_deadline(&ctx);
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            if let Err(e) = deadline.check() {
                return callback(Err(Error::from(e)));
            }
            let command_duration = Instant::now();

            if let Err(e) = Self::check_causal_ts_flushed(&mut ctx, CMD).await {
                return callback(Err(e));
            }

            let key_guard = get_raw_key_guard(&provider, concurrency_manager).await;
            if let Err(e) = key_guard {
                return callback(Err(e));
            }
            let ts = get_causal_ts(&provider).await;
            if let Err(e) = ts {
                return callback(Err(e));
            }

            let modifies = Self::raw_batch_put_requests_to_modifies(cf, pairs, ttls, ts.unwrap());
            let mut batch = WriteData::from_modifies(modifies);
            batch.set_allowed_on_disk_almost_full();
            let res = kv::write(&engine, &ctx, batch, None);
            callback(
                res.await
                    .unwrap_or_else(|| Err(box_err!("stale command")))
                    .map_err(Error::from),
            );
            KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
            SCHED_STAGE_COUNTER_VEC.get(CMD).write_finish.inc();
            SCHED_HISTOGRAM_VEC_STATIC
                .get(CMD)
                .observe(command_duration.saturating_elapsed().as_secs_f64());
        })
    }

    fn raw_delete_request_to_modify(cf: CfName, key: Vec<u8>, ts: Option<TimeStamp>) -> Modify {
        let key = F::encode_raw_key_owned(key, ts);
        match F::TAG {
            ApiVersion::V2 => Modify::Put(cf, key, ApiV2::ENCODED_LOGICAL_DELETE.to_vec()),
            _ => Modify::Delete(cf, key),
        }
    }

    /// Delete a raw key from the storage.
    /// In API V2, data is "logical" deleted, to enable CDC of delete
    /// operations.
    pub fn raw_delete(
        &self,
        mut ctx: Context,
        cf: String,
        key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_delete;
        Self::check_api_version(self.api_version, ctx.api_version, CMD, [&key])?;

        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);
        let cf = Self::rawkv_cf(&cf, self.api_version)?;
        let provider = self.causal_ts_provider.clone();
        let engine = self.engine.clone();
        let concurrency_manager = self.concurrency_manager.clone();
        let deadline = Self::get_deadline(&ctx);
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            if let Err(e) = deadline.check() {
                return callback(Err(Error::from(e)));
            }
            let command_duration = Instant::now();

            if let Err(e) = Self::check_causal_ts_flushed(&mut ctx, CMD).await {
                return callback(Err(e));
            }

            let key_guard = get_raw_key_guard(&provider, concurrency_manager).await;
            if let Err(e) = key_guard {
                return callback(Err(e));
            }
            let ts = get_causal_ts(&provider).await;
            if let Err(e) = ts {
                return callback(Err(e));
            }
            let m = Self::raw_delete_request_to_modify(cf, key, ts.unwrap());
            let mut batch = WriteData::from_modifies(vec![m]);
            batch.set_allowed_on_disk_almost_full();
            let res = kv::write(&engine, &ctx, batch, None);
            callback(
                res.await
                    .unwrap_or_else(|| Err(box_err!("stale command")))
                    .map_err(Error::from),
            );
            KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
            SCHED_STAGE_COUNTER_VEC.get(CMD).write_finish.inc();
            SCHED_HISTOGRAM_VEC_STATIC
                .get(CMD)
                .observe(command_duration.saturating_elapsed().as_secs_f64());
        })
    }

    /// Delete all raw keys in [`start_key`, `end_key`).
    /// Note that in API V2, data is still "physical" deleted, as "logical"
    /// delete for a range will be quite expensive. Notification of range delete
    /// operations will be through a special channel (unimplemented yet).
    pub fn raw_delete_range(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_delete_range;
        check_key_size!([&start_key, &end_key], self.max_key_size, callback);
        Self::check_api_version_ranges(
            self.api_version,
            ctx.api_version,
            CMD,
            [(Some(&start_key), Some(&end_key))],
        )?;

        let cf = Self::rawkv_cf(&cf, self.api_version)?;
        let engine = self.engine.clone();
        let deadline = Self::get_deadline(&ctx);
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            if let Err(e) = deadline.check() {
                return callback(Err(Error::from(e)));
            }
            let command_duration = Instant::now();
            let start_key = F::encode_raw_key_owned(start_key, None);
            let end_key = F::encode_raw_key_owned(end_key, None);

            let mut batch =
                WriteData::from_modifies(vec![Modify::DeleteRange(cf, start_key, end_key, false)]);
            batch.set_allowed_on_disk_almost_full();

            // TODO: special notification channel for API V2.
            let res = kv::write(&engine, &ctx, batch, None);
            callback(
                res.await
                    .unwrap_or_else(|| Err(box_err!("stale command")))
                    .map_err(Error::from),
            );
            KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
            SCHED_STAGE_COUNTER_VEC.get(CMD).write_finish.inc();
            SCHED_HISTOGRAM_VEC_STATIC
                .get(CMD)
                .observe(command_duration.saturating_elapsed().as_secs_f64());
        })
    }

    /// Delete some raw keys in a batch.
    /// In API V2, data is "logical" deleted, to enable CDC of delete
    /// operations.
    pub fn raw_batch_delete(
        &self,
        mut ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_batch_delete;
        Self::check_api_version(self.api_version, ctx.api_version, CMD, &keys)?;

        let cf = Self::rawkv_cf(&cf, self.api_version)?;
        check_key_size!(keys.iter(), self.max_key_size, callback);
        let provider = self.causal_ts_provider.clone();
        let engine = self.engine.clone();
        let concurrency_manager = self.concurrency_manager.clone();
        let deadline = Self::get_deadline(&ctx);
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            if let Err(e) = deadline.check() {
                return callback(Err(Error::from(e)));
            }
            let command_duration = Instant::now();

            if let Err(e) = Self::check_causal_ts_flushed(&mut ctx, CMD).await {
                return callback(Err(e));
            }

            let key_guard = get_raw_key_guard(&provider, concurrency_manager).await;
            if let Err(e) = key_guard {
                return callback(Err(e));
            }
            let ts = get_causal_ts(&provider).await;
            if let Err(e) = ts {
                return callback(Err(e));
            }
            let ts = ts.unwrap();
            let modifies: Vec<Modify> = keys
                .into_iter()
                .map(|k| Self::raw_delete_request_to_modify(cf, k, ts))
                .collect();
            let mut batch = WriteData::from_modifies(modifies);
            batch.set_allowed_on_disk_almost_full();
            let res = kv::write(&engine, &ctx, batch, None);
            callback(
                res.await
                    .unwrap_or_else(|| Err(box_err!("stale command")))
                    .map_err(Error::from),
            );
            KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
            SCHED_STAGE_COUNTER_VEC.get(CMD).write_finish.inc();
            SCHED_HISTOGRAM_VEC_STATIC
                .get(CMD)
                .observe(command_duration.saturating_elapsed().as_secs_f64());
        })
    }

    /// Scan raw keys in a range.
    ///
    /// If `reverse_scan` is false, the range is [`start_key`, `end_key`);
    /// otherwise, the range is [`end_key`, `start_key`) and it scans from
    /// `start_key` and goes backwards. If `end_key` is `None`, it means
    /// unbounded.
    ///
    /// This function scans at most `limit` keys.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be
    /// returned.
    pub fn raw_scan(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::raw_scan;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    [(Some(&start_key), end_key.as_ref())],
                )?;

                let command_duration = Instant::now();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let buckets = snapshot.ext().get_buckets();
                let cf = Self::rawkv_cf(&cf, api_version)?;
                {
                    let store = RawStore::new(snapshot, api_version);
                    let begin_instant = Instant::now();

                    let start_key = F::encode_raw_key_owned(start_key, None);
                    let end_key = end_key.map(|k| F::encode_raw_key_owned(k, None));
                    // Keys pass to `tls_collect_query` should be encoded, to get correct keys for
                    // region split.
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key.as_ref().map(|k| k.as_encoded()).unwrap_or(&vec![]),
                        reverse_scan,
                        QueryKind::Scan,
                    );

                    let mut statistics = Statistics::default();
                    let result = if reverse_scan {
                        store
                            .reverse_raw_scan(
                                cf,
                                &start_key,
                                end_key.as_ref(),
                                limit,
                                &mut statistics,
                                key_only,
                            )
                            .await
                    } else {
                        store
                            .forward_raw_scan(
                                cf,
                                &start_key,
                                end_key.as_ref(),
                                limit,
                                &mut statistics,
                                key_only,
                            )
                            .await
                    }
                    .map(|pairs| {
                        pairs
                            .into_iter()
                            .map(|pair| {
                                pair.map(|(k, v)| {
                                    let (user_key, _) =
                                        F::decode_raw_key_owned(Key::from_encoded(k), true)
                                            .unwrap();
                                    (user_key, v)
                                })
                                .map_err(Error::from)
                            })
                            .collect()
                    })
                    .map_err(Error::from);

                    metrics::tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(start_key.as_encoded()),
                        end_key.as_ref().map(|k| k.as_encoded().as_slice()),
                        &statistics,
                        buckets.as_ref(),
                    );
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(statistics.data.flow_stats.read_keys as f64);
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));

                    result
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    /// Scan raw keys in multiple ranges in a batch.
    pub fn raw_batch_scan(
        &self,
        ctx: Context,
        cf: String,
        mut ranges: Vec<KeyRange>,
        each_limit: usize,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::raw_batch_scan;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let key_ranges = ranges
            .iter()
            .map(|key_range| (key_range.start_key.clone(), key_range.end_key.clone()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, key_ranges);
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    ranges
                        .iter()
                        .map(|range| (Some(range.get_start_key()), Some(range.get_end_key()))),
                )?;

                let command_duration = Instant::now();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let buckets = snapshot.ext().get_buckets();
                let cf = Self::rawkv_cf(&cf, api_version)?;
                {
                    let store = RawStore::new(snapshot, api_version);
                    let begin_instant = Instant::now();
                    let mut statistics = Statistics::default();
                    if !Self::check_key_ranges(&ranges, reverse_scan) {
                        return Err(box_err!("Invalid KeyRanges"));
                    };
                    let mut result = Vec::new();
                    let mut key_ranges = vec![];
                    let ranges_len = ranges.len();

                    for i in 0..ranges_len {
                        let start_key = F::encode_raw_key_owned(ranges[i].take_start_key(), None);
                        let end_key = ranges[i].take_end_key();
                        let end_key = if end_key.is_empty() {
                            if i + 1 == ranges_len {
                                None
                            } else {
                                Some(F::encode_raw_key(ranges[i + 1].get_start_key(), None))
                            }
                        } else {
                            Some(F::encode_raw_key_owned(end_key, None))
                        };
                        let mut stats = Statistics::default();
                        let pairs: Vec<Result<KvPair>> = if reverse_scan {
                            store
                                .reverse_raw_scan(
                                    cf,
                                    &start_key,
                                    end_key.as_ref(),
                                    each_limit,
                                    &mut stats,
                                    key_only,
                                )
                                .await
                        } else {
                            store
                                .forward_raw_scan(
                                    cf,
                                    &start_key,
                                    end_key.as_ref(),
                                    each_limit,
                                    &mut stats,
                                    key_only,
                                )
                                .await
                        }
                        .map(|pairs| {
                            pairs
                                .into_iter()
                                .map(|pair| {
                                    pair.map(|(k, v)| {
                                        let (user_key, _) =
                                            F::decode_raw_key_owned(Key::from_encoded(k), true)
                                                .unwrap();
                                        (user_key, v)
                                    })
                                    .map_err(Error::from)
                                })
                                .collect()
                        })
                        .map_err(Error::from)?;

                        key_ranges.push(build_key_range(
                            start_key.as_encoded(),
                            end_key.as_ref().map(|k| k.as_encoded()).unwrap_or(&vec![]),
                            reverse_scan,
                        ));
                        metrics::tls_collect_read_flow(
                            ctx.get_region_id(),
                            Some(start_key.as_encoded()),
                            end_key.as_ref().map(|k| k.as_encoded().as_slice()),
                            &stats,
                            buckets.as_ref(),
                        );
                        statistics.add(&stats);
                        result.extend(pairs);
                    }

                    tls_collect_query_batch(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key_ranges,
                        QueryKind::Scan,
                    );
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(statistics.data.flow_stats.read_keys as f64);
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));
                    Ok(result)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    /// Get the value of a raw key.
    pub fn raw_get_key_ttl(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Option<u64>>> {
        const CMD: CommandKind = CommandKind::raw_get_key_ttl;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, vec![(key.clone(), key.clone())]);
        let api_version = self.api_version;
        let busy_threshold = Duration::from_millis(ctx.busy_threshold_ms as u64);

        self.read_pool_spawn_with_busy_check(
            busy_threshold,
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;

                let command_duration = Instant::now();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let buckets = snapshot.ext().get_buckets();
                let store = RawStore::new(snapshot, api_version);
                let cf = Self::rawkv_cf(&cf, api_version)?;
                {
                    let begin_instant = Instant::now();
                    let mut stats = Statistics::default();
                    let key = F::encode_raw_key_owned(key, None);
                    // Keys pass to `tls_collect_query` should be encoded, to get correct keys for
                    // region split.
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );
                    let r = store
                        .raw_get_key_ttl(cf, &key, &mut stats)
                        .map_err(Error::from);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(key.as_encoded()),
                        Some(key.as_encoded()),
                        &stats,
                        buckets.as_ref(),
                    );
                    let now = Instant::now();
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(duration_to_sec(
                            now.saturating_duration_since(begin_instant),
                        ));
                    SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                        now.saturating_duration_since(command_duration),
                    ));
                    r
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        )
    }

    pub fn raw_compare_and_swap_atomic(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        previous_value: Option<Vec<u8>>,
        value: Vec<u8>,
        ttl: u64,
        callback: Callback<(Option<Value>, bool)>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_compare_and_swap;
        let api_version = self.api_version;
        Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;
        let cf = Self::rawkv_cf(&cf, api_version)?;

        if !F::IS_TTL_ENABLED && ttl != 0 {
            return Err(Error::from(ErrorInner::TtlNotEnabled));
        }
        let sched = self.get_scheduler();
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            let key = F::encode_raw_key_owned(key, None);
            let cmd = RawCompareAndSwap::new(cf, key, previous_value, value, ttl, api_version, ctx);
            Self::sched_raw_atomic_command(sched, cmd, Box::new(|res| callback(res)));
        })
    }

    pub fn raw_batch_put_atomic(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<KvPair>,
        ttls: Vec<u64>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_atomic_store;
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CMD,
            pairs.iter().map(|(ref k, _)| k),
        )?;

        let cf = Self::rawkv_cf(&cf, self.api_version)?;
        Self::check_ttl_valid(pairs.len(), &ttls)?;

        let sched = self.get_scheduler();
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            let modifies = Self::raw_batch_put_requests_to_modifies(cf, pairs, ttls, None);
            let cmd = RawAtomicStore::new(cf, modifies, ctx);
            Self::sched_raw_atomic_command(sched, cmd, Box::new(|res| callback(res)));
        })
    }

    pub fn raw_batch_delete_atomic(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::raw_atomic_store;

        Self::check_api_version(self.api_version, ctx.api_version, CMD, &keys)?;
        let cf = Self::rawkv_cf(&cf, self.api_version)?;
        let sched = self.get_scheduler();
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        self.sched_raw_command(metadata, priority, CMD, async move {
            // Do NOT encode ts here as RawAtomicStore use key to gen lock
            let modifies = keys
                .into_iter()
                .map(|k| Self::raw_delete_request_to_modify(cf, k, None))
                .collect();
            let cmd = RawAtomicStore::new(cf, modifies, ctx);
            Self::sched_raw_atomic_command(sched, cmd, Box::new(|res| callback(res)));
        })
    }

    pub fn raw_checksum(
        &self,
        ctx: Context,
        algorithm: ChecksumAlgorithm,
        mut ranges: Vec<KeyRange>,
    ) -> impl Future<Output = Result<(u64, u64, u64)>> {
        const CMD: CommandKind = CommandKind::raw_checksum;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let resource_limiter = self.resource_manager.as_ref().and_then(|r| {
            r.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
        let priority_tag = get_priority_tag(priority);
        let key_ranges = ranges
            .iter()
            .map(|key_range| (key_range.start_key.clone(), key_range.end_key.clone()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&ctx, key_ranges);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                if algorithm != ChecksumAlgorithm::Crc64Xor {
                    return Err(box_err!("unknown checksum algorithm {:?}", algorithm));
                }

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    ranges
                        .iter()
                        .map(|range| (Some(range.get_start_key()), Some(range.get_end_key()))),
                )?;
                for range in ranges.iter_mut() {
                    let start_key = F::encode_raw_key_owned(range.take_start_key(), None);
                    let end_key = F::encode_raw_key_owned(range.take_end_key(), None);
                    range.set_start_key(start_key.into_encoded());
                    range.set_end_key(end_key.into_encoded());
                }

                let command_duration = Instant::now();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let buckets = snapshot.ext().get_buckets();
                let store = RawStore::new(snapshot, api_version);
                let cf = Self::rawkv_cf("", api_version)?;

                let begin_instant = Instant::now();
                let mut stats = Vec::with_capacity(ranges.len());
                let ret = store
                    .raw_checksum_ranges(cf, &ranges, &mut stats)
                    .await
                    .map_err(Error::from);
                stats.iter().zip(ranges.iter()).for_each(|(stats, range)| {
                    tls_collect_read_flow(
                        ctx.get_region_id(),
                        Some(range.get_start_key()),
                        Some(range.get_end_key()),
                        stats,
                        buckets.as_ref(),
                    );
                });
                let now = Instant::now();
                SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(duration_to_sec(
                        now.saturating_duration_since(begin_instant),
                    ));
                SCHED_HISTOGRAM_VEC_STATIC.get(CMD).observe(duration_to_sec(
                    now.saturating_duration_since(command_duration),
                ));

                ret
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
            metadata,
            resource_limiter,
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    fn read_pool_spawn_with_busy_check<Fut, T>(
        &self,
        busy_threshold: Duration,
        future: Fut,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> impl Future<Output = Result<T>>
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if let Err(busy_err) = self.read_pool.check_busy_threshold(busy_threshold) {
            let mut err = kvproto::errorpb::Error::default();
            err.set_server_is_busy(busy_err);
            return Either::Left(future::err(Error::from(ErrorInner::Kv(err.into()))));
        }
        Either::Right(
            self.read_pool
                .spawn_handle(future, priority, task_id, metadata, resource_limiter)
                .map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .and_then(|res| future::ready(res)),
        )
    }

    pub fn update_txn_status_cache(
        &self,
        ctx: Context,
        txn_statuses: Vec<kvrpcpb::TxnStatus>,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::update_txn_status_cache;
        let priority = ctx.get_priority();
        let metadata = TaskMetadata::from_ctx(ctx.get_resource_control_context());
        let cache = self.get_scheduler().get_txn_status_cache();
        let f = async move {
            let now = SystemTime::now();
            for txn_status in txn_statuses {
                let txn_state = TxnState::from_ts(
                    txn_status.start_ts.into(),
                    txn_status.min_commit_ts.into(),
                    txn_status.commit_ts.into(),
                    txn_status.rolled_back,
                );
                if txn_status.is_completed {
                    // large_txn_cache is only for **ongoing** large txns, so remove it when
                    // completed.
                    assert_matches!(txn_state, TxnState::Committed { .. } | TxnState::RolledBack);
                    cache.remove_large_txn(txn_status.start_ts.into());
                }
                cache.upsert(txn_status.start_ts.into(), txn_state, now);
            }
            callback(Ok(()));
        };
        self.sched_raw_command(metadata, priority, CMD, f)
    }
}

pub async fn get_raw_key_guard(
    ts_provider: &Option<Arc<CausalTsProviderImpl>>,
    concurrency_manager: ConcurrencyManager,
) -> Result<Option<KeyHandleGuard>> {
    // NOTE: the ts cannot be reused as timestamp of data key.
    // There is a little chance that CDC will acquired a timestamp for resolved-ts
    // just between the get_causal_ts & concurrency_manager.lock_key,
    // which violate the constraint that resolve-ts should not be larger
    // than timestamp of captured data.
    let ts = get_causal_ts(ts_provider).await?;
    if let Some(ts) = ts {
        let raw_key = vec![api_version::api_v2::RAW_KEY_PREFIX];
        // Make keys for locking by RAW_KEY_PREFIX & ts. RAW_KEY_PREFIX to avoid
        // conflict with TiDB & TxnKV keys, and ts to avoid collision with
        // other raw write requests. Ts in lock value to used by CDC which
        // get maximum resolved-ts from concurrency_manager.global_min_lock_ts
        let encode_key = ApiV2::encode_raw_key(&raw_key, Some(ts));
        let key_guard = concurrency_manager.lock_key(&encode_key).await;
        let lock = Lock::new(LockType::Put, raw_key, ts, 0, None, 0.into(), 1, ts, false);
        key_guard.with_lock(|l| *l = Some(lock));
        Ok(Some(key_guard))
    } else {
        Ok(None)
    }
}

pub async fn get_causal_ts(
    ts_provider: &Option<Arc<CausalTsProviderImpl>>,
) -> Result<Option<TimeStamp>> {
    if let Some(p) = ts_provider {
        match p.async_get_ts().await {
            Ok(ts) => Ok(Some(ts)),
            Err(e) => Err(box_err!("Fail to get ts: {}", e)),
        }
    } else {
        Ok(None)
    }
}

pub struct DynamicConfigs {
    pub pipelined_pessimistic_lock: Arc<AtomicBool>,
    pub in_memory_pessimistic_lock: Arc<AtomicBool>,
    pub wake_up_delay_duration_ms: Arc<AtomicU64>,
    pub in_memory_peer_size_limit: Arc<AtomicU64>,
    pub in_memory_instance_size_limit: Arc<AtomicU64>,
}

fn get_priority_tag(priority: CommandPri) -> CommandPriority {
    match priority {
        CommandPri::Low => CommandPriority::low,
        CommandPri::Normal => CommandPriority::normal,
        CommandPri::High => CommandPriority::high,
    }
}

fn prepare_snap_ctx<'a>(
    pb_ctx: &'a Context,
    keys: impl IntoIterator<Item = &'a Key> + Clone,
    start_ts: TimeStamp,
    bypass_locks: &'a TsSet,
    concurrency_manager: &ConcurrencyManager,
    cmd: CommandKind,
) -> Result<SnapContext<'a>> {
    // Update max_ts and check the in-memory lock table before getting the snapshot
    if !pb_ctx.get_stale_read() {
        concurrency_manager
            .update_max_ts(start_ts, || {
                format!("prepare_snap_ctx-{}-{}", cmd, start_ts)
            })
            .map_err(txn::Error::from)?;
    }
    fail_point!("before-storage-check-memory-locks");
    let isolation_level = pb_ctx.get_isolation_level();
    if need_check_locks(isolation_level) {
        let begin_instant = Instant::now();
        for key in keys.clone() {
            concurrency_manager
                .read_key_check(key, |lock| {
                    // No need to check access_locks because they are committed which means they
                    // can't be in memory lock table.
                    Lock::check_ts_conflict(
                        Cow::Borrowed(lock),
                        key,
                        start_ts,
                        bypass_locks,
                        isolation_level,
                    )
                })
                .map_err(|e| {
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(cmd)
                        .locked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                    txn::Error::from_mvcc(e)
                })?;
        }
        CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
            .get(cmd)
            .unlocked
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
    }

    let mut snap_ctx = SnapContext {
        pb_ctx,
        start_ts: Some(start_ts),
        ..Default::default()
    };
    if need_check_locks_in_replica_read(pb_ctx) {
        snap_ctx.key_ranges = keys
            .into_iter()
            .map(|k| point_key_range(k.clone()))
            .collect();
    }
    Ok(snap_ctx)
}

pub fn need_check_locks_in_replica_read(ctx: &Context) -> bool {
    ctx.get_replica_read() && ctx.get_isolation_level() == IsolationLevel::Si
}

// checks whether the current isolation level needs to check related locks.
pub fn need_check_locks(iso_level: IsolationLevel) -> bool {
    matches!(iso_level, IsolationLevel::Si | IsolationLevel::RcCheckTs)
}

pub fn point_key_range(key: Key) -> KeyRange {
    let mut end_key = key.as_encoded().to_vec();
    end_key.push(0);
    let end_key = Key::from_encoded(end_key);
    let mut key_range = KeyRange::default();
    key_range.set_start_key(key.into_encoded());
    key_range.set_end_key(end_key.into_encoded());
    key_range
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: Engine, L: LockManager, F: KvFormat> {
    engine: E,
    config: Config,
    pipelined_pessimistic_lock: Arc<AtomicBool>,
    in_memory_pessimistic_lock: Arc<AtomicBool>,
    wake_up_delay_duration_ms: Arc<AtomicU64>,
    lock_mgr: L,
    resource_tag_factory: ResourceTagFactory,
    _phantom: PhantomData<F>,
    in_memory_peer_size_limit: Arc<AtomicU64>,
    in_memory_instance_size_limit: Arc<AtomicU64>,
}

/// TestStorageBuilder for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type TestStorageBuilderApiV1<E, L> = TestStorageBuilder<E, L, ApiV1>;

impl<F: KvFormat> TestStorageBuilder<RocksEngine, MockLockManager, F> {
    /// Build `Storage<RocksEngine>`.
    pub fn new(lock_mgr: MockLockManager) -> Self {
        let engine = TestEngineBuilder::new()
            .api_version(F::TAG)
            .build()
            .unwrap();

        Self::from_engine_and_lock_mgr(engine, lock_mgr)
    }
}

/// An `Engine` with `TxnExt`. It is used for test purpose.
#[derive(Clone)]
pub struct TxnTestEngine<E: Engine> {
    engine: E,
    txn_ext: Arc<TxnExt>,
}

impl<E: Engine> Engine for TxnTestEngine<E> {
    type Snap = TxnTestSnapshot<E::Snap>;
    type Local = E::Local;

    fn kv_engine(&self) -> Option<Self::Local> {
        self.engine.kv_engine()
    }

    fn modify_on_kv_engine(
        &self,
        region_modifies: HashMap<u64, Vec<Modify>>,
    ) -> tikv_kv::Result<()> {
        self.engine.modify_on_kv_engine(region_modifies)
    }

    type SnapshotRes = impl Future<Output = tikv_kv::Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
        let txn_ext = self.txn_ext.clone();
        let f = self.engine.async_snapshot(ctx);
        async move {
            let snapshot = f.await?;
            Ok(TxnTestSnapshot { snapshot, txn_ext })
        }
    }

    type IMSnap = Self::Snap;
    // TODO: revert this once https://github.com/rust-lang/rust/issues/140222 is fixed.
    // type IMSnapshotRes = Self::SnapshotRes;
    type IMSnapshotRes = impl Future<Output = tikv_kv::Result<Self::Snap>> + Send;
    fn async_in_memory_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::IMSnapshotRes {
        self.async_snapshot(ctx)
    }

    type WriteRes = E::WriteRes;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        subscribed: u8,
        on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        self.engine.async_write(ctx, batch, subscribed, on_applied)
    }
}

#[derive(Clone)]
pub struct TxnTestSnapshot<S: Snapshot> {
    snapshot: S,
    txn_ext: Arc<TxnExt>,
}

impl<S: Snapshot> Snapshot for TxnTestSnapshot<S> {
    type Iter = S::Iter;
    type Ext<'a>
        = TxnTestSnapshotExt<'a>
    where
        S: 'a;

    fn get(&self, key: &Key) -> tikv_kv::Result<Option<Value>> {
        self.snapshot.get(key)
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> tikv_kv::Result<Option<Value>> {
        self.snapshot.get_cf(cf, key)
    }

    fn get_cf_opt(
        &self,
        opts: engine_traits::ReadOptions,
        cf: CfName,
        key: &Key,
    ) -> tikv_kv::Result<Option<Value>> {
        self.snapshot.get_cf_opt(opts, cf, key)
    }

    fn iter(
        &self,
        cf: CfName,
        iter_opt: engine_traits::IterOptions,
    ) -> tikv_kv::Result<Self::Iter> {
        self.snapshot.iter(cf, iter_opt)
    }

    fn ext(&self) -> Self::Ext<'_> {
        TxnTestSnapshotExt(&self.txn_ext)
    }
}

pub struct TxnTestSnapshotExt<'a>(&'a Arc<TxnExt>);

impl SnapshotExt for TxnTestSnapshotExt<'_> {
    fn get_txn_ext(&self) -> Option<&Arc<TxnExt>> {
        Some(self.0)
    }
}

#[derive(Clone)]
struct DummyReporter;

impl FlowStatsReporter for DummyReporter {
    fn report_read_stats(&self, _read_stats: ReadStats) {}
    fn report_write_stats(&self, _write_stats: WriteStats) {}
}

impl<E: Engine, L: LockManager, F: KvFormat> TestStorageBuilder<E, L, F> {
    pub fn from_engine_and_lock_mgr(engine: E, lock_mgr: L) -> Self {
        let mut config = Config::default();
        config.set_api_version(F::TAG);
        Self {
            engine,
            config,
            pipelined_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            // Make it very large to avoid tests being affected by the delayed-waking-up behavior.
            wake_up_delay_duration_ms: Arc::new(AtomicU64::new(100000)),
            lock_mgr,
            resource_tag_factory: ResourceTagFactory::new_for_test(),
            _phantom: PhantomData,
            in_memory_peer_size_limit: Arc::new(AtomicU64::new(512 << 10)),
            in_memory_instance_size_limit: Arc::new(AtomicU64::new(100 << 20)),
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn pipelined_pessimistic_lock(self, enabled: bool) -> Self {
        self.pipelined_pessimistic_lock
            .store(enabled, Ordering::Relaxed);
        self
    }

    pub fn async_apply_prewrite(mut self, enabled: bool) -> Self {
        self.config.enable_async_apply_prewrite = enabled;
        self
    }

    pub fn in_memory_pessimistic_lock(self, enabled: bool) -> Self {
        self.in_memory_pessimistic_lock
            .store(enabled, Ordering::Relaxed);
        self
    }

    pub fn wake_up_delay_duration(self, duration_ms: u64) -> Self {
        self.wake_up_delay_duration_ms
            .store(duration_ms, Ordering::Relaxed);
        self
    }

    pub fn set_api_version(mut self, api_version: ApiVersion) -> Self {
        self.config.set_api_version(api_version);
        self
    }

    pub fn set_resource_tag_factory(mut self, resource_tag_factory: ResourceTagFactory) -> Self {
        self.resource_tag_factory = resource_tag_factory;
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E, L, F>> {
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            self.engine.clone(),
        );
        let ts_provider = if F::TAG == ApiVersion::V2 {
            let test_provider: causal_ts::CausalTsProviderImpl =
                causal_ts::tests::TestProvider::default().into();
            Some(Arc::new(test_provider))
        } else {
            None
        };
        let manager = Arc::new(ResourceGroupManager::default());
        let resource_ctl = manager.derive_controller("test".into(), false);
        Storage::from_engine(
            self.engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.lock_mgr,
            ConcurrencyManager::new_for_test(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_lock: self.pipelined_pessimistic_lock,
                in_memory_pessimistic_lock: self.in_memory_pessimistic_lock,
                wake_up_delay_duration_ms: self.wake_up_delay_duration_ms,
                in_memory_peer_size_limit: self.in_memory_peer_size_limit,
                in_memory_instance_size_limit: self.in_memory_instance_size_limit,
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            self.resource_tag_factory,
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            ts_provider,
            Some(resource_ctl),
            Some(manager),
            Arc::new(TxnStatusCache::new_for_test()),
        )
    }

    pub fn build_for_txn(self, txn_ext: Arc<TxnExt>) -> Result<Storage<TxnTestEngine<E>, L, F>> {
        let engine = TxnTestEngine {
            engine: self.engine,
            txn_ext,
        };
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            engine.clone(),
        );
        let manager = Arc::new(ResourceGroupManager::default());
        let resource_ctl = manager.derive_controller("test".into(), false);
        Storage::from_engine(
            engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.lock_mgr,
            ConcurrencyManager::new_for_test(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_lock: self.pipelined_pessimistic_lock,
                in_memory_pessimistic_lock: self.in_memory_pessimistic_lock,
                wake_up_delay_duration_ms: self.wake_up_delay_duration_ms,
                in_memory_peer_size_limit: self.in_memory_peer_size_limit,
                in_memory_instance_size_limit: self.in_memory_instance_size_limit,
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            None,
            Some(resource_ctl),
            Some(manager),
            Arc::new(TxnStatusCache::new_for_test()),
        )
    }

    pub fn build_for_resource_controller(
        self,
        resource_manager: Arc<ResourceGroupManager>,
        resource_controller: Arc<ResourceController>,
    ) -> Result<Storage<TxnTestEngine<E>, L, F>> {
        let engine = TxnTestEngine {
            engine: self.engine,
            txn_ext: Arc::new(TxnExt::default()),
        };
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            engine.clone(),
        );

        Storage::from_engine(
            engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.lock_mgr,
            ConcurrencyManager::new_for_test(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_lock: self.pipelined_pessimistic_lock,
                in_memory_pessimistic_lock: self.in_memory_pessimistic_lock,
                wake_up_delay_duration_ms: self.wake_up_delay_duration_ms,
                in_memory_peer_size_limit: self.in_memory_peer_size_limit,
                in_memory_instance_size_limit: self.in_memory_instance_size_limit,
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
            None,
            Some(resource_controller),
            Some(resource_manager),
            Arc::new(TxnStatusCache::new_for_test()),
        )
    }
}

pub trait ResponseBatchConsumer<ConsumeResponse: Sized>: Send {
    fn consume(
        &self,
        id: u64,
        res: Result<ConsumeResponse>,
        begin: Instant,
        request_source: String,
        resource_priority: ResourcePriority,
    );
}

pub mod test_util {
    use std::{
        fmt::Debug,
        sync::{
            Mutex,
            mpsc::{Sender, channel},
        },
    };

    use futures_executor::block_on;
    use kvproto::kvrpcpb::Op;

    use super::*;
    use crate::storage::{
        lock_manager::WaitTimeout,
        txn::commands,
        types::{PessimisticLockKeyResult, PessimisticLockResults},
    };

    pub fn expect_none(x: Option<Value>) {
        assert_eq!(x, None);
    }

    pub fn expect_value(v: Vec<u8>, x: Option<Value>) {
        assert_eq!(x.unwrap(), v);
    }

    pub fn expect_multi_values(v: Vec<Option<KvPair>>, x: Vec<Result<KvPair>>) {
        let x: Vec<Option<KvPair>> = x.into_iter().map(Result::ok).collect();
        assert_eq!(x, v);
    }

    pub fn expect_error<T, F>(err_matcher: F, x: Result<T>)
    where
        F: FnOnce(Error) + Send + 'static,
    {
        match x {
            Err(e) => err_matcher(e),
            _ => panic!("expect result to be an error"),
        }
    }

    pub fn expect_ok_callback<T: Debug>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            x.unwrap();
            done.send(id).unwrap();
        })
    }

    pub fn expect_fail_callback<T, F>(done: Sender<i32>, id: i32, err_matcher: F) -> Callback<T>
    where
        F: FnOnce(Error) + Send + 'static,
    {
        Box::new(move |x: Result<T>| {
            expect_error(err_matcher, x);
            done.send(id).unwrap();
        })
    }

    pub fn expect_too_busy_callback<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            expect_error(
                |err| match err {
                    Error(box ErrorInner::SchedTooBusy) => {}
                    e => panic!("unexpected error chain: {:?}, expect too busy", e),
                },
                x,
            );
            done.send(id).unwrap();
        })
    }

    pub fn expect_value_callback<T: PartialEq + Debug + Send + 'static>(
        done: Sender<i32>,
        id: i32,
        value: T,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert_eq!(x.unwrap(), value);
            done.send(id).unwrap();
        })
    }

    pub fn expect_value_with_checker_callback<T: 'static>(
        done: Sender<i32>,
        id: i32,
        check: impl FnOnce(T) + Send + 'static,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            check(x.unwrap());
            done.send(id).unwrap();
        })
    }

    pub fn expect_pessimistic_lock_res_callback(
        done: Sender<i32>,
        pessimistic_lock_res: PessimisticLockResults,
    ) -> Callback<Result<PessimisticLockResults>> {
        fn key_res_matches_ignoring_error_content(
            lhs: &PessimisticLockKeyResult,
            rhs: &PessimisticLockKeyResult,
        ) -> bool {
            match (lhs, rhs) {
                (PessimisticLockKeyResult::Empty, PessimisticLockKeyResult::Empty) => true,
                (PessimisticLockKeyResult::Value(l), PessimisticLockKeyResult::Value(r)) => l == r,
                (
                    PessimisticLockKeyResult::Existence(l),
                    PessimisticLockKeyResult::Existence(r),
                ) => l == r,
                (
                    PessimisticLockKeyResult::LockedWithConflict {
                        value: value1,
                        conflict_ts: ts1,
                    },
                    PessimisticLockKeyResult::LockedWithConflict {
                        value: value2,
                        conflict_ts: ts2,
                    },
                ) => value1 == value2 && ts1 == ts2,
                (PessimisticLockKeyResult::Waiting, PessimisticLockKeyResult::Waiting) => true,
                (PessimisticLockKeyResult::Failed(_), PessimisticLockKeyResult::Failed(_)) => false,
                _ => false,
            }
        }

        Box::new(move |res: Result<Result<PessimisticLockResults>>| {
            let res = res.unwrap().unwrap();
            assert_eq!(
                res.0.len(),
                pessimistic_lock_res.0.len(),
                "pessimistic lock result length not match, expected: {:?}, got: {:?}",
                pessimistic_lock_res,
                res
            );
            for (expected, got) in pessimistic_lock_res.0.iter().zip(res.0.iter()) {
                assert!(
                    key_res_matches_ignoring_error_content(expected, got),
                    "pessimistic lock result not match, expected: {:?}, got: {:?}",
                    pessimistic_lock_res,
                    res
                );
            }
            done.send(0).unwrap();
        })
    }

    pub fn expect_secondary_locks_status_callback(
        done: Sender<i32>,
        secondary_locks_status: SecondaryLocksStatus,
    ) -> Callback<SecondaryLocksStatus> {
        Box::new(move |res: Result<SecondaryLocksStatus>| {
            assert_eq!(res.unwrap(), secondary_locks_status);
            done.send(0).unwrap();
        })
    }

    type PessimisticLockCommand = TypedCommand<Result<PessimisticLockResults>>;

    impl PessimisticLockCommand {
        pub fn allow_lock_with_conflict(mut self, v: bool) -> Self {
            if let Command::AcquirePessimisticLock(commands::AcquirePessimisticLock {
                allow_lock_with_conflict,
                ..
            }) = &mut self.cmd
            {
                *allow_lock_with_conflict = v;
            } else {
                panic!(
                    "expects AcquirePessimisticLock command, got: {:?}",
                    self.cmd
                );
            }
            self
        }

        pub fn lock_wait_timeout(mut self, timeout: Option<WaitTimeout>) -> Self {
            if let Command::AcquirePessimisticLock(commands::AcquirePessimisticLock {
                wait_timeout,
                ..
            }) = &mut self.cmd
            {
                *wait_timeout = timeout;
            } else {
                panic!(
                    "expects AcquirePessimisticLock command, got: {:?}",
                    self.cmd
                );
            }
            self
        }
    }

    pub fn new_acquire_pessimistic_lock_command(
        keys: Vec<(Key, bool)>,
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        return_values: bool,
        check_existence: bool,
    ) -> PessimisticLockCommand {
        new_acquire_pessimistic_lock_command_with_pk(
            keys,
            None,
            start_ts,
            for_update_ts,
            return_values,
            check_existence,
        )
    }

    pub fn new_acquire_pessimistic_lock_command_with_pk(
        keys: Vec<(Key, bool)>,
        pk: Option<&[u8]>,
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        return_values: bool,
        check_existence: bool,
    ) -> PessimisticLockCommand {
        let primary = pk
            .map(|k| k.to_vec())
            .unwrap_or_else(|| keys[0].0.clone().to_raw().unwrap());
        let for_update_ts: TimeStamp = for_update_ts.into();
        commands::AcquirePessimisticLock::new(
            keys,
            primary,
            start_ts.into(),
            3000,
            false,
            for_update_ts,
            Some(WaitTimeout::Default),
            return_values,
            for_update_ts.next(),
            check_existence,
            false,
            false,
            Context::default(),
        )
    }

    pub fn acquire_pessimistic_lock<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        key: Key,
        start_ts: u64,
        for_update_ts: u64,
    ) {
        acquire_pessimistic_lock_impl(
            storage,
            vec![(key, false)],
            start_ts,
            for_update_ts,
            false,
            false,
        )
    }

    fn acquire_pessimistic_lock_impl<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        keys: Vec<(Key, bool)>,
        start_ts: u64,
        for_update_ts: u64,
        return_values: bool,
        check_existence: bool,
    ) {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    keys,
                    start_ts,
                    for_update_ts,
                    return_values,
                    check_existence,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[cfg(test)]
    pub fn prewrite_lock<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        key: Key,
        primary_key: &[u8],
        value: &[u8],
        start_ts: u64,
    ) {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![txn_types::Mutation::make_put(key, value.to_vec())],
                    primary_key.to_vec(),
                    start_ts.into(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    pub fn delete_pessimistic_lock<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        key: Key,
        start_ts: u64,
        for_update_ts: u64,
    ) {
        delete_pessimistic_lock_impl(storage, Some(key), start_ts, for_update_ts)
    }

    pub fn delete_pessimistic_lock_with_scan_first<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        start_ts: u64,
        for_update_ts: u64,
    ) {
        delete_pessimistic_lock_impl(storage, None, start_ts, for_update_ts)
    }

    fn delete_pessimistic_lock_impl<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        key: Option<Key>,
        start_ts: u64,
        for_update_ts: u64,
    ) {
        let (tx, rx) = channel();
        if let Some(key) = key {
            storage
                .sched_txn_command(
                    commands::PessimisticRollback::new(
                        vec![key],
                        start_ts.into(),
                        for_update_ts.into(),
                        None,
                        Context::default(),
                    ),
                    expect_ok_callback(tx, 0),
                )
                .unwrap();
        } else {
            storage
                .sched_txn_command(
                    commands::PessimisticRollbackReadPhase::new(
                        start_ts.into(),
                        for_update_ts.into(),
                        None,
                        Context::default(),
                    ),
                    expect_ok_callback(tx, 0),
                )
                .unwrap();
        };
        rx.recv().unwrap();
    }

    pub struct GetResult {
        id: u64,
        res: Result<Option<Vec<u8>>>,
    }

    #[derive(Clone)]
    pub struct GetConsumer {
        pub data: Arc<Mutex<Vec<GetResult>>>,
    }

    impl GetConsumer {
        pub fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(vec![])),
            }
        }

        pub fn take_data(self) -> Vec<Result<Option<Vec<u8>>>> {
            let mut data = self.data.lock().unwrap();
            let mut results = std::mem::take(&mut *data);
            results.sort_by_key(|k| k.id);
            results.into_iter().map(|v| v.res).collect()
        }
    }

    impl Default for GetConsumer {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics)> for GetConsumer {
        fn consume(
            &self,
            id: u64,
            res: Result<(Option<Vec<u8>>, Statistics)>,
            _: Instant,
            _source: String,
            _resource_priority: ResourcePriority,
        ) {
            self.data.lock().unwrap().push(GetResult {
                id,
                res: res.map(|(v, ..)| v),
            });
        }
    }

    impl ResponseBatchConsumer<Option<Vec<u8>>> for GetConsumer {
        fn consume(
            &self,
            id: u64,
            res: Result<Option<Vec<u8>>>,
            _: Instant,
            _source: String,
            _resource_priority: ResourcePriority,
        ) {
            self.data.lock().unwrap().push(GetResult { id, res });
        }
    }

    pub fn latest_feature_gate() -> FeatureGate {
        let feature_gate = FeatureGate::default();
        feature_gate.set_version(env!("CARGO_PKG_VERSION")).unwrap();
        feature_gate
    }

    pub fn must_have_locks<E: Engine, L: LockManager, F: KvFormat>(
        storage: &Storage<E, L, F>,
        ts: u64,
        start_key: &[u8],
        end_key: &[u8],
        expected_locks: &[(
            // key
            &[u8],
            Op,
            // start_ts
            u64,
            // for_update_ts
            u64,
        )],
    ) {
        let locks = block_on(storage.scan_lock(
            Context::default(),
            ts.into(),
            Some(Key::from_raw(start_key)),
            Some(Key::from_raw(end_key)),
            100,
        ))
        .unwrap();
        assert_eq!(
            locks.len(),
            expected_locks.len(),
            "lock count not match, expected: {:?}; got: {:?}",
            expected_locks,
            locks
        );
        for (lock_info, (expected_key, expected_op, expected_start_ts, expected_for_update_ts)) in
            locks.into_iter().zip(expected_locks.iter())
        {
            assert_eq!(lock_info.get_key(), *expected_key);
            assert_eq!(lock_info.get_lock_type(), *expected_op);
            assert_eq!(lock_info.get_lock_version(), *expected_start_ts);
            assert_eq!(lock_info.get_lock_for_update_ts(), *expected_for_update_ts);
        }
    }
}

/// All statistics related to KvGet/KvBatchGet.
#[derive(Debug, Default)]
pub struct KvGetStatistics {
    pub stats: Statistics,
    pub latency_stats: StageLatencyStats,
}

#[cfg(test)]
mod tests {
    use std::{
        iter::Iterator,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc::{Sender, channel},
        },
        thread,
        time::Duration,
    };

    use api_version::{ApiV2, test_kv_format_impl};
    use collections::HashMap;
    use engine_traits::{CF_LOCK, CF_RAFT, CF_WRITE, raw_ttl::ttl_current_ts};
    use error_code::ErrorCodeExt;
    use errors::extract_key_error;
    use futures::executor::block_on;
    use kvproto::{
        kvrpcpb::{Assertion, AssertionLevel, CommandPri, Op, PrewriteRequestPessimisticAction::*},
        metapb::RegionEpoch,
    };
    use parking_lot::Mutex;
    use tikv_util::config::ReadableSize;
    use tracker::INVALID_TRACKER_TOKEN;
    use txn_types::{LastChange, Mutation, PessimisticLock, SHORT_VALUE_MAX_LEN, WriteType};

    use super::{
        config::EngineType,
        mvcc::tests::{must_unlocked, must_written},
        test_util::*,
        txn::{
            FLASHBACK_BATCH_SIZE,
            commands::{new_flashback_rollback_lock_cmd, new_flashback_write_cmd},
        },
        *,
    };
    use crate::{
        config::TitanDbConfig,
        coprocessor::checksum_crc64_xor,
        storage::{
            config::BlockCacheConfig,
            kv::{
                Error as KvError, ErrorInner as EngineErrorInner, ExpectedWrite, MockEngineBuilder,
            },
            lock_manager::{
                CancellationCallback, DiagnosticContext, KeyLockWaitInfo, LockDigest,
                LockWaitToken, UpdateWaitForEvent, WaitTimeout,
            },
            mvcc::{LockType, tests::must_locked},
            txn::{
                Error as TxnError, ErrorInner as TxnErrorInner, commands,
                commands::{AcquirePessimisticLock, Prewrite},
                tests::must_rollback,
                txn_status_cache::TxnStatusCache,
            },
            types::{PessimisticLockKeyResult, PessimisticLockResults},
        },
    };

    #[test]
    fn test_prewrite_blocks_read() {
        use kvproto::kvrpcpb::ExtraOp;
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();

        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let mutations = vec![Mutation::make_put(Key::from_raw(b"x"), b"z".to_vec())];
        let mut cmd = commands::Prewrite::with_defaults(mutations, vec![1, 2, 3], 10.into());
        if let Command::Prewrite(p) = &mut cmd.cmd {
            p.secondary_keys = Some(vec![]);
        }
        let wr = cmd
            .cmd
            .process_write(
                snapshot,
                commands::WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: storage.concurrency_manager.clone(),
                    extra_op: ExtraOp::Noop,
                    statistics: &mut Statistics::default(),
                    async_apply_prewrite: false,
                    raw_ext: None,
                    txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
                },
            )
            .unwrap();
        assert_eq!(wr.lock_guards.len(), 1);

        let result = block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()));
        assert!(matches!(
            result,
            Err(Error(box ErrorInner::Txn(txn::Error(box txn::ErrorInner::Mvcc(mvcc::Error(
                box mvcc::ErrorInner::KeyIsLocked { .. },
            ))))))
        ));
    }

    #[test]
    fn test_get_put() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked { .. },
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into())),
        );
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cf_error() {
        // New engine lacks normal column families.
        let engine = TestEngineBuilder::new()
            .cfs([CF_DEFAULT, "foo"])
            .build()
            .unwrap();
        let storage =
            TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
                .build()
                .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 1.into())),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"x"),
                None,
                1000,
                0,
                1.into(),
                false,
                false,
            )),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.batch_get(
                Context::default(),
                vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                1.into(),
            )),
        );
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![create_get_request(b"c", 1), create_get_request(b"d", 1)],
            vec![1, 2],
            vec![INVALID_TRACKER_TOKEN; 2],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let data = consumer.take_data();
        for v in data {
            expect_error(
                |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::Kv(KvError(box EngineErrorInner::Request(..))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                },
                v,
            );
        }
    }

    #[test]
    fn test_scan() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with sample step
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step and limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), b"aa".to_vec()))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with sample step and limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), b"cc".to_vec()))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_scan_with_key_only() {
        let db_config = crate::config::DbConfig {
            titan: TitanDbConfig {
                enabled: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = {
            let path = "".to_owned();
            let cfg_rocksdb = db_config;
            let shared = cfg_rocksdb.build_cf_resources(
                BlockCacheConfig::default().build_shared_cache(),
                Default::default(),
            );
            let cfs_opts = vec![
                (
                    CF_DEFAULT,
                    cfg_rocksdb.defaultcf.build_opt(
                        &shared,
                        None,
                        ApiVersion::V1,
                        None,
                        EngineType::RaftKv,
                    ),
                ),
                (
                    CF_LOCK,
                    cfg_rocksdb
                        .lockcf
                        .build_opt(&shared, None, EngineType::RaftKv),
                ),
                (
                    CF_WRITE,
                    cfg_rocksdb
                        .writecf
                        .build_opt(&shared, None, None, EngineType::RaftKv),
                ),
                (CF_RAFT, cfg_rocksdb.raftcf.build_opt(&shared)),
            ];
            RocksEngine::new(
                &path, None, cfs_opts, None, // io_rate_limiter
            )
        }
        .unwrap();
        let storage =
            TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
                .build()
                .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"c".to_vec(), vec![])),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"a".to_vec(), vec![])),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_batch_get() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![None],
            block_on(storage.batch_get(
                Context::default(),
                vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                2.into(),
            ))
            .unwrap()
            .0,
        );
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.batch_get(
                Context::default(),
                vec![
                    Key::from_raw(b"c"),
                    Key::from_raw(b"x"),
                    Key::from_raw(b"a"),
                    Key::from_raw(b"b"),
                ],
                5.into(),
            ))
            .unwrap()
            .0,
        );
    }

    fn create_get_request(key: &[u8], start_ts: u64) -> GetRequest {
        let mut req = GetRequest::default();
        req.set_key(key.to_owned());
        req.set_version(start_ts);
        req
    }

    #[test]
    fn test_batch_get_command() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![create_get_request(b"c", 2), create_get_request(b"d", 2)],
            vec![1, 2],
            vec![INVALID_TRACKER_TOKEN; 2],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let mut x = consumer.take_data();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked(..),
                ))))) => {}
                e => panic!("unexpected error chain: {:?}", e),
            },
            x.remove(0),
        );
        assert_eq!(x.remove(0).unwrap(), None);
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![
                create_get_request(b"c", 5),
                create_get_request(b"x", 5),
                create_get_request(b"a", 5),
                create_get_request(b"b", 5),
            ],
            vec![1, 2, 3, 4],
            vec![INVALID_TRACKER_TOKEN; 4],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();

        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(
            x,
            vec![
                Some(b"cc".to_vec()),
                None,
                Some(b"aa".to_vec()),
                Some(b"bb".to_vec()),
            ]
        );
    }

    #[test]
    fn test_txn() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"y"), b"101".to_vec())],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    110.into(),
                    None,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 2, TxnStatus::committed(110.into())),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"y")],
                    101.into(),
                    111.into(),
                    None,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 3, TxnStatus::committed(111.into())),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 120.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"101".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 120.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"105".to_vec())],
                    b"x".to_vec(),
                    105.into(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let config = Config {
            scheduler_pending_write_threshold: ReadableSize(1),
            ..Default::default()
        };
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command::<()>(
                commands::Pause::new(vec![Key::from_raw(b"x")], 1000, Context::default()),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"y"), b"101".to_vec())],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_too_busy_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"z"), b"102".to_vec())],
                    b"y".to_vec(),
                    102.into(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    100.into(),
                    TimeStamp::zero(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(cm.max_ts(), 100.into());
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 105.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cleanup_check_ttl() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let ts = TimeStamp::compose;
        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"110".to_vec())],
                    b"x".to_vec(),
                    ts(110, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    ts(110, 0),
                    ts(120, 0),
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::KeyIsLocked(info),
                    ))))) => assert_eq!(info.get_lock_ttl(), 100),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    ts(110, 0),
                    ts(220, 0),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), ts(230, 0)))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_flashback_to_version() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let mut ts = TimeStamp::zero();
        let writes = vec![
            // (Mutation, StartTS, CommitTS)
            (
                Mutation::Put((Key::from_raw(b"k"), b"v@1".to_vec()), Assertion::None),
                *ts.incr(),
                *ts.incr(),
            ),
            (
                Mutation::Put((Key::from_raw(b"k"), b"v@3".to_vec()), Assertion::None),
                *ts.incr(),
                *ts.incr(),
            ),
            (
                Mutation::Put((Key::from_raw(b"k"), b"v@5".to_vec()), Assertion::None),
                *ts.incr(),
                *ts.incr(),
            ),
            (
                Mutation::Put((Key::from_raw(b"k"), b"v@7".to_vec()), Assertion::None),
                *ts.incr(),
                *ts.incr(),
            ),
            (
                Mutation::Delete(Key::from_raw(b"k"), Assertion::None),
                *ts.incr(),
                *ts.incr(),
            ),
            (
                Mutation::Put((Key::from_raw(b"k"), b"v@11".to_vec()), Assertion::None),
                *ts.incr(),
                *ts.incr(),
            ),
            // Non-short value
            (
                Mutation::Put(
                    (Key::from_raw(b"k"), vec![b'v'; SHORT_VALUE_MAX_LEN + 1]),
                    Assertion::None,
                ),
                *ts.incr(),
                *ts.incr(),
            ),
        ];
        let (tx, rx) = channel();
        // Prewrite and commit.
        for write in writes.iter() {
            let (key, value) = write.0.clone().into_key_value();
            let start_ts = write.1;
            let commit_ts = write.2;
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        vec![write.0.clone()],
                        key.clone().to_raw().unwrap(),
                        start_ts,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![key.clone()],
                        start_ts,
                        commit_ts,
                        None,
                        Context::default(),
                    ),
                    expect_value_callback(tx.clone(), 1, TxnStatus::committed(commit_ts)),
                )
                .unwrap();
            rx.recv().unwrap();
            if let Mutation::Put(..) = write.0 {
                expect_value(
                    value.unwrap(),
                    block_on(storage.get(Context::default(), key.clone(), commit_ts))
                        .unwrap()
                        .0,
                );
            } else {
                expect_none(
                    block_on(storage.get(Context::default(), key, commit_ts))
                        .unwrap()
                        .0,
                );
            }
        }
        // Flashback.
        for write in writes {
            let start_ts = *ts.incr();
            let commit_ts = *ts.incr();
            let (key, value) = write.0.clone().into_key_value();
            // The version we want to flashback to.
            let version = write.2;
            run_flashback_to_version(
                &storage,
                start_ts,
                commit_ts,
                version,
                key.clone(),
                Some(Key::from_raw(b"z")),
            );
            if let Mutation::Put(..) = write.0 {
                expect_value(
                    value.unwrap(),
                    block_on(storage.get(Context::default(), key.clone(), commit_ts))
                        .unwrap()
                        .0,
                );
            } else {
                expect_none(
                    block_on(storage.get(Context::default(), key, commit_ts))
                        .unwrap()
                        .0,
                );
            }
        }
    }

    fn run_flashback_to_version<F: KvFormat>(
        storage: &Storage<RocksEngine, MockLockManager, F>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        version: TimeStamp,
        start_key: Key,
        end_key: Option<Key>,
    ) {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                new_flashback_rollback_lock_cmd(
                    start_ts,
                    version,
                    start_key.clone(),
                    end_key.clone(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                new_flashback_write_cmd(
                    start_ts,
                    commit_ts,
                    version,
                    start_key,
                    end_key,
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_flashback_to_version_lock() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ts = TimeStamp::zero();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"k"), b"v@1".to_vec())],
                    b"k".to_vec(),
                    *ts.incr(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k")],
                    ts,
                    *ts.incr(),
                    None,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 1, TxnStatus::committed(ts)),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"v@1".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), ts))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"k"), b"v@3".to_vec())],
                    b"k".to_vec(),
                    *ts.incr(),
                ),
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked { .. },
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), *ts.incr())),
        );

        let start_ts = *ts.incr();
        let commit_ts = *ts.incr();
        run_flashback_to_version(
            &storage,
            start_ts,
            commit_ts,
            2.into(),
            Key::from_raw(b"k"),
            Some(Key::from_raw(b"z")),
        );
        expect_value(
            b"v@1".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), commit_ts))
                .unwrap()
                .0,
        );
        let start_ts = *ts.incr();
        let commit_ts = *ts.incr();
        run_flashback_to_version(
            &storage,
            start_ts,
            commit_ts,
            1.into(),
            Key::from_raw(b"k"),
            Some(Key::from_raw(b"z")),
        );
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), commit_ts))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_flashback_to_version_in_multi_batch() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ts = TimeStamp::zero();
        // Add (FLASHBACK_BATCH_SIZE * 2) lock records.
        for i in 1..=FLASHBACK_BATCH_SIZE * 2 {
            let start_ts = *ts.incr();
            let key = Key::from_raw(format!("k{}", i).as_bytes());
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        vec![Mutation::make_put(
                            key.clone(),
                            format!("v@{}", i).as_bytes().to_vec(),
                        )],
                        key.to_raw().unwrap(),
                        start_ts,
                    ),
                    expect_ok_callback(tx.clone(), i as i32),
                )
                .unwrap();
            rx.recv().unwrap();
            expect_error(
                |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::KeyIsLocked { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                },
                block_on(storage.get(Context::default(), key, start_ts)),
            );
        }
        // Add (FLASHBACK_BATCH_SIZE * 2) write records.
        for i in FLASHBACK_BATCH_SIZE * 2 + 1..=FLASHBACK_BATCH_SIZE * 4 {
            let start_ts = *ts.incr();
            let commit_ts = *ts.incr();
            let key = Key::from_raw(format!("k{}", i).as_bytes());
            let value = format!("v@{}", i).as_bytes().to_vec();
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        vec![Mutation::make_put(key.clone(), value.clone())],
                        key.to_raw().unwrap(),
                        start_ts,
                    ),
                    expect_ok_callback(tx.clone(), i as i32),
                )
                .unwrap();
            rx.recv().unwrap();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![key.clone()],
                        start_ts,
                        commit_ts,
                        None,
                        Context::default(),
                    ),
                    expect_value_callback(tx.clone(), i as i32, TxnStatus::committed(commit_ts)),
                )
                .unwrap();
            rx.recv().unwrap();
            expect_value(
                value,
                block_on(storage.get(Context::default(), key, commit_ts))
                    .unwrap()
                    .0,
            );
        }
        // Flashback all records multiple times to make sure the flashback operation is
        // idempotent.
        let flashback_start_ts = *ts.incr();
        let flashback_commit_ts = *ts.incr();
        for _ in 0..10 {
            run_flashback_to_version(
                &storage,
                flashback_start_ts,
                flashback_commit_ts,
                TimeStamp::zero(),
                Key::from_raw(b"k"),
                Some(Key::from_raw(b"z")),
            );
            for i in 1..=FLASHBACK_BATCH_SIZE * 4 {
                let key = Key::from_raw(format!("k{}", i).as_bytes());
                expect_none(
                    block_on(storage.get(Context::default(), key, *ts.incr()))
                        .unwrap()
                        .0,
                );
            }
        }
    }

    #[test]
    fn test_flashback_to_version_deleted_key() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ts = TimeStamp::zero();
        let (k, v) = (Key::from_raw(b"k"), b"v".to_vec());
        // Write a key.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    k.as_encoded().to_vec(),
                    *ts.incr(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(vec![k.clone()], ts, *ts.incr(), None, Context::default()),
                expect_value_callback(tx.clone(), 1, TxnStatus::committed(ts)),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            v,
            block_on(storage.get(Context::default(), k.clone(), ts))
                .unwrap()
                .0,
        );
        // Delete the key.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_delete(k.clone())],
                    k.as_encoded().to_vec(),
                    *ts.incr(),
                ),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(vec![k.clone()], ts, *ts.incr(), None, Context::default()),
                expect_value_callback(tx, 3, TxnStatus::committed(ts)),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), ts))
                .unwrap()
                .0,
        );
        // Flashback the key.
        let flashback_start_ts = *ts.incr();
        let flashback_commit_ts = *ts.incr();
        run_flashback_to_version(
            &storage,
            flashback_start_ts,
            flashback_commit_ts,
            1.into(),
            Key::from_raw(b"k"),
            Some(Key::from_raw(b"z")),
        );
        expect_none(
            block_on(storage.get(Context::default(), k, flashback_commit_ts))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_mvcc_flashback_retry_prepare() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ts = TimeStamp::zero();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"k"), b"v@1".to_vec())],
                    b"k".to_vec(),
                    *ts.incr(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k")],
                    ts,
                    *ts.incr(),
                    None,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 1, TxnStatus::committed(ts)),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"v@1".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), ts))
                .unwrap()
                .0,
        );
        // Try to prepare flashback first.
        let flashback_start_ts = *ts.incr();
        let flashback_commit_ts = *ts.incr();
        storage
            .sched_txn_command(
                new_flashback_rollback_lock_cmd(
                    flashback_start_ts,
                    TimeStamp::zero(),
                    Key::from_raw(b"k"),
                    Some(Key::from_raw(b"z")),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Mock the prepare flashback retry.
        run_flashback_to_version(
            &storage,
            flashback_start_ts,
            flashback_commit_ts,
            TimeStamp::zero(),
            Key::from_raw(b"k"),
            Some(Key::from_raw(b"z")),
        );
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"k"), flashback_commit_ts))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            block_on(storage.get(ctx, Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_txn_command(
                commands::Prewrite::with_context(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                    ctx,
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_txn_command(
                commands::Commit::new(vec![Key::from_raw(b"x")], 100.into(), 101.into(), None, ctx),
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            block_on(storage.get(ctx, Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(ctx, Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_no_block() {
        let config = Config {
            scheduler_worker_pool_size: 1,
            ..Default::default()
        };
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Pause::new(vec![Key::from_raw(b"y")], 1000, Context::default()),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(ctx, Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        // Command Get with high priority not block by command Pause.
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_delete_range() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        // Write x and y.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"x"), b"100".to_vec()),
                        Mutation::make_put(Key::from_raw(b"y"), b"100".to_vec()),
                        Mutation::make_put(Key::from_raw(b"z"), b"100".to_vec()),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"x"),
                        Key::from_raw(b"y"),
                        Key::from_raw(b"z"),
                    ],
                    100.into(),
                    101.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );

        // Delete range [x, z)
        storage
            .delete_range(
                Context::default(),
                Key::from_raw(b"x"),
                Key::from_raw(b"z"),
                false,
                expect_ok_callback(tx.clone(), 5),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );

        storage
            .delete_range(
                Context::default(),
                Key::from_raw(b""),
                Key::from_raw(&[255]),
                false,
                expect_ok_callback(tx, 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_raw_get_put() {
        test_kv_format_impl!(test_raw_get_put_impl);
    }

    fn test_raw_get_put_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
            (b"r\0f".to_vec(), b"ff".to_vec()),
        ];

        // Write key-value pairs one by one
        for (key, value) in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        for (k, v) in test_data {
            expect_value(
                v,
                block_on(storage.raw_get(ctx.clone(), "".to_string(), k)).unwrap(),
            );
        }
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );
    }

    #[test]
    fn test_raw_checksum() {
        test_kv_format_impl!(test_raw_checksum_impl);
    }

    fn test_raw_checksum_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
            (b"r\0f".to_vec(), b"ff".to_vec()),
        ];

        let digest = crc64fast::Digest::new();
        let mut checksum: u64 = 0;
        let mut total_kvs: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut is_first = true;
        // Write key-value pairs one by one
        for (key, value) in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            // start key is set to b"r\0a\0", if raw_checksum does not encode the key,
            // first key will be included in checksum. This is for testing issue #12950.
            if !is_first {
                total_kvs += 1;
                total_bytes += (key.len() + value.len()) as u64;
                checksum = checksum_crc64_xor(checksum, digest.clone(), key, value);
            }
            is_first = false;
            rx.recv().unwrap();
        }
        let mut range = KeyRange::default();
        range.set_start_key(b"r\0a\0".to_vec());
        range.set_end_key(b"r\0z".to_vec());
        assert_eq!(
            (checksum, total_kvs, total_bytes),
            block_on(storage.raw_checksum(ctx, ChecksumAlgorithm::Crc64Xor, vec![range])).unwrap(),
        );
    }

    #[test]
    fn test_raw_v2_multi_versions() {
        // Test update on the same key to verify multi-versions implementation of RawKV
        // V2.
        let test_data = vec![
            Some(b"v1".to_vec()),
            Some(b"v2".to_vec()),
            None,
            Some(b"".to_vec()),
            Some(b"v3".to_vec()),
        ];
        let k = b"r\0k".to_vec();

        let storage = TestStorageBuilder::<_, _, ApiV2>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: ApiVersion::V2,
            ..Default::default()
        };

        let last_data = test_data
            .last()
            .unwrap()
            .as_ref()
            .map(|x| (k.clone(), x.clone()));
        for v in test_data {
            if let Some(v) = v {
                storage
                    .raw_put(
                        ctx.clone(),
                        "".to_string(),
                        k.clone(),
                        v.clone(),
                        0,
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                expect_value(
                    v.clone(),
                    block_on(storage.raw_get(ctx.clone(), "".to_string(), k.clone())).unwrap(),
                );
            } else {
                storage
                    .raw_delete(
                        ctx.clone(),
                        "".to_string(),
                        k.clone(),
                        expect_ok_callback(tx.clone(), 1),
                    )
                    .unwrap();
                rx.recv().unwrap();

                expect_none(
                    block_on(storage.raw_get(ctx.clone(), "".to_string(), k.clone())).unwrap(),
                );
            }
        }
        // Verify by `raw_scan`. As `raw_scan` will check timestamp in keys.
        expect_multi_values(
            vec![last_data],
            block_on(storage.raw_scan(
                ctx,
                "".to_string(),
                b"r".to_vec(),
                Some(b"rz".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_raw_delete() {
        test_kv_format_impl!(test_raw_delete_impl);
    }

    fn test_raw_delete_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = [
            (b"r\0a", b"001"),
            (b"r\0b", b"002"),
            (b"r\0c", b"003"),
            (b"r\0d", b"004"),
            (b"r\0e", b"005"),
        ];

        // Write some key-value pairs to the db
        for kv in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    kv.0.to_vec(),
                    kv.1.to_vec(),
                    if !F::IS_TTL_ENABLED { 0 } else { 30 },
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        expect_value(
            b"004".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );

        // Delete "a"
        storage
            .raw_delete(
                ctx.clone(),
                "".to_string(),
                b"r\0a".to_vec(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // Assert key "a" has gone
        expect_none(
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0a".to_vec())).unwrap(),
        );

        // Delete all
        for kv in &test_data {
            storage
                .raw_delete(
                    ctx.clone(),
                    "".to_string(),
                    kv.0.to_vec(),
                    expect_ok_callback(tx.clone(), 1),
                )
                .unwrap();
            rx.recv().unwrap();
        }
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // Assert now no key remains
        for kv in &test_data {
            expect_none(
                block_on(storage.raw_get(ctx.clone(), "".to_string(), kv.0.to_vec())).unwrap(),
            );
        }
    }

    #[test]
    fn test_raw_delete_range() {
        test_kv_format_impl!(test_raw_delete_range_impl);
    }

    fn test_raw_delete_range_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = [
            (b"r\0a", b"001"),
            (b"r\0b", b"002"),
            (b"r\0c", b"003"),
            (b"r\0d", b"004"),
            (b"r\0e", b"005"),
        ];

        // Write some key-value pairs to the db
        for kv in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    kv.0.to_vec(),
                    kv.1.to_vec(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        expect_value(
            b"004".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );

        // Delete ["d", "e")
        storage
            .raw_delete_range(
                ctx.clone(),
                "".to_string(),
                b"r\0d".to_vec(),
                b"r\0e".to_vec(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert key "d" has gone
        expect_value(
            b"003".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0c".to_vec())).unwrap(),
        );
        expect_none(
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );
        expect_value(
            b"005".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0e".to_vec())).unwrap(),
        );

        // Delete ["aa", "ab")
        storage
            .raw_delete_range(
                ctx.clone(),
                "".to_string(),
                b"r\0aa".to_vec(),
                b"r\0ab".to_vec(),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert nothing happened
        expect_value(
            b"001".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0a".to_vec())).unwrap(),
        );
        expect_value(
            b"002".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0b".to_vec())).unwrap(),
        );

        // Delete all
        storage
            .raw_delete_range(
                ctx.clone(),
                "".to_string(),
                b"r\0a".to_vec(),
                b"r\0z".to_vec(),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert now no key remains
        for kv in &test_data {
            expect_none(
                block_on(storage.raw_get(ctx.clone(), "".to_string(), kv.0.to_vec())).unwrap(),
            );
        }
    }

    #[test]
    fn test_raw_batch_put() {
        for for_cas in vec![false, true].into_iter() {
            test_kv_format_impl!(test_raw_batch_put_impl(for_cas));
        }
    }

    fn run_raw_batch_put<F: KvFormat>(
        for_cas: bool,
        storage: &Storage<RocksEngine, MockLockManager, F>,
        ctx: Context,
        kvpairs: Vec<KvPair>,
        ttls: Vec<u64>,
        cb: Callback<()>,
    ) -> Result<()> {
        if for_cas {
            storage.raw_batch_put_atomic(ctx, "".to_string(), kvpairs, ttls, cb)
        } else {
            storage.raw_batch_put(ctx, "".to_string(), kvpairs, ttls, cb)
        }
    }

    fn test_raw_batch_put_impl<F: KvFormat>(for_cas: bool) {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let empty_key = if F::TAG == ApiVersion::V2 {
            b"r".to_vec()
        } else {
            b"".to_vec()
        };
        let test_data = vec![
            (empty_key.clone(), b"ff".to_vec(), 10), // empty key
            (b"r\0a".to_vec(), b"aa".to_vec(), 10),
            (b"r\0b".to_vec(), b"bb".to_vec(), 20),
            (b"r\0c".to_vec(), b"cc".to_vec(), 30),
            (b"r\0d".to_vec(), b"dd".to_vec(), 0),
            (b"r\0e".to_vec(), b"ee".to_vec(), 40),
            (b"r\0g".to_vec(), b"".to_vec(), 50), // empty value
        ];

        let kvpairs = test_data
            .clone()
            .into_iter()
            .map(|(key, value, _)| (key, value))
            .collect();
        let ttls = if F::IS_TTL_ENABLED {
            test_data
                .clone()
                .into_iter()
                .map(|(_, _, ttl)| ttl)
                .collect()
        } else {
            vec![0; test_data.len()]
        };
        // Write key-value pairs in a batch
        run_raw_batch_put(
            for_cas,
            &storage,
            ctx.clone(),
            kvpairs,
            ttls,
            expect_ok_callback(tx, 0),
        )
        .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // Verify pairs one by one
        for (key, val, _) in &test_data {
            expect_value(
                val.to_vec(),
                block_on(storage.raw_get(ctx.clone(), "".to_string(), key.to_vec())).unwrap(),
            );
        }
        // Verify by `raw_scan`. As `raw_scan` will check timestamp in keys.
        let expected = test_data
            .iter()
            .map(|(k, v, _)| Some((k.clone(), v.clone())))
            .collect();
        expect_multi_values(
            expected,
            block_on(storage.raw_scan(
                ctx,
                "".to_string(),
                empty_key,
                Some(b"rz".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_raw_batch_get() {
        test_kv_format_impl!(test_raw_batch_get_impl);
    }

    fn test_raw_batch_get_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs one by one
        for (key, value) in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        // Verify pairs in a batch
        let keys = test_data.iter().map(|(k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            block_on(storage.raw_batch_get(ctx, "".to_string(), keys)).unwrap(),
        );
    }

    #[test]
    fn test_raw_batch_get_command() {
        test_kv_format_impl!(test_raw_batch_get_command_impl);
    }

    fn test_raw_batch_get_command_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs one by one
        for (key, value) in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        // Verify pairs in a batch
        let mut ids = vec![];
        let cmds = test_data
            .iter()
            .map(|(k, _)| {
                let mut req = RawGetRequest::default();
                req.set_context(ctx.clone());
                req.set_key(k.clone());
                ids.push(ids.len() as u64);
                req
            })
            .collect();
        let results: Vec<Option<Vec<u8>>> = test_data.into_iter().map(|(_, v)| Some(v)).collect();
        let consumer = GetConsumer::new();
        block_on(storage.raw_batch_get_command(cmds, ids, consumer.clone())).unwrap();
        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(x, results);
    }

    #[test]
    fn test_raw_batch_delete() {
        for for_cas in vec![false, true].into_iter() {
            test_kv_format_impl!(test_raw_batch_delete_impl(for_cas));
        }
    }

    fn run_raw_batch_delete<F: KvFormat>(
        for_cas: bool,
        storage: &Storage<RocksEngine, MockLockManager, F>,
        ctx: Context,
        keys: Vec<Vec<u8>>,
        cb: Callback<()>,
    ) -> Result<()> {
        if for_cas {
            storage.raw_batch_delete_atomic(ctx, "".to_string(), keys, cb)
        } else {
            storage.raw_batch_delete(ctx, "".to_string(), keys, cb)
        }
    }

    fn test_raw_batch_delete_impl<F: KvFormat>(for_cas: bool) {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs in batch
        run_raw_batch_put(
            for_cas,
            &storage,
            ctx.clone(),
            test_data.clone(),
            vec![0; test_data.len()],
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|(k, _)| k.clone()).collect();
        let results = test_data
            .iter()
            .map(|(k, v)| Some((k.clone(), v.clone())))
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_batch_get(ctx.clone(), "".to_string(), keys)).unwrap(),
        );

        // Delete ["b", "d"]
        run_raw_batch_delete(
            for_cas,
            &storage,
            ctx.clone(),
            vec![b"r\0b".to_vec(), b"r\0d".to_vec()],
            expect_ok_callback(tx.clone(), 1),
        )
        .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // Assert "b" and "d" are gone
        expect_value(
            b"aa".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0a".to_vec())).unwrap(),
        );
        expect_none(
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0b".to_vec())).unwrap(),
        );
        expect_value(
            b"cc".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0c".to_vec())).unwrap(),
        );
        expect_none(
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );
        expect_value(
            b"ee".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), b"r\0e".to_vec())).unwrap(),
        );

        // Delete ["a", "c", "e"]
        run_raw_batch_delete(
            for_cas,
            &storage,
            ctx.clone(),
            vec![b"r\0a".to_vec(), b"r\0c".to_vec(), b"r\0e".to_vec()],
            expect_ok_callback(tx, 2),
        )
        .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // Assert no key remains
        for (k, _) in test_data {
            expect_none(block_on(storage.raw_get(ctx.clone(), "".to_string(), k)).unwrap());
        }
    }

    #[test]
    fn test_raw_scan() {
        test_kv_format_impl!(test_raw_scan_impl);
    }

    fn test_raw_scan_impl<F: KvFormat>() {
        let (end_key, end_key_reverse_scan) = if let ApiVersion::V2 = F::TAG {
            (Some(b"r\0z".to_vec()), Some(b"r\0\0".to_vec()))
        } else {
            (None, None)
        };

        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0a1".to_vec(), b"aa11".to_vec()),
            (b"r\0a2".to_vec(), b"aa22".to_vec()),
            (b"r\0a3".to_vec(), b"aa33".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0b1".to_vec(), b"bb11".to_vec()),
            (b"r\0b2".to_vec(), b"bb22".to_vec()),
            (b"r\0b3".to_vec(), b"bb33".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0c1".to_vec(), b"cc11".to_vec()),
            (b"r\0c2".to_vec(), b"cc22".to_vec()),
            (b"r\0c3".to_vec(), b"cc33".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0d1".to_vec(), b"dd11".to_vec()),
            (b"r\0d2".to_vec(), b"dd22".to_vec()),
            (b"r\0d3".to_vec(), b"dd33".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
            (b"r\0e1".to_vec(), b"ee11".to_vec()),
            (b"r\0e2".to_vec(), b"ee22".to_vec()),
            (b"r\0e3".to_vec(), b"ee33".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .raw_batch_put(
                ctx.clone(),
                "".to_string(),
                test_data.clone(),
                vec![0; test_data.len()],
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Scan pairs with key only
        let mut results: Vec<Option<KvPair>> = test_data
            .iter()
            .map(|(k, _)| Some((k.clone(), vec![])))
            .collect();
        expect_multi_values(
            results.clone(),
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0".to_vec(),
                end_key.clone(),
                20,
                true,
                false,
            ))
            .unwrap(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0c2".to_vec(),
                end_key.clone(),
                20,
                true,
                false,
            ))
            .unwrap(),
        );
        let mut results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results.clone(),
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0".to_vec(),
                end_key.clone(),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0c2".to_vec(),
                end_key,
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0z".to_vec(),
                end_key_reverse_scan.clone(),
                20,
                false,
                true,
            ))
            .unwrap(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .take(5)
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0z".to_vec(),
                end_key_reverse_scan,
                5,
                false,
                true,
            ))
            .unwrap(),
        );

        // Scan with end_key
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .skip(6)
            .take(4)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0b2".to_vec(),
                Some(b"r\0c2".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0b2".to_vec(),
                Some(b"r\0b2\x00".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );

        // Reverse scan with end_key
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .rev()
            .skip(10)
            .take(4)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0c2".to_vec(),
                Some(b"r\0b2".to_vec()),
                20,
                false,
                true,
            ))
            .unwrap(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r\0b2\x00".to_vec(),
                Some(b"r\0b2".to_vec()),
                20,
                false,
                true,
            ))
            .unwrap(),
        );

        // End key tests. Confirm that lower/upper bound works correctly.
        let results = vec![
            (b"r\0c1".to_vec(), b"cc11".to_vec()),
            (b"r\0c2".to_vec(), b"cc22".to_vec()),
            (b"r\0c3".to_vec(), b"cc33".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0d1".to_vec(), b"dd11".to_vec()),
            (b"r\0d2".to_vec(), b"dd22".to_vec()),
        ]
        .into_iter()
        .map(|(k, v)| Some((k, v)));
        expect_multi_values(
            results.clone().collect(),
            block_on(async {
                storage
                    .raw_scan(
                        ctx.clone(),
                        "".to_string(),
                        b"r\0c1".to_vec(),
                        Some(b"r\0d3".to_vec()),
                        20,
                        false,
                        false,
                    )
                    .await
            })
            .unwrap(),
        );
        expect_multi_values(
            results.rev().collect(),
            block_on(async {
                storage
                    .raw_scan(
                        ctx.clone(),
                        "".to_string(),
                        b"r\0d3".to_vec(),
                        Some(b"r\0c1".to_vec()),
                        20,
                        false,
                        true,
                    )
                    .await
            })
            .unwrap(),
        );
    }

    #[test]
    fn test_check_key_ranges() {
        fn make_ranges(ranges: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<KeyRange> {
            ranges
                .into_iter()
                .map(|(s, e)| {
                    let mut range = KeyRange::default();
                    range.set_start_key(s);
                    if !e.is_empty() {
                        range.set_end_key(e);
                    }
                    range
                })
                .collect()
        }

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        // TODO: refactor to use `Api` parameter.
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"c".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false),
            true
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false),
            false
        );

        // if end_key is omitted, the next start_key is used instead. so, false is
        // returned.
        let ranges = make_ranges(vec![
            (b"c".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"a".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, false),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true),
            true
        );

        let ranges = make_ranges(vec![
            (b"c3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"a3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"c3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <StorageApiV1<RocksEngine, MockLockManager>>::check_key_ranges(&ranges, true),
            false
        );
    }

    #[test]
    fn test_raw_batch_scan() {
        test_kv_format_impl!(test_raw_batch_scan_impl);
    }

    fn test_raw_batch_scan_impl<F: KvFormat>() {
        let make_ranges = |delimiters: Vec<Vec<u8>>| -> Vec<KeyRange> {
            delimiters
                .windows(2)
                .map(|key_pair| {
                    let mut range = KeyRange::default();
                    range.set_start_key(key_pair[0].clone());
                    if let ApiVersion::V2 = F::TAG {
                        range.set_end_key(key_pair[1].clone());
                    };
                    range
                })
                .collect()
        };

        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0a1".to_vec(), b"aa11".to_vec()),
            (b"r\0a2".to_vec(), b"aa22".to_vec()),
            (b"r\0a3".to_vec(), b"aa33".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0b1".to_vec(), b"bb11".to_vec()),
            (b"r\0b2".to_vec(), b"bb22".to_vec()),
            (b"r\0b3".to_vec(), b"bb33".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0c1".to_vec(), b"cc11".to_vec()),
            (b"r\0c2".to_vec(), b"cc22".to_vec()),
            (b"r\0c3".to_vec(), b"cc33".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0d1".to_vec(), b"dd11".to_vec()),
            (b"r\0d2".to_vec(), b"dd22".to_vec()),
            (b"r\0d3".to_vec(), b"dd33".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
            (b"r\0e1".to_vec(), b"ee11".to_vec()),
            (b"r\0e2".to_vec(), b"ee22".to_vec()),
            (b"r\0e3".to_vec(), b"ee33".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .raw_batch_put(
                ctx.clone(),
                "".to_string(),
                test_data.clone(),
                vec![0; test_data.len()],
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|(k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            block_on(storage.raw_batch_get(ctx.clone(), "".to_string(), keys)).unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), b"aa".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0a3".to_vec(), b"aa33".to_vec())),
            Some((b"r\0b".to_vec(), b"bb".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0b3".to_vec(), b"bb33".to_vec())),
            Some((b"r\0c".to_vec(), b"cc".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
            Some((b"r\0c3".to_vec(), b"cc33".to_vec())),
            Some((b"r\0d".to_vec(), b"dd".to_vec())),
        ];
        let ranges: Vec<KeyRange> = make_ranges(vec![
            b"r\0a".to_vec(),
            b"r\0b".to_vec(),
            b"r\0c".to_vec(),
            b"r\0z".to_vec(),
        ]);
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(
                ctx.clone(),
                "".to_string(),
                ranges.clone(),
                5,
                false,
                false,
            ))
            .unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), vec![])),
            Some((b"r\0a1".to_vec(), vec![])),
            Some((b"r\0a2".to_vec(), vec![])),
            Some((b"r\0a3".to_vec(), vec![])),
            Some((b"r\0b".to_vec(), vec![])),
            Some((b"r\0b1".to_vec(), vec![])),
            Some((b"r\0b2".to_vec(), vec![])),
            Some((b"r\0b3".to_vec(), vec![])),
            Some((b"r\0c".to_vec(), vec![])),
            Some((b"r\0c1".to_vec(), vec![])),
            Some((b"r\0c2".to_vec(), vec![])),
            Some((b"r\0c3".to_vec(), vec![])),
            Some((b"r\0d".to_vec(), vec![])),
        ];
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(
                ctx.clone(),
                "".to_string(),
                ranges.clone(),
                5,
                true,
                false,
            ))
            .unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), b"aa".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0b".to_vec(), b"bb".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0c".to_vec(), b"cc".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
        ];
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(
                ctx.clone(),
                "".to_string(),
                ranges.clone(),
                3,
                false,
                false,
            ))
            .unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), vec![])),
            Some((b"r\0a1".to_vec(), vec![])),
            Some((b"r\0a2".to_vec(), vec![])),
            Some((b"r\0b".to_vec(), vec![])),
            Some((b"r\0b1".to_vec(), vec![])),
            Some((b"r\0b2".to_vec(), vec![])),
            Some((b"r\0c".to_vec(), vec![])),
            Some((b"r\0c1".to_vec(), vec![])),
            Some((b"r\0c2".to_vec(), vec![])),
        ];
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(ctx.clone(), "".to_string(), ranges, 3, true, false))
                .unwrap(),
        );

        let results = vec![
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
            Some((b"r\0a".to_vec(), b"aa".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0b".to_vec(), b"bb".to_vec())),
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0c".to_vec(), b"cc".to_vec())),
        ];
        let ranges: Vec<KeyRange> = vec![
            (b"r\0a3".to_vec(), b"r\0a".to_vec()),
            (b"r\0b3".to_vec(), b"r\0b".to_vec()),
            (b"r\0c3".to_vec(), b"r\0c".to_vec()),
        ]
        .into_iter()
        .map(|(s, e)| {
            let mut range = KeyRange::default();
            range.set_start_key(s);
            range.set_end_key(e);
            range
        })
        .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(ctx.clone(), "".to_string(), ranges, 5, false, true))
                .unwrap(),
        );

        let results = vec![
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
        ];
        let ranges: Vec<KeyRange> = make_ranges(vec![
            b"r\0c3".to_vec(),
            b"r\0b3".to_vec(),
            b"r\0a3".to_vec(),
            b"r\0".to_vec(),
        ]);
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(ctx.clone(), "".to_string(), ranges, 2, false, true))
                .unwrap(),
        );

        let results = vec![
            Some((b"r\0a2".to_vec(), vec![])),
            Some((b"r\0a1".to_vec(), vec![])),
            Some((b"r\0a".to_vec(), vec![])),
            Some((b"r\0b2".to_vec(), vec![])),
            Some((b"r\0b1".to_vec(), vec![])),
            Some((b"r\0b".to_vec(), vec![])),
            Some((b"r\0c2".to_vec(), vec![])),
            Some((b"r\0c1".to_vec(), vec![])),
            Some((b"r\0c".to_vec(), vec![])),
        ];
        let ranges: Vec<KeyRange> = vec![
            (b"r\0a3".to_vec(), b"r\0a".to_vec()),
            (b"r\0b3".to_vec(), b"r\0b".to_vec()),
            (b"r\0c3".to_vec(), b"r\0c".to_vec()),
        ]
        .into_iter()
        .map(|(s, e)| {
            let mut range = KeyRange::default();
            range.set_start_key(s);
            range.set_end_key(e);
            range
        })
        .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_batch_scan(ctx, "".to_string(), ranges, 5, true, true)).unwrap(),
        );
    }

    #[test]
    fn test_raw_get_key_ttl() {
        test_kv_format_impl!(test_raw_get_key_ttl_impl<ApiV1Ttl ApiV2>());
    }

    fn test_raw_get_key_ttl_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec(), 10),
            (b"r\0b".to_vec(), b"bb".to_vec(), 20),
            (b"r\0c".to_vec(), b"cc".to_vec(), 0),
            (b"r\0d".to_vec(), b"dd".to_vec(), 10),
            (b"r\0e".to_vec(), b"ee".to_vec(), 20),
            (b"r\0f".to_vec(), b"ff".to_vec(), u64::MAX),
        ];

        let before_written = ttl_current_ts();
        // Write key-value pairs one by one
        for &(ref key, ref value, ttl) in &test_data {
            storage
                .raw_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    ttl,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        for &(ref key, _, ttl) in &test_data {
            let res = block_on(storage.raw_get_key_ttl(ctx.clone(), "".to_string(), key.clone()))
                .unwrap()
                .unwrap();
            if ttl != 0 {
                let lower_bound = before_written.saturating_add(ttl) - ttl_current_ts();
                assert!(
                    res >= lower_bound && res <= ttl,
                    "{} < {} < {}",
                    lower_bound,
                    res,
                    ttl
                );
            } else {
                assert_eq!(res, 0);
            }
        }
    }

    #[test]
    fn test_raw_compare_and_swap() {
        test_kv_format_impl!(test_raw_compare_and_swap_impl);
    }

    fn test_raw_compare_and_swap_impl<F: KvFormat>() {
        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let ctx = Context {
            api_version: F::CLIENT_TAG,
            ..Default::default()
        };

        let key = b"r\0key";

        // "v1" -> "v"
        let expected = (None, false);
        storage
            .raw_compare_and_swap_atomic(
                ctx.clone(),
                "".to_string(),
                key.to_vec(),
                Some(b"v1".to_vec()),
                b"v".to_vec(),
                0,
                expect_value_callback(tx.clone(), 0, expected),
            )
            .unwrap();
        rx.recv().unwrap();

        // "None" -> "v1"
        let expected = (None, true);
        storage
            .raw_compare_and_swap_atomic(
                ctx.clone(),
                "".to_string(),
                key.to_vec(),
                None,
                b"v1".to_vec(),
                0,
                expect_value_callback(tx.clone(), 0, expected),
            )
            .unwrap();
        rx.recv().unwrap();

        // "v1" -> "v2"
        let expected = (Some(b"v1".to_vec()), true);
        storage
            .raw_compare_and_swap_atomic(
                ctx.clone(),
                "".to_string(),
                key.to_vec(),
                Some(b"v1".to_vec()),
                b"v2".to_vec(),
                0,
                expect_value_callback(tx.clone(), 0, expected),
            )
            .unwrap();
        rx.recv().unwrap();

        // "v1" -> "v2"
        let expected = (Some(b"v2".to_vec()), false);
        storage
            .raw_compare_and_swap_atomic(
                ctx.clone(),
                "".to_string(),
                key.to_vec(),
                Some(b"v1".to_vec()),
                b"v2".to_vec(),
                0,
                expect_value_callback(tx.clone(), 0, expected),
            )
            .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // expect "v2"
        expect_value(
            b"v2".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), key.to_vec())).unwrap(),
        );
        expect_multi_values(
            vec![Some((key.to_vec(), b"v2".to_vec()))],
            block_on(storage.raw_scan(
                ctx.clone(),
                "".to_string(),
                b"r".to_vec(),
                Some(b"rz".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );

        // put "v3"
        storage
            .raw_batch_put_atomic(
                ctx.clone(),
                "".to_string(),
                vec![(key.to_vec(), b"v3".to_vec())],
                vec![0],
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // "v3" -> "v4"
        let expected = (Some(b"v3".to_vec()), true);
        storage
            .raw_compare_and_swap_atomic(
                ctx.clone(),
                "".to_string(),
                key.to_vec(),
                Some(b"v3".to_vec()),
                b"v4".to_vec(),
                0,
                expect_value_callback(tx.clone(), 0, expected),
            )
            .unwrap();
        rx.recv().unwrap();

        // delete
        storage
            .raw_batch_delete_atomic(
                ctx.clone(),
                "".to_string(),
                vec![key.to_vec()],
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // "None" -> "v"
        let expected = (None, true);
        storage
            .raw_compare_and_swap_atomic(
                ctx.clone(),
                "".to_string(),
                key.to_vec(),
                None,
                b"v".to_vec(),
                0,
                expect_value_callback(tx, 0, expected),
            )
            .unwrap();
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            storage
                .get_concurrency_manager()
                .global_min_lock_ts()
                .is_none()
        );

        // expect "v"
        expect_value(
            b"v".to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), key.to_vec())).unwrap(),
        );
        expect_multi_values(
            vec![Some((key.to_vec(), b"v".to_vec()))],
            block_on(storage.raw_scan(
                ctx,
                "".to_string(),
                b"r".to_vec(),
                Some(b"rz".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_scan_lock_with_memory_lock() {
        for in_memory_pessimistic_lock_enabled in [false, true] {
            let txn_ext = Arc::new(TxnExt::default());
            let lock_mgr = MockLockManager::new();
            let storage = TestStorageBuilderApiV1::new(lock_mgr.clone())
                .pipelined_pessimistic_lock(in_memory_pessimistic_lock_enabled)
                .in_memory_pessimistic_lock(in_memory_pessimistic_lock_enabled)
                .build_for_txn(txn_ext.clone())
                .unwrap();
            let (tx, rx) = channel();
            storage
                .sched_txn_command(
                    commands::AcquirePessimisticLock::new(
                        vec![(Key::from_raw(b"a"), false), (Key::from_raw(b"b"), false)],
                        b"a".to_vec(),
                        20.into(),
                        3000,
                        true,
                        20.into(),
                        Some(WaitTimeout::Millis(1000)),
                        false,
                        21.into(),
                        false,
                        false,
                        false,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            if in_memory_pessimistic_lock_enabled {
                // Check if the lock exists in the memory buffer.
                let pessimistic_locks = txn_ext.pessimistic_locks.read();
                let lock = pessimistic_locks.get(&Key::from_raw(b"a")).unwrap();
                assert_eq!(
                    lock,
                    &(
                        PessimisticLock {
                            primary: Box::new(*b"a"),
                            start_ts: 20.into(),
                            ttl: 3000,
                            for_update_ts: 20.into(),
                            min_commit_ts: 21.into(),
                            last_change: LastChange::NotExist,
                            is_locked_with_conflict: false,
                        },
                        false
                    )
                );
            }

            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        vec![
                            Mutation::make_put(Key::from_raw(b"x"), b"foo".to_vec()),
                            Mutation::make_put(Key::from_raw(b"y"), b"foo".to_vec()),
                            Mutation::make_put(Key::from_raw(b"z"), b"foo".to_vec()),
                        ],
                        b"x".to_vec(),
                        10.into(),
                    ),
                    expect_ok_callback(tx, 0),
                )
                .unwrap();
            rx.recv().unwrap();

            let (lock_a, lock_b, lock_x, lock_y, lock_z) = (
                {
                    let mut lock = LockInfo::default();
                    lock.set_primary_lock(b"a".to_vec());
                    lock.set_lock_version(20);
                    lock.set_lock_for_update_ts(20);
                    lock.set_key(b"a".to_vec());
                    lock.set_min_commit_ts(21);
                    lock.set_lock_type(Op::PessimisticLock);
                    lock.set_lock_ttl(3000);
                    lock
                },
                {
                    let mut lock = LockInfo::default();
                    lock.set_primary_lock(b"a".to_vec());
                    lock.set_lock_version(20);
                    lock.set_lock_for_update_ts(20);
                    lock.set_key(b"b".to_vec());
                    lock.set_min_commit_ts(21);
                    lock.set_lock_type(Op::PessimisticLock);
                    lock.set_lock_ttl(3000);
                    lock
                },
                {
                    let mut lock = LockInfo::default();
                    lock.set_primary_lock(b"x".to_vec());
                    lock.set_lock_version(10);
                    lock.set_key(b"x".to_vec());
                    lock
                },
                {
                    let mut lock = LockInfo::default();
                    lock.set_primary_lock(b"x".to_vec());
                    lock.set_lock_version(10);
                    lock.set_key(b"y".to_vec());
                    lock
                },
                {
                    let mut lock = LockInfo::default();
                    lock.set_primary_lock(b"x".to_vec());
                    lock.set_lock_version(10);
                    lock.set_key(b"z".to_vec());
                    lock
                },
            );
            let res = block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 10))
                .unwrap();
            assert_eq!(res, vec![lock_a, lock_b, lock_x, lock_y, lock_z]);
        }
    }

    #[test]
    fn test_scan_lock() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"x"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"y"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"z"), b"foo".to_vec()),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    101.into(),
                    123,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (lock_a, lock_b, lock_c, lock_x, lock_y, lock_z) = (
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"a".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"b".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"c".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"x".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"y".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"z".to_vec());
                lock
            },
        );

        let cm = storage.concurrency_manager.clone();

        let res =
            block_on(storage.scan_lock(Context::default(), 99.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![]);
        assert_eq!(cm.max_ts(), 99.into());

        let res =
            block_on(storage.scan_lock(Context::default(), 100.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![lock_x.clone(), lock_y.clone(), lock_z.clone()]);
        assert_eq!(cm.max_ts(), 100.into());

        let res = block_on(storage.scan_lock(
            Context::default(),
            100.into(),
            Some(Key::from_raw(b"a")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_x.clone(), lock_y.clone(), lock_z.clone()]);

        let res = block_on(storage.scan_lock(
            Context::default(),
            100.into(),
            Some(Key::from_raw(b"y")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_y.clone(), lock_z.clone()]);

        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 10)).unwrap();
        assert_eq!(
            res,
            vec![
                lock_a.clone(),
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
                lock_z.clone(),
            ]
        );
        assert_eq!(cm.max_ts(), 101.into());

        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 4)).unwrap();
        assert_eq!(
            res,
            vec![lock_a, lock_b.clone(), lock_c.clone(), lock_x.clone()]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            None,
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            None,
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
                lock_z,
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"c")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b.clone()]);

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            3,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b.clone(), lock_c.clone(), lock_x.clone()]);

        let mem_lock = |k: &[u8], ts: u64, lock_type| {
            let key = Key::from_raw(k);
            let guard = block_on(cm.lock_key(&key));
            guard.with_lock(|lock| {
                *lock = Some(txn_types::Lock::new(
                    lock_type,
                    k.to_vec(),
                    ts.into(),
                    100,
                    None,
                    0.into(),
                    1,
                    20.into(),
                    false,
                ));
            });
            guard
        };

        let guard = mem_lock(b"z", 80, LockType::Put);
        block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 1)).unwrap_err();

        let guard2 = mem_lock(b"a", 80, LockType::Put);
        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
            ]
        );
        drop(guard);
        drop(guard2);

        // LockType::Lock can't be ignored by scan_lock
        let guard = mem_lock(b"c", 80, LockType::Lock);
        block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            1,
        ))
        .unwrap_err();
        drop(guard);

        let guard = mem_lock(b"c", 102, LockType::Put);
        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b, lock_c, lock_x, lock_y]);
        drop(guard);
    }

    #[test]
    fn test_resolve_lock() {
        test_resolve_lock_impl::<ApiV1>();
        test_resolve_lock_impl::<ApiV2>();
    }

    fn test_resolve_lock_impl<F: KvFormat>() {
        use crate::storage::txn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        // These locks (transaction ts=99) are not going to be resolved.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"ta"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"tb"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"tc"), b"foo".to_vec()),
                    ],
                    b"tc".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (lock_a, lock_b, lock_c) = (
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"tc".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"ta".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"tc".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"tb".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"tc".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"tc".to_vec());
                lock
            },
        );

        // We should be able to resolve all locks for transaction ts=100 when there are
        // this many locks.
        let scanned_locks_coll = vec![
            1,
            RESOLVE_LOCK_BATCH_SIZE,
            RESOLVE_LOCK_BATCH_SIZE - 1,
            RESOLVE_LOCK_BATCH_SIZE + 1,
            RESOLVE_LOCK_BATCH_SIZE * 2,
            RESOLVE_LOCK_BATCH_SIZE * 2 - 1,
            RESOLVE_LOCK_BATCH_SIZE * 2 + 1,
        ];

        let is_rollback_coll = vec![
            false, // commit
            true,  // rollback
        ];
        let mut ts = 100.into();

        for scanned_locks in scanned_locks_coll {
            for is_rollback in &is_rollback_coll {
                let mut mutations = vec![];
                for i in 0..scanned_locks {
                    mutations.push(Mutation::make_put(
                        Key::from_raw(format!("tx{:08}", i).as_bytes()),
                        b"foo".to_vec(),
                    ));
                }

                storage
                    .sched_txn_command(
                        commands::Prewrite::with_defaults(mutations, b"tx".to_vec(), ts),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let mut txn_status = HashMap::default();
                txn_status.insert(
                    ts,
                    if *is_rollback {
                        TimeStamp::zero() // rollback
                    } else {
                        (ts.into_inner() + 5).into() // commit, commit_ts = start_ts + 5
                    },
                );
                storage
                    .sched_txn_command(
                        commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All locks should be resolved except for a, b and c.
                let res =
                    block_on(storage.scan_lock(Context::default(), ts, None, None, 0)).unwrap();
                assert_eq!(res, vec![lock_a.clone(), lock_b.clone(), lock_c.clone()]);

                ts = (ts.into_inner() + 10).into();
            }
        }
    }

    #[test]
    fn test_resolve_lock_lite() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Rollback key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    resolve_keys,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check lock for key 'a'.
        let lock_a = {
            let mut lock = LockInfo::default();
            lock.set_primary_lock(b"c".to_vec());
            lock.set_lock_version(99);
            lock.set_key(b"a".to_vec());
            lock
        };
        let res =
            block_on(storage.scan_lock(Context::default(), 99.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![lock_a]);

        // Resolve lock for key 'a'.
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    vec![Key::from_raw(b"a")],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    101.into(),
                    102.into(),
                    resolve_keys,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check lock for key 'a'.
        let lock_a = {
            let mut lock = LockInfo::default();
            lock.set_primary_lock(b"c".to_vec());
            lock.set_lock_version(101);
            lock.set_key(b"a".to_vec());
            lock
        };
        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![lock_a]);
    }

    #[test]
    fn test_txn_heart_beat() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let k = Key::from_raw(b"k");
        let v = b"v".to_vec();

        let uncommitted = TxnStatus::uncommitted;

        // No lock.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 100, 0, Context::default()),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    b"k".to_vec(),
                    10.into(),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let lock_with_ttl = |ttl| {
            txn_types::Lock::new(
                LockType::Put,
                b"k".to_vec(),
                10.into(),
                ttl,
                Some(v.clone()),
                0.into(),
                0,
                0.into(),
                false,
            )
        };

        // `advise_ttl` = 90, which is less than current ttl 100. The lock's ttl will
        // remains 100.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 90, 0, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(lock_with_ttl(100), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 110, which is greater than current ttl. The lock's ttl will be
        // updated to 110.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 110, 0, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(lock_with_ttl(110), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // Lock not match. Nothing happens except throwing an error.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k, 11.into(), 150, 0, Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_txn_status() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k = Key::from_raw(b"k");
        let v = b"b".to_vec();

        let ts = TimeStamp::compose;
        use TxnStatus::*;
        let uncommitted = TxnStatus::uncommitted;
        let committed = TxnStatus::committed;

        // No lock and no commit info. Gets an error.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), ts(9, 1));

        // No lock and no commit info. If specified rollback_if_not_exist, the key will
        // be rolled back.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    true,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, LockNotExist),
            )
            .unwrap();
        rx.recv().unwrap();

        // A rollback will be written, so an later-arriving prewrite will fail.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    k.as_encoded().to_vec(),
                    ts(9, 0),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    b"k".to_vec(),
                    ts(10, 0),
                    100,
                    false,
                    3,
                    ts(10, 1),
                    TimeStamp::default(),
                    Some(vec![b"k1".to_vec(), b"k2".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If lock exists and not expired, returns the lock's information.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    0.into(),
                    0.into(),
                    true,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    uncommitted(
                        txn_types::Lock::new(
                            LockType::Put,
                            b"k".to_vec(),
                            ts(10, 0),
                            100,
                            Some(v.clone()),
                            0.into(),
                            3,
                            ts(10, 1),
                            false,
                        )
                        .use_async_commit(vec![b"k1".to_vec(), b"k2".to_vec()]),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // TODO: Check the lock's min_commit_ts field.

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![k.clone()],
                    ts(10, 0),
                    ts(20, 0),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the transaction is committed, returns the commit_ts.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    ts(12, 0),
                    ts(15, 0),
                    true,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, committed(ts(20, 0))),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(k.clone(), v)],
                    k.to_raw().unwrap(),
                    ts(25, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the lock has expired, cleanup it.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(25, 0),
                    ts(126, 0),
                    ts(127, 0),
                    true,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Commit::new(vec![k], ts(25, 0), ts(28, 0), None, Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_secondary_locks() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k2");

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_lock(k1.clone()),
                        Mutation::make_lock(k2.clone()),
                    ],
                    b"k".to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // All locks exist

        let mut lock1 = LockInfo::default();
        lock1.set_primary_lock(b"k".to_vec());
        lock1.set_lock_version(10);
        lock1.set_key(b"k1".to_vec());
        lock1.set_txn_size(2);
        lock1.set_lock_ttl(100);
        lock1.set_lock_type(Op::Lock);
        let mut lock2 = lock1.clone();
        lock2.set_key(b"k2".to_vec());

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![k1.clone(), k2.clone()],
                    10.into(),
                    Context::default(),
                ),
                expect_secondary_locks_status_callback(
                    tx.clone(),
                    SecondaryLocksStatus::Locked(vec![lock1, lock2]),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // One of the locks are committed

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![k1.clone()],
                    10.into(),
                    20.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![k1, k2], 10.into(), Context::default()),
                expect_secondary_locks_status_callback(
                    tx.clone(),
                    SecondaryLocksStatus::Committed(20.into()),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), 10.into());

        // Some of the locks do not exist
        let k3 = Key::from_raw(b"k3");
        let k4 = Key::from_raw(b"k4");

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_lock(k3.clone())],
                    b"k".to_vec(),
                    30.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![k3, k4], 10.into(), Context::default()),
                expect_secondary_locks_status_callback(tx, SecondaryLocksStatus::RolledBack),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn test_pessimistic_lock_impl(pipelined_pessimistic_lock: bool) {
        let lock_mgr = MockLockManager::new();
        let storage = TestStorageBuilderApiV1::new(lock_mgr.clone())
            .pipelined_pessimistic_lock(pipelined_pessimistic_lock)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();
        let (key, val) = (Key::from_raw(b"key"), b"val".to_vec());
        let (key2, val2) = (Key::from_raw(b"key2"), b"val2".to_vec());

        let results_values = |res: Vec<Option<Value>>| {
            PessimisticLockResults(
                res.into_iter()
                    .map(|v| PessimisticLockKeyResult::Value(v))
                    .collect::<Vec<_>>(),
            )
        };
        let results_existence = |res: Vec<bool>| {
            PessimisticLockResults(
                res.into_iter()
                    .map(|v| PessimisticLockKeyResult::Existence(v))
                    .collect::<Vec<_>>(),
            )
        };
        let results_empty =
            |len| PessimisticLockResults(vec![PessimisticLockKeyResult::Empty; len]);

        // Key not exist
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            let pessimistic_lock_res = if return_values {
                results_values(vec![None])
            } else if check_existence {
                results_existence(vec![false])
            } else {
                results_empty(1)
            };

            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            if return_values || check_existence {
                assert_eq!(cm.max_ts(), 10.into());
            }

            // Duplicated command
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            delete_pessimistic_lock(&storage, key.clone(), 10, 10);
        }

        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(key.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_pessimistic_lock_res_callback(tx.clone(), results_empty(1)),
            )
            .unwrap();
        rx.recv().unwrap();

        // KeyIsLocked
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        20,
                        20,
                        return_values,
                        check_existence,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::KeyIsLocked(_)),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            // The request enters lock waiting state.
            rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
            lock_mgr.simulate_timeout_all();
            // The lock-waiting request is cancelled.
            rx.recv().unwrap();
        }

        // Always update max_ts when trying to read.
        assert_eq!(cm.max_ts(), 20.into());

        // Put key and key2.
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (
                            Mutation::make_put(key.clone(), val.clone()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(key2.clone(), val2.clone()),
                            SkipPessimisticCheck,
                        ),
                    ],
                    key.to_raw().unwrap(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![key.clone(), key2.clone()],
                    10.into(),
                    20.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // WriteConflict
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        15,
                        15,
                        return_values,
                        check_existence,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::WriteConflict { .. }),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        assert_eq!(cm.max_ts(), 20.into());

        // Return multiple values
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            let pessimistic_lock_res = if return_values {
                results_values(vec![Some(val.clone()), Some(val2.clone()), None])
            } else if check_existence {
                results_existence(vec![true, true, false])
            } else {
                results_empty(3)
            };
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![
                            (key.clone(), false),
                            (key2.clone(), false),
                            (Key::from_raw(b"key3"), false),
                        ],
                        30,
                        30,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res),
                )
                .unwrap();
            rx.recv().unwrap();

            if return_values || check_existence {
                assert_eq!(cm.max_ts(), 30.into());
            }

            delete_pessimistic_lock(&storage, key.clone(), 30, 30);
        }
    }

    #[test]
    fn test_pessimistic_lock() {
        test_pessimistic_lock_impl(false);
        test_pessimistic_lock_impl(true);
    }

    fn test_pessimistic_lock_resumable_impl(
        pipelined_pessimistic_lock: bool,
        in_memory_lock: bool,
    ) {
        type Res = PessimisticLockKeyResult;
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .pipelined_pessimistic_lock(pipelined_pessimistic_lock)
            .in_memory_pessimistic_lock(in_memory_lock)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let results_empty =
            |len| PessimisticLockResults(vec![PessimisticLockKeyResult::Empty; len]);

        for case_num in 0..4 {
            let key = |i| vec![b'k', case_num, i];
            // Put key "k1".
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(1)), b"v1".to_vec())],
                        key(1),
                        10.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(1))],
                        10.into(),
                        20.into(),
                        None,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Put key "k2".
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(2)), b"v2".to_vec())],
                        key(2),
                        30.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(2))],
                        30.into(),
                        40.into(),
                        None,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Lock "k3", and we will pessimistic-rollback it.
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(3)), false)],
                        20,
                        20,
                        false,
                        false,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), results_empty(1)),
                )
                .unwrap();
            rx.recv().unwrap();

            // Prewrite "k4", and we will commit it
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(4)), b"v4".to_vec())],
                        key(4),
                        30.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Prewrite "k5", and we will roll it back
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(5)), b"v5".to_vec())],
                        key(5),
                        30.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            // Prewrite "k6", and it won't cause conflict after committing.
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(Key::from_raw(&key(6)), b"v6".to_vec())],
                        key(6),
                        10.into(),
                        3000,
                        false,
                        1,
                        TimeStamp::zero(),
                        TimeStamp::default(),
                        None,
                        false,
                        AssertionLevel::Off,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        for &(case_num, return_values, check_existence) in &[
            (0, false, false),
            (1, false, true),
            (2, true, false),
            (3, true, true),
        ] {
            let key = |i| vec![b'k', case_num, i];
            let expected_results = if return_values {
                vec![
                    Res::Value(Some(b"v1".to_vec())),
                    Res::LockedWithConflict {
                        value: Some(b"v2".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::Value(None),
                    Res::LockedWithConflict {
                        value: Some(b"v4".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::LockedWithConflict {
                        value: None,
                        conflict_ts: 30.into(),
                    },
                    Res::Value(Some(b"v6".to_vec())),
                ]
            } else if check_existence {
                vec![
                    Res::Existence(true),
                    Res::LockedWithConflict {
                        value: Some(b"v2".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::Existence(false),
                    Res::LockedWithConflict {
                        value: Some(b"v4".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::LockedWithConflict {
                        value: None,
                        conflict_ts: 30.into(),
                    },
                    Res::Existence(true),
                ]
            } else {
                vec![
                    Res::Empty,
                    Res::LockedWithConflict {
                        value: Some(b"v2".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::Empty,
                    Res::LockedWithConflict {
                        value: Some(b"v4".to_vec()),
                        conflict_ts: 40.into(),
                    },
                    Res::LockedWithConflict {
                        value: None,
                        conflict_ts: 30.into(),
                    },
                    Res::Empty,
                ]
            };

            // k1 & k2
            for (i, k) in &[(0, key(1)), (1, key(2))] {
                let i = *i;
                storage
                    .sched_txn_command(
                        new_acquire_pessimistic_lock_command(
                            vec![(Key::from_raw(k), false)],
                            25,
                            25,
                            return_values,
                            check_existence,
                        )
                        .allow_lock_with_conflict(true),
                        expect_pessimistic_lock_res_callback(
                            tx.clone(),
                            PessimisticLockResults(vec![expected_results[i].clone()]),
                        ),
                    )
                    .unwrap();
                rx.recv().unwrap();
            }

            // k3
            // Report KeyIsLocked if no wait
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(3)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(None),
                    expect_value_with_checker_callback(
                        tx.clone(),
                        0,
                        |res: Result<PessimisticLockResults>| {
                            let e = res.unwrap().0[0].unwrap_err();
                            match e.inner() {
                                ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                                    mvcc::Error(box mvcc::ErrorInner::KeyIsLocked(..)),
                                ))) => (),
                                e => panic!("unexpected error chain: {:?}", e),
                            }
                        },
                    ),
                )
                .unwrap();
            rx.recv().unwrap();

            // Lock wait
            let (tx1, rx1) = channel();
            // k3
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(3)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[2].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();

            delete_pessimistic_lock(&storage, Key::from_raw(&key(3)), 20, 20);
            rx1.recv().unwrap();

            // k4
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(4)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[3].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(4))],
                        30.into(),
                        40.into(),
                        None,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            rx1.recv().unwrap();

            // k5
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(5)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[4].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
            storage
                .sched_txn_command(
                    commands::Rollback::new(
                        vec![Key::from_raw(&key(5))],
                        30.into(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            rx1.recv().unwrap();

            // k6
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(&key(6)), false)],
                        25,
                        25,
                        return_values,
                        check_existence,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(
                        tx1.clone(),
                        PessimisticLockResults(vec![expected_results[5].clone()]),
                    ),
                )
                .unwrap();
            rx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
            storage
                .sched_txn_command(
                    commands::Commit::new(
                        vec![Key::from_raw(&key(6))],
                        10.into(),
                        20.into(),
                        None,
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
            rx1.recv().unwrap();

            must_have_locks(
                &storage,
                50,
                &key(0),
                &key(10),
                &[
                    (&key(1), Op::PessimisticLock, 25, 25),
                    (&key(2), Op::PessimisticLock, 25, 40),
                    (&key(3), Op::PessimisticLock, 25, 25),
                    (&key(4), Op::PessimisticLock, 25, 40),
                    (&key(5), Op::PessimisticLock, 25, 30),
                    (&key(6), Op::PessimisticLock, 25, 25),
                ],
            );

            // Test idempotency
            for i in 0..6usize {
                storage
                    .sched_txn_command(
                        new_acquire_pessimistic_lock_command(
                            vec![(Key::from_raw(&key(i as u8 + 1)), false)],
                            25,
                            25,
                            return_values,
                            check_existence,
                        )
                        .allow_lock_with_conflict(true)
                        .lock_wait_timeout(Some(WaitTimeout::Default)),
                        expect_pessimistic_lock_res_callback(
                            tx1.clone(),
                            PessimisticLockResults(vec![expected_results[i].clone()]),
                        ),
                    )
                    .unwrap();
                rx1.recv().unwrap();
            }
        }

        // Check the channel is clear to avoid misusing in the above test code.
        tx.send(100).unwrap();
        assert_eq!(rx.recv().unwrap(), 100);

        // Test request queueing.
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"k21"), false)],
                    10,
                    10,
                    false,
                    false,
                )
                .allow_lock_with_conflict(true)
                .lock_wait_timeout(Some(WaitTimeout::Default)),
                expect_pessimistic_lock_res_callback(tx, results_empty(1)),
            )
            .unwrap();
        rx.recv().unwrap();

        let channels: Vec<_> = (0..4).map(|_| channel()).collect();
        let start_ts = &[20, 50, 30, 40];
        for i in 0..4 {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(Key::from_raw(b"k21"), false)],
                        start_ts[i],
                        start_ts[i],
                        false,
                        false,
                    )
                    .allow_lock_with_conflict(true)
                    .lock_wait_timeout(Some(WaitTimeout::Default)),
                    expect_pessimistic_lock_res_callback(channels[i].0.clone(), results_empty(1)),
                )
                .unwrap();
            channels[i]
                .1
                .recv_timeout(Duration::from_millis(100))
                .unwrap_err();
        }

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 10, 10);
        channels[0].1.recv().unwrap();
        channels[2]
            .1
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 20, 20);
        channels[2].1.recv().unwrap();
        channels[3]
            .1
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 30, 30);
        channels[3].1.recv().unwrap();
        channels[1]
            .1
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        delete_pessimistic_lock(&storage, Key::from_raw(b"k21"), 40, 40);
        channels[1].1.recv().unwrap();
    }

    #[test]
    fn test_pessimistic_lock_resumable() {
        for &pipelined_pessimistic_lock in &[false, true] {
            for &in_memory_lock in &[false, true] {
                test_pessimistic_lock_resumable_impl(pipelined_pessimistic_lock, in_memory_lock);
            }
        }
    }

    #[allow(clippy::large_enum_variant)]
    pub enum Msg {
        WaitFor {
            #[allow(dead_code)]
            token: LockWaitToken,
            #[allow(dead_code)]
            region_id: u64,
            #[allow(dead_code)]
            region_epoch: RegionEpoch,
            #[allow(dead_code)]
            term: u64,
            start_ts: TimeStamp,
            wait_info: KeyLockWaitInfo,
            is_first_lock: bool,
            timeout: Option<WaitTimeout>,
            #[allow(dead_code)]
            cancel_callback: CancellationCallback,
            #[allow(dead_code)]
            diag_ctx: DiagnosticContext,
        },
        RemoveLockWait {
            #[allow(dead_code)]
            token: LockWaitToken,
        },
    }

    // `ProxyLockMgr` sends all msgs it received to `Sender`.
    // It's used to check whether we send right messages to lock manager.
    #[derive(Clone)]
    pub struct ProxyLockMgr {
        tx: Arc<Mutex<Sender<Msg>>>,
        has_waiter: Arc<AtomicBool>,
    }

    impl ProxyLockMgr {
        pub fn new(tx: Sender<Msg>) -> Self {
            Self {
                tx: Arc::new(Mutex::new(tx)),
                has_waiter: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl LockManager for ProxyLockMgr {
        fn allocate_token(&self) -> LockWaitToken {
            LockWaitToken(Some(1))
        }

        fn wait_for(
            &self,
            token: LockWaitToken,
            region_id: u64,
            region_epoch: RegionEpoch,
            term: u64,
            start_ts: TimeStamp,
            wait_info: KeyLockWaitInfo,
            is_first_lock: bool,
            timeout: Option<WaitTimeout>,
            cancel_callback: CancellationCallback,
            diag_ctx: DiagnosticContext,
        ) {
            self.tx
                .lock()
                .send(Msg::WaitFor {
                    token,
                    region_id,
                    region_epoch,
                    term,
                    start_ts,
                    wait_info,
                    is_first_lock,
                    timeout,
                    cancel_callback,
                    diag_ctx,
                })
                .unwrap();
        }

        fn update_wait_for(&self, _updated_items: Vec<UpdateWaitForEvent>) {}

        fn remove_lock_wait(&self, token: LockWaitToken) {
            self.tx.lock().send(Msg::RemoveLockWait { token }).unwrap();
        }

        fn has_waiter(&self) -> bool {
            self.has_waiter.load(Ordering::Relaxed)
        }

        fn dump_wait_for_entries(&self, _cb: waiter_manager::Callback) {
            unimplemented!()
        }
    }

    // Test whether `Storage` sends right wait-for-lock msgs to `LockManager`.
    #[test]
    fn validate_wait_for_lock_msg() {
        let (msg_tx, msg_rx) = channel();
        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            TestEngineBuilder::new().build().unwrap(),
            ProxyLockMgr::new(msg_tx),
        )
        .build()
        .unwrap();

        let (k, v) = (b"k".to_vec(), b"v".to_vec());
        let (tx, rx) = channel();
        // Write lock-k.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_raw(&k), v)],
                    k.clone(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // No wait for msg
        assert!(msg_rx.try_recv().is_err());

        // Meet lock-k.
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(Key::from_raw(b"foo"), false), (Key::from_raw(&k), false)],
                    k.clone(),
                    20.into(),
                    3000,
                    true,
                    20.into(),
                    Some(WaitTimeout::Millis(100)),
                    false,
                    21.into(),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        // The transaction should be waiting for lock released so cb won't be called.
        rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

        let msg = msg_rx.try_recv().unwrap();
        // Check msg validation.
        match msg {
            Msg::WaitFor {
                start_ts,
                wait_info,
                is_first_lock,
                timeout,
                ..
            } => {
                assert_eq!(start_ts, TimeStamp::new(20));
                assert_eq!(
                    wait_info.lock_digest,
                    LockDigest {
                        ts: 10.into(),
                        hash: Key::from_raw(&k).gen_hash(),
                    }
                );
                assert_eq!(is_first_lock, true);
                assert_eq!(timeout, Some(WaitTimeout::Millis(100)));
            }

            _ => panic!("unexpected msg"),
        }
    }

    // Test whether `Storage` correctly wakes up lock-waiting requests
    #[test]
    fn test_wake_up() {
        struct BlockedLockRequestHandle {
            remaining: usize,
            rx: std::sync::mpsc::Receiver<i32>,
        }

        impl BlockedLockRequestHandle {
            fn assert_blocked(&mut self) {
                while self.remaining > 0 {
                    match self.rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(_) => self.remaining -= 1,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return,
                        Err(e) => panic!("unexpected error: {:?}", e),
                    }
                }
                panic!("pessimistic lock requests expected to be blocked finished unexpectedly")
            }

            fn assert_woken_up(mut self) {
                while self.remaining > 0 {
                    match self.rx.recv_timeout(Duration::from_millis(200)) {
                        Ok(_) => self.remaining -= 1,
                        Err(e) => panic!("unexpected error: {:?}", e),
                    }
                }
            }
        }

        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            TestEngineBuilder::new().build().unwrap(),
            MockLockManager::new(),
        )
        .build()
        .unwrap();

        let lock_blocked = |keys: &[Key],
                            lock_ts: u64,
                            expected_conflicting_start_ts: u64,
                            expected_conflicting_commit_ts: u64| {
            let (tx, rx) = channel();
            for k in keys {
                storage
                    .sched_txn_command(
                        commands::AcquirePessimisticLock::new(
                            vec![(k.clone(), false)],
                            k.to_raw().unwrap(),
                            lock_ts.into(),
                            3000,
                            false,
                            lock_ts.into(),
                            Some(WaitTimeout::Millis(5000)),
                            false,
                            (lock_ts + 1).into(),
                            false,
                            false,
                            false,
                            Context::default(),
                        ),
                        expect_fail_callback(tx.clone(), 6, move |e| match e {
                            Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                                mvcc::Error(box mvcc::ErrorInner::WriteConflict {
                                    conflict_start_ts,
                                    conflict_commit_ts,
                                    ..
                                }),
                            )))) => {
                                assert_eq!(conflict_start_ts, expected_conflicting_start_ts.into());
                                assert_eq!(
                                    conflict_commit_ts,
                                    expected_conflicting_commit_ts.into()
                                );
                            }
                            e => panic!("unexpected error chain: {:?}", e),
                        }),
                    )
                    .unwrap();
            }
            let mut h = BlockedLockRequestHandle {
                remaining: keys.len(),
                rx,
            };
            h.assert_blocked();
            h
        };

        let (tx, rx) = channel();
        let prewrite_locks = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        keys.iter()
                            .map(|k| Mutation::make_put(k.clone(), b"v".to_vec()))
                            .collect(),
                        keys[0].to_raw().unwrap(),
                        ts,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };
        let acquire_pessimistic_locks = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        keys.iter().map(|k| (k.clone(), false)).collect(),
                        ts,
                        ts,
                        false,
                        false,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };

        let keys = vec![
            Key::from_raw(b"a"),
            Key::from_raw(b"b"),
            Key::from_raw(b"c"),
        ];

        // Commit
        prewrite_locks(&keys, 10.into());
        let h = lock_blocked(&keys, 15, 10, 20);
        storage
            .sched_txn_command(
                commands::Commit::new(keys.clone(), 10.into(), 20.into(), None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        h.assert_woken_up();

        // Cleanup
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(30);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_locks(&keys[..1], ts);
            } else {
                prewrite_locks(&keys[..1], ts);
            }
            let h = lock_blocked(&keys[..1], 35, ts.into_inner(), 0);
            storage
                .sched_txn_command(
                    commands::Cleanup::new(
                        keys[0].clone(),
                        ts,
                        TimeStamp::max(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            h.assert_woken_up();
        }

        // Rollback
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(40);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_locks(&keys, ts);
            } else {
                prewrite_locks(&keys, ts);
            }
            let h = lock_blocked(&keys, 45, ts.into_inner(), 0);
            storage
                .sched_txn_command(
                    commands::Rollback::new(keys.clone(), ts, Context::default()),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            h.assert_woken_up();
        }

        // PessimisticRollback
        acquire_pessimistic_locks(&keys, 50.into());
        let h = lock_blocked(&keys, 55, 50, 0);
        storage
            .sched_txn_command(
                commands::PessimisticRollback::new(
                    keys.clone(),
                    50.into(),
                    50.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        h.assert_woken_up();

        // ResolveLockLite
        for commit in &[false, true] {
            let mut start_ts = TimeStamp::new(60);
            let commit_ts = if *commit {
                start_ts.incr();
                start_ts.next()
            } else {
                TimeStamp::zero()
            };
            prewrite_locks(&keys, start_ts);
            let h = lock_blocked(&keys, 65, start_ts.into_inner(), commit_ts.into_inner());
            storage
                .sched_txn_command(
                    commands::ResolveLockLite::new(
                        start_ts,
                        commit_ts,
                        keys.clone(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            h.assert_woken_up();
        }

        // ResolveLock
        let mut txn_status = HashMap::default();
        acquire_pessimistic_locks(&keys, 70.into());
        // Rollback start_ts=70
        txn_status.insert(TimeStamp::new(70), TimeStamp::zero());
        let committed_keys = vec![
            Key::from_raw(b"d"),
            Key::from_raw(b"e"),
            Key::from_raw(b"f"),
        ];
        prewrite_locks(&committed_keys, 75.into());
        txn_status.insert(TimeStamp::new(75), TimeStamp::new(76));
        let h_rolled_back = lock_blocked(&keys, 76, 70, 0);
        let h_committed = lock_blocked(&committed_keys, 76, 75, 76);
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        h_rolled_back.assert_woken_up();
        h_committed.assert_woken_up();

        // CheckTxnStatus
        let key = Key::from_raw(b"k");
        let start_ts = TimeStamp::compose(100, 0);
        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(key.clone(), b"v".to_vec())],
                    key.to_raw().unwrap(),
                    start_ts,
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut h = lock_blocked(&[key.clone()], 105, start_ts.into_inner(), 0);

        // Not expire
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key.clone(),
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(150, 0),
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    TxnStatus::uncommitted(
                        txn_types::Lock::new(
                            LockType::Put,
                            b"k".to_vec(),
                            start_ts,
                            100,
                            Some(b"v".to_vec()),
                            0.into(),
                            0,
                            0.into(),
                            false,
                        ),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        // Not woken up
        h.assert_blocked();

        // Expired
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key,
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(201, 0),
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TxnStatus::TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();
        h.assert_woken_up();
    }

    #[test]
    fn test_check_memory_locks() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.get_concurrency_manager();
        let key = Key::from_raw(b"key");
        let guard = block_on(cm.lock_key(&key));
        guard.with_lock(|lock| {
            *lock = Some(txn_types::Lock::new(
                LockType::Put,
                b"key".to_vec(),
                10.into(),
                100,
                Some(vec![]),
                0.into(),
                1,
                20.into(),
                false,
            ));
        });

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);

        // Test get
        let key_error = extract_key_error(
            &block_on(storage.get(ctx.clone(), Key::from_raw(b"key"), 100.into())).unwrap_err(),
        );
        assert_eq!(key_error.get_locked().get_key(), b"key");
        // Ignore memory locks in resolved or committed locks.
        ctx.set_resolved_locks(vec![10]);
        block_on(storage.get(ctx.clone(), Key::from_raw(b"key"), 100.into())).unwrap();
        ctx.take_resolved_locks();

        // Test batch_get
        let batch_get = |ctx| {
            block_on(storage.batch_get(
                ctx,
                vec![Key::from_raw(b"a"), Key::from_raw(b"key")],
                100.into(),
            ))
        };
        let key_error = extract_key_error(&batch_get(ctx.clone()).unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        // Ignore memory locks in resolved locks.
        ctx.set_resolved_locks(vec![10]);
        batch_get(ctx.clone()).unwrap();
        ctx.take_resolved_locks();

        // Test scan
        let scan = |ctx, start_key, end_key, reverse| {
            block_on(storage.scan(ctx, start_key, end_key, 10, 0, 100.into(), false, reverse))
        };
        let key_error =
            extract_key_error(&scan(ctx.clone(), Key::from_raw(b"a"), None, false).unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        ctx.set_resolved_locks(vec![10]);
        scan(ctx.clone(), Key::from_raw(b"a"), None, false).unwrap();
        ctx.take_resolved_locks();
        let key_error =
            extract_key_error(&scan(ctx.clone(), Key::from_raw(b"\xff"), None, true).unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        ctx.set_resolved_locks(vec![10]);
        scan(ctx.clone(), Key::from_raw(b"\xff"), None, false).unwrap();
        ctx.take_resolved_locks();
        // Ignore memory locks in resolved or committed locks.

        // Test batch_get_command
        let mut req1 = GetRequest::default();
        req1.set_context(ctx.clone());
        req1.set_key(b"a".to_vec());
        req1.set_version(50);
        let mut req2 = GetRequest::default();
        req2.set_context(ctx);
        req2.set_key(b"key".to_vec());
        req2.set_version(100);
        let batch_get_command = |req2| {
            let consumer = GetConsumer::new();
            block_on(storage.batch_get_command(
                vec![req1.clone(), req2],
                vec![1, 2],
                vec![INVALID_TRACKER_TOKEN; 2],
                consumer.clone(),
                Instant::now(),
            ))
            .unwrap();
            consumer.take_data()
        };
        let res = batch_get_command(req2.clone());
        res[0].as_ref().unwrap();
        let key_error = extract_key_error(res[1].as_ref().unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
        // Ignore memory locks in resolved or committed locks.
        req2.mut_context().set_resolved_locks(vec![10]);
        let res = batch_get_command(req2.clone());
        res[0].as_ref().unwrap();
        res[1].as_ref().unwrap();
        req2.mut_context().take_resolved_locks();
    }

    #[test]
    fn test_read_access_locks() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();

        let (k1, v1) = (b"k1".to_vec(), b"v1".to_vec());
        let (k2, v2) = (b"k2".to_vec(), b"v2".to_vec());
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_raw(&k1), v1.clone()),
                        Mutation::make_put(Key::from_raw(&k2), v2.clone()),
                    ],
                    k1.clone(),
                    100.into(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);
        ctx.set_committed_locks(vec![100]);
        // get
        assert_eq!(
            block_on(storage.get(ctx.clone(), Key::from_raw(&k1), 110.into()))
                .unwrap()
                .0,
            Some(v1.clone())
        );
        // batch get
        let res = block_on(storage.batch_get(
            ctx.clone(),
            vec![Key::from_raw(&k1), Key::from_raw(&k2)],
            110.into(),
        ))
        .unwrap()
        .0;
        if res[0].as_ref().unwrap().0 == k1 {
            assert_eq!(&res[0].as_ref().unwrap().1, &v1);
            assert_eq!(&res[1].as_ref().unwrap().1, &v2);
        } else {
            assert_eq!(&res[0].as_ref().unwrap().1, &v2);
            assert_eq!(&res[1].as_ref().unwrap().1, &v1);
        }
        // batch get commands
        let mut req = GetRequest::default();
        req.set_context(ctx.clone());
        req.set_key(k1.clone());
        req.set_version(110);
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![req],
            vec![1],
            vec![INVALID_TRACKER_TOKEN],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let res = consumer.take_data();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].as_ref().unwrap(), &Some(v1.clone()));
        // scan
        for desc in &[false, true] {
            let mut values = vec![
                Some((k1.clone(), v1.clone())),
                Some((k2.clone(), v2.clone())),
            ];
            let mut key = Key::from_raw(b"\x00");
            if *desc {
                key = Key::from_raw(b"\xff");
                values.reverse();
            }
            expect_multi_values(
                values,
                block_on(storage.scan(ctx.clone(), key, None, 1000, 0, 110.into(), false, *desc))
                    .unwrap(),
            );
        }
    }

    #[test]
    fn test_async_commit_prewrite() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        cm.update_max_ts(10.into(), "").unwrap();

        // Optimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_raw(b"a"), b"v".to_vec()),
                        Mutation::make_put(Key::from_raw(b"b"), b"v".to_vec()),
                        Mutation::make_put(Key::from_raw(b"c"), b"v".to_vec()),
                    ],
                    b"c".to_vec(),
                    100.into(),
                    1000,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    Some(vec![b"a".to_vec(), b"b".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 101.into());

        // Pessimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"d"), false), (Key::from_raw(b"e"), false)],
                    200,
                    300,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        cm.update_max_ts(1000.into(), "").unwrap();

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (
                            Mutation::make_put(Key::from_raw(b"d"), b"v".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"e"), b"v".to_vec()),
                            DoPessimisticCheck,
                        ),
                    ],
                    b"d".to_vec(),
                    200.into(),
                    1000,
                    400.into(),
                    2,
                    401.into(),
                    TimeStamp::default(),
                    Some(vec![b"e".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 1001.into());
    }

    // This is one of the series of tests to test overlapped timestamps.
    // Overlapped ts means there is a rollback record and a commit record with the
    // same ts. In this test we check that if rollback happens before commit, then
    // they should not have overlapped ts, which is an expected property.
    #[test]
    fn test_overlapped_ts_rollback_before_prewrite() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            engine.clone(),
            MockLockManager::new(),
        )
        .build()
        .unwrap();

        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key1 = Key::from_raw(k1);
        let key2 = Key::from_raw(k2);
        let value1 = v1.to_vec();
        let value2 = v2.to_vec();

        let (tx, rx) = channel();

        // T1 acquires lock on k1, start_ts = 1, for_update_ts = 3
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(key1.clone(), false)],
                    k1.to_vec(),
                    1.into(),
                    0,
                    true,
                    3.into(),
                    None,
                    false,
                    0.into(),
                    false,
                    false,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 acquires lock on k2, start_ts = 10, for_update_ts = 15
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(key2.clone(), false)],
                    k2.to_vec(),
                    10.into(),
                    0,
                    true,
                    15.into(),
                    None,
                    false,
                    0.into(),
                    false,
                    false,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 pessimistically prewrites, start_ts = 10, lock ttl = 0
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(key2.clone(), value2.clone()),
                        DoPessimisticCheck,
                    )],
                    k2.to_vec(),
                    10.into(),
                    0,
                    15.into(),
                    1,
                    0.into(),
                    100.into(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T3 checks T2, which rolls back key2 and pushes max_ts to 10
        // use a large timestamp to make the lock expire so key2 will be rolled back.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key2.clone(),
                    10.into(),
                    ((1 << 18) + 8).into(),
                    ((1 << 18) + 8).into(),
                    true,
                    false,
                    false,
                    true,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        must_unlocked(&mut engine, k2);
        must_written(&mut engine, k2, 10, 10, WriteType::Rollback);

        // T1 prewrites, start_ts = 1, for_update_ts = 3
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::make_put(key1.clone(), value1), DoPessimisticCheck),
                        (
                            Mutation::make_put(key2.clone(), value2),
                            SkipPessimisticCheck,
                        ),
                    ],
                    k1.to_vec(),
                    1.into(),
                    0,
                    3.into(),
                    2,
                    0.into(),
                    (1 << 19).into(),
                    Some(vec![k2.to_vec()]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T1.commit_ts must be pushed to be larger than T2.start_ts (if we resolve T1)
        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![key1, key2], 1.into(), Default::default()),
                Box::new(move |res| {
                    let pr = res.unwrap();
                    match pr {
                        SecondaryLocksStatus::Locked(l) => {
                            let min_commit_ts = l
                                .iter()
                                .map(|lock_info| lock_info.min_commit_ts)
                                .max()
                                .unwrap();
                            tx.send(min_commit_ts as i32).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }),
            )
            .unwrap();
        assert!(rx.recv().unwrap() > 10);
    }

    // this test shows that the scheduler take `response_policy` in `WriteResult`
    // serious, ie. call the callback at expected stage when writing to the
    // engine
    #[test]
    fn test_scheduler_response_policy() {
        struct Case<T: 'static + StorageCallbackType + Send> {
            expected_writes: Vec<ExpectedWrite>,

            command: TypedCommand<T>,
            pipelined_pessimistic_lock: bool,
        }

        impl<T: 'static + StorageCallbackType + Send> Case<T> {
            fn run(self) {
                let mut builder =
                    MockEngineBuilder::from_rocks_engine(TestEngineBuilder::new().build().unwrap());
                for expected_write in self.expected_writes {
                    builder = builder.add_expected_write(expected_write)
                }
                let engine = builder.build();
                let mut builder = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
                    engine,
                    MockLockManager::new(),
                );
                builder.config.enable_async_apply_prewrite = true;
                if self.pipelined_pessimistic_lock {
                    builder
                        .pipelined_pessimistic_lock
                        .store(true, Ordering::Relaxed);
                }
                let storage = builder.build().unwrap();
                let (tx, rx) = channel();
                storage
                    .sched_txn_command(
                        self.command,
                        Box::new(move |res| {
                            tx.send(res).unwrap();
                        }),
                    )
                    .unwrap();
                rx.recv().unwrap().unwrap();
            }
        }

        let keys = [b"k1", b"k2"];
        let values = [b"v1", b"v2"];
        let mutations = vec![
            Mutation::make_put(Key::from_raw(keys[0]), keys[0].to_vec()),
            Mutation::make_put(Key::from_raw(keys[1]), values[1].to_vec()),
        ];

        let on_applied_case = Case {
            // this case's command return ResponsePolicy::OnApplied
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
            ],

            command: Prewrite::new(
                mutations.clone(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                None,
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };
        let on_commited_case = Case {
            // this case's command return ResponsePolicy::OnCommitted
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new().expect_committed_cb(),
                ExpectedWrite::new().expect_committed_cb(),
            ],

            command: Prewrite::new(
                mutations,
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };
        let on_proposed_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // untested, but all AcquirePessimisticLock should return ResponsePolicy::OnProposed now
            // and the scheduler expected to take OnProposed serious when
            // enable pipelined pessimistic lock
            expected_writes: vec![
                ExpectedWrite::new().expect_proposed_cb(),
                ExpectedWrite::new().expect_proposed_cb(),
            ],

            command: AcquirePessimisticLock::new(
                keys.iter().map(|&it| (Key::from_raw(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                false,
                false,
                false,
                Context::default(),
            ),
            pipelined_pessimistic_lock: true,
        };
        let on_proposed_fallback_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // but when pipelined pessimistic lock is off,
            // the scheduler should fallback to use OnApplied
            expected_writes: vec![
                ExpectedWrite::new().expect_no_proposed_cb(),
                ExpectedWrite::new().expect_no_proposed_cb(),
            ],

            command: AcquirePessimisticLock::new(
                keys.iter().map(|&it| (Key::from_raw(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                false,
                false,
                false,
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };

        on_applied_case.run();
        on_commited_case.run();
        on_proposed_case.run();
        on_proposed_fallback_case.run();
    }

    #[test]
    fn test_resolve_commit_pessimistic_locks() {
        let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let (tx, rx) = channel();

        // Pessimistically lock k1, k2, k3, k4, after the pessimistic retry k2 is no
        // longer needed and the pessimistic lock on k2 is left.
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![
                        (Key::from_raw(b"k1"), false),
                        (Key::from_raw(b"k2"), false),
                        (Key::from_raw(b"k3"), false),
                        (Key::from_raw(b"k4"), false),
                        (Key::from_raw(b"k5"), false),
                        (Key::from_raw(b"k6"), false),
                    ],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Prewrite keys except the k2.
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::with_defaults(
                    vec![
                        (
                            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k3"), b"v2".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k4"), b"v4".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k5"), b"v5".to_vec()),
                            DoPessimisticCheck,
                        ),
                        (
                            Mutation::make_put(Key::from_raw(b"k6"), b"v6".to_vec()),
                            DoPessimisticCheck,
                        ),
                    ],
                    b"k1".to_vec(),
                    10.into(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit the primary key.
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k1")],
                    10.into(),
                    20.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Pessimistically rollback the k2 lock.
        // Non lite lock resolve on k1 and k2, there should no errors as lock on k2 is
        // pessimistic type.
        must_rollback(&mut storage.engine, b"k2", 10, false);
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        storage
            .sched_txn_command(
                commands::ResolveLock::new(
                    temp_map.clone(),
                    None,
                    vec![
                        (
                            Key::from_raw(b"k1"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v1".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                                false,
                            ),
                        ),
                        (
                            Key::from_raw(b"k2"),
                            mvcc::Lock::new(
                                mvcc::LockType::Pessimistic,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                None,
                                10.into(),
                                0,
                                11.into(),
                                false,
                            ),
                        ),
                    ],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Non lite lock resolve on k3 and k4, there should be no errors.
        storage
            .sched_txn_command(
                commands::ResolveLock::new(
                    temp_map.clone(),
                    None,
                    vec![
                        (
                            Key::from_raw(b"k3"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v3".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                                false,
                            ),
                        ),
                        (
                            Key::from_raw(b"k4"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v4".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                                false,
                            ),
                        ),
                    ],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Unlock the k6 first.
        // Non lite lock resolve on k5 and k6, error should be reported.
        must_rollback(&mut storage.engine, b"k6", 10, true);
        storage
            .sched_txn_command(
                commands::ResolveLock::new(
                    temp_map,
                    None,
                    vec![
                        (
                            Key::from_raw(b"k5"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v5".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                                false,
                            ),
                        ),
                        (
                            Key::from_raw(b"k6"),
                            mvcc::Lock::new(
                                mvcc::LockType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v6".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                                false,
                            ),
                        ),
                    ],
                    Context::default(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    // Test check_api_version.
    // See the following for detail:
    //   * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
    //   * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto,
    //     enum APIVersion.
    #[test]
    fn test_check_api_version() {
        use error_code::storage::*;

        const TIDB_KEY_CASE: &[u8] = b"t_a";
        const TXN_KEY_CASE: &[u8] = b"x\0a";
        const RAW_KEY_CASE: &[u8] = b"r\0a";

        let test_data = vec![
            // storage api_version = V1, for backward compatible.
            (
                ApiVersion::V1,                    // storage api_version
                ApiVersion::V1,                    // request api_version
                CommandKind::get,                  // command kind
                vec![TIDB_KEY_CASE, RAW_KEY_CASE], // keys
                None,                              // expected error code
            ),
            (
                ApiVersion::V1,
                ApiVersion::V1,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE, TXN_KEY_CASE],
                None,
            ),
            // storage api_version = V1ttl, allow RawKV request only.
            (
                ApiVersion::V1ttl,
                ApiVersion::V1,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            // storage api_version = V1, reject V2 request.
            (
                ApiVersion::V1,
                ApiVersion::V2,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            // storage api_version = V2.
            // backward compatible for TiDB request, and TiDB request only.
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE, TIDB_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::raw_get,
                vec![TIDB_KEY_CASE, TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![RAW_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            // V2 api validation.
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![TXN_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE, RAW_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![RAW_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::raw_get,
                vec![RAW_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
        ];

        for (i, (storage_api_version, req_api_version, cmd, keys, err)) in
            test_data.into_iter().enumerate()
        {
            // TODO: refactor to use `Api` parameter.
            let res = StorageApiV1::<RocksEngine, MockLockManager>::check_api_version(
                storage_api_version,
                req_api_version,
                cmd,
                keys,
            );
            if let Some(err) = err {
                assert!(res.is_err(), "case {}", i);
                assert_eq!(res.unwrap_err().error_code(), err, "case {}", i);
            } else {
                assert!(res.is_ok(), "case {} {:?}", i, res);
            }
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_check_api_version_ranges() {
        use error_code::storage::*;

        const TIDB_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"t_a"), Some(b"t_z")),
            (Some(b"t"), Some(b"u")),
            (Some(b"m"), Some(b"n")),
            (Some(b"m_a"), Some(b"m_z")),
        ];
        const TXN_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] =
            &[(Some(b"x\0a"), Some(b"x\0z")), (Some(b"x"), Some(b"y"))];
        const RAW_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] =
            &[(Some(b"r\0a"), Some(b"r\0z")), (Some(b"r"), Some(b"s"))];
        // The cases that should fail in API V2
        const TIDB_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"t_a"), Some(b"ua")),
            (Some(b"t"), None),
            (None, Some(b"t_z")),
            (Some(b"m_a"), Some(b"na")),
            (Some(b"m"), None),
            (None, Some(b"m_z")),
        ];
        const TXN_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"x\0a"), Some(b"ya")),
            (Some(b"x"), None),
            (None, Some(b"x\0z")),
        ];
        const RAW_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"r\0a"), Some(b"sa")),
            (Some(b"r"), None),
            (None, Some(b"r\0z")),
        ];

        let test_case = |storage_api_version,
                         req_api_version,
                         cmd,
                         range: &[(Option<&[u8]>, Option<&[u8]>)],
                         err| {
            // TODO: refactor to use `Api` parameter.
            let res = StorageApiV1::<RocksEngine, MockLockManager>::check_api_version_ranges(
                storage_api_version,
                req_api_version,
                cmd,
                range.iter().cloned(),
            );
            if let Some(err) = err {
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().error_code(), err);
            } else {
                res.unwrap();
            }
        };

        // storage api_version = V1, for backward compatible.
        test_case(
            ApiVersion::V1,    // storage api_version
            ApiVersion::V1,    // request api_version
            CommandKind::scan, // command kind
            TIDB_KEY_CASE,     // ranges
            None,              // expected error code
        );
        test_case(
            ApiVersion::V1,
            ApiVersion::V1,
            CommandKind::raw_scan,
            TIDB_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V1,
            ApiVersion::V1,
            CommandKind::raw_scan,
            TIDB_KEY_CASE_APIV2_ERR,
            None,
        );
        // storage api_version = V1ttl, allow RawKV request only.
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::raw_scan,
            RAW_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::raw_scan,
            RAW_KEY_CASE_APIV2_ERR,
            None,
        );
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::scan,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        // storage api_version = V1, reject V2 request.
        test_case(
            ApiVersion::V1,
            ApiVersion::V2,
            CommandKind::scan,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        // storage api_version = V2.
        // backward compatible for TiDB request, and TiDB request only.
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::scan,
            TIDB_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::raw_scan,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::scan,
            TXN_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::scan,
            RAW_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        // V2 api validation.
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::scan,
            TXN_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::raw_scan,
            RAW_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::scan,
            RAW_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::raw_scan,
            TXN_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::scan,
            TIDB_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );

        for range in TIDB_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::scan,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
        for range in TXN_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::scan,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
        for range in RAW_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::raw_scan,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
    }

    #[test]
    fn test_write_in_memory_pessimistic_locks() {
        let txn_ext = Arc::new(TxnExt::default());
        let lock_mgr = MockLockManager::new();
        let storage = TestStorageBuilderApiV1::new(lock_mgr.clone())
            .pipelined_pessimistic_lock(true)
            .in_memory_pessimistic_lock(true)
            .build_for_txn(txn_ext.clone())
            .unwrap();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(k1.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        {
            let pessimistic_locks = txn_ext.pessimistic_locks.read();
            let lock = pessimistic_locks.get(&k1).unwrap();
            assert_eq!(
                lock,
                &(
                    PessimisticLock {
                        primary: Box::new(*b"k1"),
                        start_ts: 10.into(),
                        ttl: 3000,
                        for_update_ts: 10.into(),
                        min_commit_ts: 11.into(),
                        last_change: LastChange::NotExist,
                        is_locked_with_conflict: false,
                    },
                    false
                )
            );
        }

        let (tx, rx) = channel();
        // The written in-memory pessimistic lock should be visible, so the new lock
        // request should fail.
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(k1.clone(), false)],
                    20,
                    20,
                    false,
                    false,
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        // The request enters lock waiting state.
        rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
        lock_mgr.simulate_timeout_all();
        // The lock-waiting request is cancelled.
        rx.recv().unwrap().unwrap_err();

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        DoPessimisticCheck,
                    )],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    20.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap().unwrap();
        // After prewrite, the memory lock should be removed.
        {
            let pessimistic_locks = txn_ext.pessimistic_locks.read();
            assert!(pessimistic_locks.get(&k1).is_none());
        }
    }

    #[test]
    fn test_disable_in_memory_pessimistic_locks() {
        let txn_ext = Arc::new(TxnExt::default());
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .pipelined_pessimistic_lock(true)
            .in_memory_pessimistic_lock(false)
            .build_for_txn(txn_ext.clone())
            .unwrap();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(k1.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // When disabling in-memory pessimistic lock, the lock map should remain
        // unchanged.
        assert!(txn_ext.pessimistic_locks.read().is_empty());

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::make_put(k1, b"v".to_vec()), DoPessimisticCheck)],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    20.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        // Prewrite still succeeds
        rx.recv().unwrap().unwrap();
    }

    #[test]
    fn test_prewrite_cached_committed_transaction_do_not_skip_constraint_check() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let k1 = Key::from_raw(b"k1");
        let pk = b"pk";
        // Simulate the case that the current TiKV instance have a non-unique
        // index key of a pessimistic transaction. It won't be pessimistic
        // locked, and prewrite skips constraint checks.
        // Simulate the case that a prewrite is performed twice, with async
        // commit enabled, and max_ts changes when the second request arrives.

        // A retrying prewrite request arrives.
        cm.update_max_ts(20.into(), "").unwrap();
        let mut ctx = Context::default();
        ctx.set_is_retry_request(true);
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        SkipPessimisticCheck,
                    )],
                    pk.to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    11.into(),
                    0.into(),
                    Some(vec![]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    ctx,
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();

        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 21.into());

        // Commit it.
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![k1.clone()],
                    10.into(),
                    21.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // The txn's status is cached
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(10.into())
                .unwrap(),
            21.into()
        );

        // Check committed; push max_ts to 30
        assert_eq!(
            block_on(storage.get(Context::default(), k1.clone(), 30.into()))
                .unwrap()
                .0,
            Some(b"v".to_vec())
        );

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        SkipPessimisticCheck,
                    )],
                    pk.to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    11.into(),
                    0.into(),
                    Some(vec![]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 21.into());

        // Key must not be locked.
        assert_eq!(
            block_on(storage.get(Context::default(), k1, 50.into()))
                .unwrap()
                .0,
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn test_updating_txn_status_cache() {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();

        // Commit
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                        SkipPessimisticCheck,
                    )],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    11.into(),
                    0.into(),
                    Some(vec![]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(10.into())
                .is_none()
        );

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k1")],
                    10.into(),
                    20.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(10.into())
                .unwrap(),
            20.into()
        );

        // Unsuccessful commit won't update cache
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k2")],
                    30.into(),
                    40.into(),
                    None,
                    Context::default(),
                ),
                expect_fail_callback(tx, 0, |_| ()),
            )
            .unwrap();
        rx.recv().unwrap();
        assert!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(30.into())
                .is_none()
        );

        // 1PC update
        let (tx, rx) = channel();
        cm.update_max_ts(59.into(), "").unwrap();
        storage
            .sched_txn_command(
                Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(b"k3"), b"v3".to_vec())],
                    b"k3".to_vec(),
                    50.into(),
                    3000,
                    false,
                    1,
                    51.into(),
                    0.into(),
                    Some(vec![]),
                    true,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.one_pc_commit_ts, 60.into());
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(50.into())
                .unwrap(),
            60.into()
        );

        // Resolve lock commit
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(b"k4"), b"v4".to_vec())],
                    b"pk".to_vec(),
                    70.into(),
                    3000,
                    false,
                    1,
                    0.into(),
                    0.into(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(
                    vec![(TimeStamp::from(70), TimeStamp::from(80))]
                        .into_iter()
                        .collect(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(70.into())
                .unwrap(),
            80.into()
        );

        // Resolve lock lite
        storage
            .sched_txn_command(
                Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(b"k5"), b"v5".to_vec())],
                    b"pk".to_vec(),
                    90.into(),
                    3000,
                    false,
                    1,
                    0.into(),
                    0.into(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    90.into(),
                    100.into(),
                    vec![Key::from_raw(b"k5")],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(90.into())
                .unwrap(),
            100.into()
        );

        // CheckTxnStatus: uncommitted transaction
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    Key::from_raw(b"k1"),
                    9.into(),
                    110.into(),
                    110.into(),
                    true,
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(9.into())
                .is_none()
        );

        // CheckTxnStatus: committed transaction
        storage
            .sched
            .get_txn_status_cache()
            .remove_normal(10.into());
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    Key::from_raw(b"k1"),
                    10.into(),
                    110.into(),
                    110.into(),
                    true,
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(10.into())
                .unwrap(),
            20.into()
        );

        // CheckSecondaryLocks: uncommitted transaction
        storage
            .sched_txn_command(
                Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(b"k6"), b"v6".to_vec())],
                    b"pk".to_vec(),
                    120.into(),
                    3000,
                    false,
                    1,
                    0.into(),
                    0.into(),
                    Some(vec![]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Lock exists but the transaction status is still unknown
        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![Key::from_raw(b"k6")],
                    120.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(120.into())
                .is_none()
        );

        // One of the lock doesn't exist so the transaction becomes rolled-back status.
        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![Key::from_raw(b"k6"), Key::from_raw(b"k7")],
                    120.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(120.into())
                .is_none()
        );

        // CheckSecondaryLocks: committed transaction
        storage
            .sched_txn_command(
                Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_raw(b"k8"), b"v8".to_vec()),
                        Mutation::make_put(Key::from_raw(b"k9"), b"v9".to_vec()),
                    ],
                    b"pk".to_vec(),
                    130.into(),
                    3000,
                    false,
                    1,
                    0.into(),
                    0.into(),
                    Some(vec![]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Commit one of the key
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k9")],
                    130.into(),
                    140.into(),
                    None,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .remove_normal(130.into())
                .unwrap(),
            140.into()
        );

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![Key::from_raw(b"k8"), Key::from_raw(b"k9")],
                    130.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(
            storage
                .sched
                .get_txn_status_cache()
                .get_committed_no_promote(130.into())
                .unwrap(),
            140.into()
        );
    }

    #[test]
    fn test_pessimistic_rollback_with_scan_first() {
        use crate::storage::txn::tests::must_pessimistic_locked;
        let format_key = |prefix: char, i: usize| format!("{}{:04}", prefix, i).as_bytes().to_vec();
        let k1 = format_key('k', 1);
        let k2 = format_key('k', 2);
        let start_ts = 10;
        let for_update_ts = 10;
        for enable_in_memory_lock in [true, false] {
            let txn_ext = Arc::new(TxnExt::default());
            let mut storage = TestStorageBuilderApiV1::new(MockLockManager::new())
                .pipelined_pessimistic_lock(enable_in_memory_lock)
                .in_memory_pessimistic_lock(enable_in_memory_lock)
                .build_for_txn(txn_ext.clone())
                .unwrap();

            // Basic case, two keys could be rolled back within one pessimistic rollback
            // request.
            acquire_pessimistic_lock(
                &storage,
                Key::from_raw(k1.as_slice()),
                start_ts,
                for_update_ts,
            );
            acquire_pessimistic_lock(
                &storage,
                Key::from_raw(k2.as_slice()),
                start_ts,
                for_update_ts,
            );
            must_pessimistic_locked(&mut storage.engine, k1.as_slice(), start_ts, for_update_ts);
            delete_pessimistic_lock_with_scan_first(&storage, start_ts, for_update_ts);
            must_unlocked(&mut storage.engine, k1.as_slice());
            must_unlocked(&mut storage.engine, k2.as_slice());

            // Acquire pessimistic locks for more than 256 keys.
            // Only pessimistic locks should be rolled back.
            let start_ts = 11;
            let for_update_ts = 11;
            let num_keys = 400;
            let prewrite_primary_key = format_key('k', 1);
            for i in 0..num_keys {
                let key = format_key('k', i);
                if i % 2 == 0 {
                    acquire_pessimistic_lock(
                        &storage,
                        Key::from_raw(key.as_slice()),
                        start_ts,
                        for_update_ts,
                    );
                } else {
                    prewrite_lock(
                        &storage,
                        Key::from_raw(key.as_slice()),
                        prewrite_primary_key.as_slice(),
                        b"value",
                        start_ts,
                    );
                }
            }
            {
                let pessimistic_locks = txn_ext.pessimistic_locks.read();
                if enable_in_memory_lock {
                    let k0 = format_key('k', 0);
                    let lock = pessimistic_locks
                        .get(&Key::from_raw(k0.as_slice()))
                        .unwrap();
                    assert_eq!(
                        lock,
                        &(
                            PessimisticLock {
                                primary: Box::new(*b"k0000"),
                                start_ts: start_ts.into(),
                                ttl: 3000,
                                for_update_ts: for_update_ts.into(),
                                min_commit_ts: (for_update_ts + 1).into(),
                                last_change: LastChange::NotExist,
                                is_locked_with_conflict: false,
                            },
                            false
                        )
                    );
                } else {
                    assert_eq!(pessimistic_locks.len(), 0);
                }
            }
            delete_pessimistic_lock_with_scan_first(&storage, start_ts, for_update_ts);
            for i in 0..num_keys {
                let key = format_key('k', i);
                if i % 2 == 0 {
                    must_unlocked(&mut storage.engine, key.as_slice());
                } else {
                    must_locked(&mut storage.engine, key.as_slice(), start_ts);
                }
            }
        }
    }
}
