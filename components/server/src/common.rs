// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This mod is exported to make convenience for creating TiKV-like servers.

use std::{
    cmp,
    collections::HashMap,
    env, fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc, Arc,
    },
    time::Duration,
    u64,
};

use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{
    flush_engine_statistics,
    raw::{Cache, Env},
    FlowInfo, RocksEngine, RocksStatistics,
};
use engine_traits::{
    data_cf_offset, CachedTablet, CfOptions, CfOptionsExt, FlowControlFactorsExt, KvEngine,
    RaftEngine, RangeCacheEngine, StatisticsReporter, TabletRegistry, CF_DEFAULT, DATA_CFS,
};
use error_code::ErrorCodeExt;
use file_system::{get_io_rate_limiter, set_io_rate_limiter, BytesFetcher, File, IoBudgetAdjustor};
use grpcio::Environment;
use hybrid_engine::HybridEngine;
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::coprocessor::RegionInfoProvider;
use region_cache_memory_engine::{
    flush_range_cache_engine_statistics, RangeCacheEngineContext, RangeCacheMemoryEngine,
    RangeCacheMemoryEngineStatistics,
};
use security::SecurityManager;
use tikv::{
    config::{ConfigController, DbConfigManger, DbType, TikvConfig},
    server::{status_server::StatusServer, DEFAULT_CLUSTER_ID},
};
use tikv_util::{
    config::{ensure_dir_exist, RaftDataStateMachine},
    math::MovingAvgU32,
    metrics::INSTANCE_BACKEND_CPU_QUOTA,
    quota_limiter::QuotaLimiter,
    sys::{cpu_time::ProcessStat, disk, path_in_diff_mount_point, SysQuota},
    time::Instant,
    worker::{LazyWorker, Worker},
};

use crate::{raft_engine_switch::*, setup::validate_and_persist_config};

// minimum number of core kept for background requests
const BACKGROUND_REQUEST_CORE_LOWER_BOUND: f64 = 1.0;
// max ratio of core quota for background requests
const BACKGROUND_REQUEST_CORE_MAX_RATIO: f64 = 0.95;
// default ratio of core quota for background requests = core_number * 0.5
const BACKGROUND_REQUEST_CORE_DEFAULT_RATIO: f64 = 0.5;
// indication of TiKV instance is short of cpu
const SYSTEM_BUSY_THRESHOLD: f64 = 0.80;
// indication of TiKV instance in healthy state when cpu usage is in [0.5, 0.80)
const SYSTEM_HEALTHY_THRESHOLD: f64 = 0.50;
// pace of cpu quota adjustment
const CPU_QUOTA_ADJUSTMENT_PACE: f64 = 200.0; // 0.2 vcpu
const DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL: Duration = Duration::from_secs(5);

/// This is the common part of TiKV-like servers. It is a collection of all
/// capabilities a TikvServer should have or may take advantage of. By holding
/// it in its own TikvServer implementation, one can easily access the common
/// ability of a TiKV server.
// Fields in this struct are all public since they are open for other TikvServer
// to use, e.g. a custom TikvServer may alter some fields in `config` or push
// some services into `to_stop`.
pub struct TikvServerCore {
    pub config: TikvConfig,
    pub store_path: PathBuf,
    pub lock_files: Vec<File>,
    pub encryption_key_manager: Option<Arc<DataKeyManager>>,
    pub flow_info_sender: Option<mpsc::Sender<FlowInfo>>,
    pub flow_info_receiver: Option<mpsc::Receiver<FlowInfo>>,
    pub to_stop: Vec<Box<dyn Stop>>,
    pub background_worker: Worker,
}

impl TikvServerCore {
    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config
    ///   to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    pub fn init_config(mut config: TikvConfig) -> ConfigController {
        validate_and_persist_config(&mut config, true);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }
        if config.raft_engine.enable {
            ensure_dir_exist(&config.raft_engine.config().dir).unwrap();
        } else {
            ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
            if !config.raftdb.wal_dir.is_empty() {
                ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
            }
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    pub fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_lock_dir();

        let search_base = env::temp_dir().join(lock_dir);
        file_system::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in file_system::read_dir(&search_base).unwrap().flatten() {
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                let ip = addr.ip();
                let port = addr.port();
                if cur_port == port
                    && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                {
                    let _ = try_lock_conflict_addr(file_path);
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    pub fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }

        // Allocate a big file to make sure that TiKV have enough space to
        // recover from disk full errors. This file is created in data_dir rather than
        // db_path, because we must not increase store size of db_path.
        fn calculate_reserved_space(capacity: u64, reserved_size_from_config: u64) -> u64 {
            let mut reserved_size = reserved_size_from_config;
            if reserved_size_from_config != 0 {
                reserved_size =
                    cmp::max((capacity as f64 * 0.05) as u64, reserved_size_from_config);
            }
            reserved_size
        }
        fn reserve_physical_space(data_dir: &String, available: u64, reserved_size: u64) {
            let path = Path::new(data_dir).join(file_system::SPACE_PLACEHOLDER_FILE);
            if let Err(e) = file_system::remove_file(path) {
                warn!("failed to remove space holder on starting: {}", e);
            }

            // place holder file size is 20% of total reserved space.
            if available > reserved_size {
                file_system::reserve_space_for_recover(data_dir, reserved_size / 5)
                    .map_err(|e| panic!("Failed to reserve space for recovery: {}.", e))
                    .unwrap();
            } else {
                warn!("no enough disk space left to create the place holder file");
            }
        }

        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }
        // reserve space for kv engine
        let kv_reserved_size =
            calculate_reserved_space(capacity, self.config.storage.reserve_space.0);
        disk::set_disk_reserved_space(kv_reserved_size);
        reserve_physical_space(
            &self.config.storage.data_dir,
            disk_stats.available_space(),
            kv_reserved_size,
        );

        let raft_data_dir = if self.config.raft_engine.enable {
            self.config.raft_engine.config().dir
        } else {
            self.config.raft_store.raftdb_path.clone()
        };

        let separated_raft_mount_path =
            path_in_diff_mount_point(&self.config.storage.data_dir, &raft_data_dir);
        if separated_raft_mount_path {
            let raft_disk_stats = fs2::statvfs(&raft_data_dir).unwrap();
            // reserve space for raft engine if raft engine is deployed separately
            let raft_reserved_size = calculate_reserved_space(
                raft_disk_stats.total_space(),
                self.config.storage.reserve_raft_space.0,
            );
            disk::set_raft_disk_reserved_space(raft_reserved_size);
            reserve_physical_space(
                &raft_data_dir,
                raft_disk_stats.available_space(),
                raft_reserved_size,
            );
        }
    }

    pub fn init_yatp(&self) {
        yatp::metrics::set_namespace(Some("tikv"));
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::TASK_EXEC_DURATION.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::TASK_POLL_DURATION.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::TASK_EXEC_TIMES.clone())).unwrap();
    }

    pub fn init_encryption(&mut self) {
        self.encryption_key_manager = data_key_manager_from_config(
            &self.config.security.encryption,
            &self.config.storage.data_dir,
        )
        .map_err(|e| {
            panic!(
                "Encryption failed to initialize: {}. code: {}",
                e,
                e.error_code()
            )
        })
        .unwrap()
        .map(Arc::new);
    }

    pub fn init_io_utility(&mut self) -> BytesFetcher {
        let stats_collector_enabled = file_system::init_io_stats_collector()
            .map_err(|e| warn!("failed to init I/O stats collector: {}", e))
            .is_ok();

        let limiter = Arc::new(
            self.config
                .storage
                .io_rate_limit
                .build(!stats_collector_enabled /* enable_statistics */),
        );
        let fetcher = if stats_collector_enabled {
            BytesFetcher::FromIoStatsCollector()
        } else {
            BytesFetcher::FromRateLimiter(limiter.statistics().unwrap())
        };
        // Set up IO limiter even when rate limit is disabled, so that rate limits can
        // be dynamically applied later on.
        set_io_rate_limiter(Some(limiter));
        fetcher
    }

    pub fn init_flow_receiver(&mut self) -> engine_rocks::FlowListener {
        let (tx, rx) = mpsc::channel();
        self.flow_info_sender = Some(tx.clone());
        self.flow_info_receiver = Some(rx);
        engine_rocks::FlowListener::new(tx)
    }

    pub fn connect_to_pd_cluster(
        config: &mut TikvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    // Only background cpu quota tuning is implemented at present. iops and frontend
    // quota tuning is on the way
    pub fn init_quota_tuning_task(&self, quota_limiter: Arc<QuotaLimiter>) {
        // No need to do auto tune when capacity is really low
        if SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO
            < BACKGROUND_REQUEST_CORE_LOWER_BOUND
        {
            return;
        };

        // Determine the base cpu quota
        let base_cpu_quota =
            // if cpu quota is not specified, start from optimistic case
            if quota_limiter.cputime_limiter(false).is_infinite() {
                1000_f64
                    * f64::max(
                        BACKGROUND_REQUEST_CORE_LOWER_BOUND,
                        SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_DEFAULT_RATIO,
                    )
            } else {
                quota_limiter.cputime_limiter(false) / 1000_f64
            };

        // Calculate the celling and floor quota
        let celling_quota = f64::min(
            base_cpu_quota * 2.0,
            1_000_f64 * SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO,
        );
        let floor_quota = f64::max(
            base_cpu_quota * 0.5,
            1_000_f64 * BACKGROUND_REQUEST_CORE_LOWER_BOUND,
        );

        let mut proc_stats: ProcessStat = ProcessStat::cur_proc_stat().unwrap();
        self.background_worker.spawn_interval_task(
            DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL,
            move || {
                if quota_limiter.auto_tune_enabled() {
                    let cputime_limit = quota_limiter.cputime_limiter(false);
                    let old_quota = if cputime_limit.is_infinite() {
                        base_cpu_quota
                    } else {
                        cputime_limit / 1000_f64
                    };
                    let cpu_usage = match proc_stats.cpu_usage() {
                        Ok(r) => r,
                        Err(_e) => 0.0,
                    };
                    // Try tuning quota when cpu_usage is correctly collected.
                    // rule based tuning:
                    // - if instance is busy, shrink cpu quota for analyze by one quota pace until
                    //   lower bound is hit;
                    // - if instance cpu usage is healthy, no op;
                    // - if instance is idle, increase cpu quota by one quota pace  until upper
                    //   bound is hit.
                    if cpu_usage > 0.0f64 {
                        let mut target_quota = old_quota;

                        let cpu_util = cpu_usage / SysQuota::cpu_cores_quota();
                        if cpu_util >= SYSTEM_BUSY_THRESHOLD {
                            target_quota =
                                f64::max(target_quota - CPU_QUOTA_ADJUSTMENT_PACE, floor_quota);
                        } else if cpu_util < SYSTEM_HEALTHY_THRESHOLD {
                            target_quota =
                                f64::min(target_quota + CPU_QUOTA_ADJUSTMENT_PACE, celling_quota);
                        }

                        if old_quota != target_quota {
                            quota_limiter.set_cpu_time_limit(target_quota as usize, false);
                            debug!(
                                "cpu_time_limiter tuned for backend request";
                                "cpu_util" => ?cpu_util,
                                "new_quota" => ?target_quota);
                            INSTANCE_BACKEND_CPU_QUOTA.set(target_quota as i64);
                        }
                    }
                }
            },
        );
    }
}

#[cfg(unix)]
fn get_lock_dir() -> String {
    format!("{}_TIKV_LOCK_FILES", unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn get_lock_dir() -> String {
    "TIKV_LOCK_FILES".to_owned()
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create lock at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is binding with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

const RESERVED_OPEN_FDS: u64 = 1000;
pub fn check_system_config(config: &TikvConfig) {
    info!("beginning system configuration check");
    let mut rocksdb_max_open_files = config.rocksdb.max_open_files;
    if let Some(true) = config.rocksdb.titan.enabled {
        // Titan engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as rocksdb does. So we double the max required
        // open files here
        rocksdb_max_open_files *= 2;
    }
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (rocksdb_max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "check: rocksdb-data-dir";
            "path" => &config.storage.data_dir,
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "check: raftdb-path";
            "path" => &config.raft_store.raftdb_path,
            "err" => %e
        );
    }
}

pub struct EnginesResourceInfo {
    tablet_registry: TabletRegistry<RocksEngine>,
    // The initial value of max_compactions.
    base_max_compactions: [u32; 3],
    raft_engine: Option<RocksEngine>,
    latest_normalized_pending_bytes: AtomicU32,
    normalized_pending_bytes_collector: MovingAvgU32,
}

impl EnginesResourceInfo {
    const SCALE_FACTOR: u64 = 100;

    pub fn new(
        config: &TikvConfig,
        tablet_registry: TabletRegistry<RocksEngine>,
        raft_engine: Option<RocksEngine>,
        max_samples_to_preserve: usize,
    ) -> Self {
        // Match DATA_CFS.
        let base_max_compactions = [
            config.rocksdb.defaultcf.max_compactions.unwrap_or(0),
            config.rocksdb.lockcf.max_compactions.unwrap_or(0),
            config.rocksdb.writecf.max_compactions.unwrap_or(0),
        ];
        EnginesResourceInfo {
            tablet_registry,
            base_max_compactions,
            raft_engine,
            latest_normalized_pending_bytes: AtomicU32::new(0),
            normalized_pending_bytes_collector: MovingAvgU32::new(max_samples_to_preserve),
        }
    }

    pub fn update(
        &self,
        _now: Instant,
        cached_latest_tablets: &mut HashMap<u64, CachedTablet<RocksEngine>>,
    ) {
        let mut compaction_pending_bytes = [0; DATA_CFS.len()];
        let mut soft_pending_compaction_bytes_limit = [0; DATA_CFS.len()];
        // level0 file number ratio within [compaction trigger, slowdown trigger].
        let mut level0_ratio = [0.0f32; DATA_CFS.len()];

        let mut fetch_engine_cf = |engine: &RocksEngine, cf: &str| {
            if let Ok(cf_opts) = engine.get_options_cf(cf) {
                let offset = data_cf_offset(cf);
                if let Ok(Some(b)) = engine.get_cf_pending_compaction_bytes(cf) {
                    compaction_pending_bytes[offset] += b;
                    soft_pending_compaction_bytes_limit[offset] = cmp::max(
                        cf_opts.get_soft_pending_compaction_bytes_limit(),
                        soft_pending_compaction_bytes_limit[offset],
                    );
                }
                if let Ok(Some(n)) = engine.get_cf_num_files_at_level(cf, 0) {
                    let level0 = n as f32;
                    let slowdown_trigger = cf_opts.get_level_zero_slowdown_writes_trigger() as f32;
                    let compaction_trigger =
                        cf_opts.get_level_zero_file_num_compaction_trigger() as f32;
                    let ratio = if slowdown_trigger > compaction_trigger {
                        (level0 - compaction_trigger) / (slowdown_trigger - compaction_trigger)
                    } else {
                        1.0
                    };

                    if ratio > level0_ratio[offset] {
                        level0_ratio[offset] = ratio;
                    }
                }
            }
        };

        if let Some(raft_engine) = &self.raft_engine {
            fetch_engine_cf(raft_engine, CF_DEFAULT);
        }

        self.tablet_registry
            .for_each_opened_tablet(|id, db: &mut CachedTablet<RocksEngine>| {
                cached_latest_tablets.insert(id, db.clone());
                true
            });

        for (_, cache) in cached_latest_tablets.iter_mut() {
            let Some(tablet) = cache.latest() else {
                continue;
            };
            for cf in DATA_CFS {
                fetch_engine_cf(tablet, cf);
            }
        }

        let mut normalized_pending_bytes = 0;
        for (i, (pending, limit)) in compaction_pending_bytes
            .iter()
            .zip(soft_pending_compaction_bytes_limit)
            .enumerate()
        {
            if limit > 0 {
                normalized_pending_bytes = cmp::max(
                    normalized_pending_bytes,
                    (*pending * EnginesResourceInfo::SCALE_FACTOR / limit) as u32,
                );
                let base = self.base_max_compactions[i];
                if base > 0 {
                    let level = *pending as f32 / limit as f32;
                    // 50% -> 1, 70% -> 2, 85% -> 3, 95% -> 6, 98% -> 1024.
                    let delta1 = if level > 0.98 {
                        1024
                    } else if level > 0.95 {
                        cmp::min(SysQuota::cpu_cores_quota() as u32 - 2, 6)
                    } else if level > 0.85 {
                        3
                    } else if level > 0.7 {
                        2
                    } else {
                        u32::from(level > 0.5)
                    };
                    // 20% -> 1, 60% -> 2, 80% -> 3, 90% -> 6, 98% -> 1024.
                    let delta2 = if level0_ratio[i] > 0.98 {
                        // effectively disable the limiter.
                        1024
                    } else if level0_ratio[i] > 0.9 {
                        cmp::min(SysQuota::cpu_cores_quota() as u32 - 2, 6)
                    } else if level0_ratio[i] > 0.8 {
                        3
                    } else if level0_ratio[i] > 0.6 {
                        2
                    } else {
                        u32::from(level0_ratio[i] > 0.2)
                    };
                    let delta = cmp::max(delta1, delta2);
                    let cf = DATA_CFS[i];
                    if delta != 0 {
                        info!(
                            "adjusting `max-compactions`";
                            "cf" => cf,
                            "n" => base + delta,
                            "pending_bytes" => *pending,
                            "soft_limit" => limit,
                            "level0_ratio" => level0_ratio[i],
                        );
                    }
                    // We cannot get the current limit from limiter to avoid repeatedly setting the
                    // same value. But this operation is as simple as an atomic store.
                    cached_latest_tablets.iter_mut().any(|(_, tablet)| {
                        if let Some(latest) = tablet.latest() {
                            let opts = latest.get_options_cf(cf).unwrap();
                            if let Err(e) = opts.set_max_compactions(base + delta) {
                                error!("failed to adjust `max-compactions`"; "err" => ?e);
                            }
                            true
                        } else {
                            false
                        }
                    });
                }
            }
        }

        // Clear ensures that these tablets are not hold forever.
        cached_latest_tablets.clear();

        let (_, avg) = self
            .normalized_pending_bytes_collector
            .add(normalized_pending_bytes);
        self.latest_normalized_pending_bytes.store(
            std::cmp::max(normalized_pending_bytes, avg),
            Ordering::Relaxed,
        );
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn latest_normalized_pending_bytes(&self) -> u32 {
        self.latest_normalized_pending_bytes.load(Ordering::Relaxed)
    }
}

impl IoBudgetAdjustor for EnginesResourceInfo {
    fn adjust(&self, total_budgets: usize) -> usize {
        let score = self.latest_normalized_pending_bytes.load(Ordering::Relaxed) as f32
            / Self::SCALE_FACTOR as f32;
        // Two reasons for adding `sqrt` on top:
        // 1) In theory the convergence point is independent of the value of pending
        //    bytes (as long as backlog generating rate equals consuming rate, which is
        //    determined by compaction budgets), a convex helps reach that point while
        //    maintaining low level of pending bytes.
        // 2) Variance of compaction pending bytes grows with its magnitude, a filter
        //    with decreasing derivative can help balance such trend.
        let score = score.sqrt();
        // The target global write flow slides between Bandwidth / 2 and Bandwidth.
        let score = 0.5 + score / 2.0;
        (total_budgets as f32 * score) as usize
    }
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `TiKV`, rather than storing each component individually.
pub trait Stop {
    fn stop(self: Box<Self>);
}

impl<R> Stop for StatusServer<R>
where
    R: 'static + Send,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl Stop for Worker {
    fn stop(self: Box<Self>) {
        Worker::stop(&self);
    }
}

impl<T: fmt::Display + Send + 'static> Stop for LazyWorker<T> {
    fn stop(self: Box<Self>) {
        self.stop_worker();
    }
}

pub trait KvEngineBuilder: KvEngine {
    fn build(
        range_cache_engine_context: RangeCacheEngineContext,
        disk_engine: RocksEngine,
        pd_client: Option<Arc<RpcClient>>,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
    ) -> Self;
}

impl KvEngineBuilder for RocksEngine {
    fn build(
        _: RangeCacheEngineContext,
        disk_engine: RocksEngine,
        _pd_client: Option<Arc<RpcClient>>,
        _region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
    ) -> Self {
        disk_engine
    }
}

impl KvEngineBuilder for HybridEngine<RocksEngine, RangeCacheMemoryEngine> {
    fn build(
        range_cache_engine_context: RangeCacheEngineContext,
        disk_engine: RocksEngine,
        pd_client: Option<Arc<RpcClient>>,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
    ) -> Self {
        // todo(SpadeA): add config for it
        let mut memory_engine = RangeCacheMemoryEngine::with_region_info_provider(
            range_cache_engine_context,
            region_info_provider,
        );
        memory_engine.set_disk_engine(disk_engine.clone());
        if let Some(pd_client) = pd_client.as_ref() {
            memory_engine.start_hint_service(
                <RangeCacheMemoryEngine as RangeCacheEngine>::RangeHintService::from(
                    pd_client.clone(),
                ),
            )
        }
        HybridEngine::new(disk_engine, memory_engine)
    }
}

pub trait ConfiguredRaftEngine: RaftEngine {
    fn build(
        _: &TikvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>);
    fn as_rocks_engine(&self) -> Option<&RocksEngine>;
    fn register_config(&self, _cfg_controller: &mut ConfigController);
}

impl<T: RaftEngine> ConfiguredRaftEngine for T {
    default fn build(
        _: &TikvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        unimplemented!()
    }
    default fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        None
    }
    default fn register_config(&self, _cfg_controller: &mut ConfigController) {}
}

impl ConfiguredRaftEngine for RocksEngine {
    fn build(
        config: &TikvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_engine.config().dir,
            &config.raft_store.raftdb_path,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_db_path = &config.raft_store.raftdb_path;
        let config_raftdb = &config.raftdb;
        let statistics = Arc::new(RocksStatistics::new_titan());
        let raft_db_opts = config_raftdb.build_opt(env.clone(), Some(&statistics));
        let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
        let raftdb = engine_rocks::util::new_engine_opt(raft_db_path, raft_db_opts, raft_cf_opts)
            .expect("failed to open raftdb");

        if should_dump {
            let raft_engine =
                RaftLogEngine::new(config.raft_engine.config(), key_manager.clone(), None)
                    .expect("failed to open raft engine for migration");
            dump_raft_engine_to_raftdb(&raft_engine, &raftdb, 8 /* threads */);
            raft_engine.stop();
            drop(raft_engine);
            raft_data_state_machine.after_dump_data();
        }
        (raftdb, Some(statistics))
    }

    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        Some(self)
    }

    fn register_config(&self, cfg_controller: &mut ConfigController) {
        cfg_controller.register(
            tikv::config::Module::Raftdb,
            Box::new(DbConfigManger::new(
                cfg_controller.get_current().rocksdb,
                self.clone(),
                DbType::Raft,
            )),
        );
    }
}

impl ConfiguredRaftEngine for RaftLogEngine {
    fn build(
        config: &TikvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_store.raftdb_path,
            &config.raft_engine.config().dir,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_config = config.raft_engine.config();
        let raft_engine =
            RaftLogEngine::new(raft_config, key_manager.clone(), get_io_rate_limiter())
                .expect("failed to open raft engine");

        if should_dump {
            let config_raftdb = &config.raftdb;
            let raft_db_opts = config_raftdb.build_opt(env.clone(), None);
            let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
            let raftdb = engine_rocks::util::new_engine_opt(
                &config.raft_store.raftdb_path,
                raft_db_opts,
                raft_cf_opts,
            )
            .expect("failed to open raftdb for migration");
            dump_raftdb_to_raft_engine(&raftdb, &raft_engine, 8 /* threads */);
            raftdb.stop();
            drop(raftdb);
            raft_data_state_machine.after_dump_data();
        }
        (raft_engine, None)
    }
}

const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);
pub struct EngineMetricsManager<EK: KvEngine, ER: RaftEngine> {
    tablet_registry: TabletRegistry<EK>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    range_cache_engine_statistics: Option<Arc<RangeCacheMemoryEngineStatistics>>,
    kv_is_titan: bool,
    raft_engine: ER,
    raft_statistics: Option<Arc<RocksStatistics>>,
    last_reset: Instant,
}

impl<EK: KvEngine, ER: RaftEngine> EngineMetricsManager<EK, ER> {
    pub fn new(
        tablet_registry: TabletRegistry<EK>,
        kv_statistics: Option<Arc<RocksStatistics>>,
        range_cache_engine_statistics: Option<Arc<RangeCacheMemoryEngineStatistics>>,
        kv_is_titan: bool,
        raft_engine: ER,
        raft_statistics: Option<Arc<RocksStatistics>>,
    ) -> Self {
        EngineMetricsManager {
            tablet_registry,
            kv_statistics,
            range_cache_engine_statistics,
            kv_is_titan,
            raft_engine,
            raft_statistics,
            last_reset: Instant::now(),
        }
    }

    pub fn flush(&mut self, now: Instant) {
        let mut reporter = EK::StatisticsReporter::new("kv");
        self.tablet_registry
            .for_each_opened_tablet(|_, db: &mut CachedTablet<EK>| {
                if let Some(db) = db.latest() {
                    reporter.collect(db);
                }
                true
            });
        reporter.flush();
        self.raft_engine.flush_metrics("raft");

        if let Some(s) = self.kv_statistics.as_ref() {
            flush_engine_statistics(s, "kv", self.kv_is_titan);
        }
        if let Some(s) = self.raft_statistics.as_ref() {
            flush_engine_statistics(s, "raft", false);
        }
        if let Some(s) = self.range_cache_engine_statistics.as_ref() {
            flush_range_cache_engine_statistics(s);
        }
        if now.saturating_duration_since(self.last_reset) >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            if let Some(s) = self.kv_statistics.as_ref() {
                s.reset();
            }
            if let Some(s) = self.raft_statistics.as_ref() {
                s.reset();
            }
            self.last_reset = now;
        }
    }
}
