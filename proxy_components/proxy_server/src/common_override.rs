// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This mod overrides common in TiKV.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    u64,
};

use encryption_export::DataKeyManager;
use engine_rocks::{
    raw::{Cache, Env},
    RocksEngine, RocksStatistics,
};
use engine_store_ffi::{self, TiFlashEngine};
use engine_tiflash::PSLogEngine;
use engine_traits::{
    CfOptionsExt, Engines, FlowControlFactorsExt, RaftEngine, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use file_system::{get_io_rate_limiter, File, IoBudgetAdjustor};
use raft_log_engine::RaftLogEngine;
use server::{common::Stop, raft_engine_switch::*};
use tikv::config::{ConfigController, DbConfigManger, DbType, TikvConfig};
use tikv_util::{config::RaftDataStateMachine, math::MovingAvgU32, time::Instant};

use crate::status_server::StatusServer;

impl<E: 'static, R> Stop for StatusServer<E, R>
where
    R: 'static + Send,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

pub trait ConfiguredRaftEngine: RaftEngine {
    fn build(
        _: &TikvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>);
    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        None
    }

    fn register_config(&self, _cfg_controller: &mut ConfigController) {}

    fn as_ps_engine(&mut self) -> Option<&mut PSLogEngine> {
        None
    }
}

impl ConfiguredRaftEngine for engine_rocks::RocksEngine {
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

impl ConfiguredRaftEngine for PSLogEngine {
    fn build(
        _config: &TikvConfig,
        _env: &Arc<Env>,
        _key_manager: &Option<Arc<DataKeyManager>>,
        _block_cache: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        // Create a dummy file in raft engine dir to pass initial config check
        // See raftengine_exists.
        let raft_engine_path = _config.raft_engine.config().dir + "/ps_engine.raftlog";
        let path = Path::new(&raft_engine_path);
        if !path.exists() {
            File::create(path).unwrap();
        }
        (PSLogEngine::new(), None)
    }

    fn as_ps_engine(&mut self) -> Option<&mut PSLogEngine> {
        Some(self)
    }
}

pub struct EnginesResourceInfo {
    kv_engine: TiFlashEngine,
    raft_engine: Option<RocksEngine>,
    latest_normalized_pending_bytes: AtomicU32,
    normalized_pending_bytes_collector: MovingAvgU32,
}

impl EnginesResourceInfo {
    const SCALE_FACTOR: u64 = 100;

    pub fn new<CER: ConfiguredRaftEngine>(
        engines: &Engines<TiFlashEngine, CER>,
        max_samples_to_preserve: usize,
    ) -> Self {
        let raft_engine = engines.raft.as_rocks_engine().cloned();
        EnginesResourceInfo {
            kv_engine: engines.kv.clone(),
            raft_engine,
            latest_normalized_pending_bytes: AtomicU32::new(0),
            normalized_pending_bytes_collector: MovingAvgU32::new(max_samples_to_preserve),
        }
    }

    pub fn update(&self, _now: Instant) {
        let mut normalized_pending_bytes = 0;

        fn fetch_engine_cf(engine: &RocksEngine, cf: &str, normalized_pending_bytes: &mut u32) {
            if let Ok(cf_opts) = engine.get_options_cf(cf) {
                if let Ok(Some(b)) = engine.get_cf_pending_compaction_bytes(cf) {
                    if cf_opts.get_soft_pending_compaction_bytes_limit() > 0 {
                        *normalized_pending_bytes = std::cmp::max(
                            *normalized_pending_bytes,
                            (b * EnginesResourceInfo::SCALE_FACTOR
                                / cf_opts.get_soft_pending_compaction_bytes_limit())
                                as u32,
                        );
                    }
                }
            }
        }

        if let Some(raft_engine) = &self.raft_engine {
            fetch_engine_cf(raft_engine, CF_DEFAULT, &mut normalized_pending_bytes);
        }
        for cf in &[CF_DEFAULT, CF_WRITE, CF_LOCK] {
            fetch_engine_cf(&self.kv_engine.rocks, cf, &mut normalized_pending_bytes);
        }
        let (_, avg) = self
            .normalized_pending_bytes_collector
            .add(normalized_pending_bytes);
        self.latest_normalized_pending_bytes.store(
            std::cmp::max(normalized_pending_bytes, avg),
            Ordering::Relaxed,
        );
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
