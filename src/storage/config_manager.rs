// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage online config manager.

use crate::server::ttl::TtlCheckerTask;
use crate::server::CONFIG_ROCKSDB_GAUGE;
use crate::storage::lock_manager::LockManager;
use crate::storage::txn::flow_controller::FlowController;
use crate::storage::TxnScheduler;
use engine_traits::{CFNamesExt, CFOptionsExt, ColumnFamilyOptions, CF_DEFAULT};
use file_system::{get_io_rate_limiter, IOPriority, IOType};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use std::sync::Arc;
use strum::IntoEnumIterator;
use tikv_kv::Engine;
use tikv_util::config::{ReadableDuration, ReadableSize};
use tikv_util::worker::Scheduler;

pub struct StorageConfigManger<E: Engine, L: LockManager> {
    kvdb: <E as Engine>::Local,
    shared_block_cache: bool,
    ttl_checker_scheduler: Scheduler<TtlCheckerTask>,
    flow_controller: Arc<FlowController>,
    scheduler: TxnScheduler<E, L>,
}

unsafe impl<E: Engine, L: LockManager> Send for StorageConfigManger<E, L> {}
unsafe impl<E: Engine, L: LockManager> Sync for StorageConfigManger<E, L> {}

impl<E: Engine, L: LockManager> StorageConfigManger<E, L> {
    pub fn new(
        kvdb: <E as Engine>::Local,
        shared_block_cache: bool,
        ttl_checker_scheduler: Scheduler<TtlCheckerTask>,
        flow_controller: Arc<FlowController>,
        scheduler: TxnScheduler<E, L>,
    ) -> Self {
        StorageConfigManger {
            kvdb,
            shared_block_cache,
            ttl_checker_scheduler,
            flow_controller,
            scheduler,
        }
    }
}

impl<EK: Engine, L: LockManager> ConfigManager for StorageConfigManger<EK, L> {
    fn dispatch(&mut self, mut change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(mut block_cache)) = change.remove("block_cache") {
            if !self.shared_block_cache {
                return Err("shared block cache is disabled".into());
            }
            if let Some(size) = block_cache.remove("capacity") {
                if size != ConfigValue::None {
                    let s: ReadableSize = size.into();
                    // Hack: since all CFs in both kvdb and raftdb share a block cache, we can change
                    // the size through any of them. Here we change it through default CF in kvdb.
                    // A better way to do it is to hold the cache reference somewhere, and use it to
                    // change cache size.
                    let opt = self.kvdb.get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
                    opt.set_block_cache_capacity(s.0)?;
                    // Write config to metric
                    CONFIG_ROCKSDB_GAUGE
                        .with_label_values(&[CF_DEFAULT, "block_cache_size"])
                        .set(s.0 as f64);
                }
            }
        } else if let Some(v) = change.remove("ttl_check_poll_interval") {
            let interval: ReadableDuration = v.into();
            self.ttl_checker_scheduler
                .schedule(TtlCheckerTask::UpdatePollInterval(interval.into()))
                .unwrap();
        } else if let Some(ConfigValue::Module(mut flow_control)) = change.remove("flow_control") {
            if let Some(v) = flow_control.remove("enable") {
                let enable: bool = v.into();
                if enable {
                    for cf in self.kvdb.cf_names() {
                        self.kvdb
                            .set_options_cf(cf, &[("disable_write_stall", "true")])
                            .unwrap();
                    }
                    self.flow_controller.enable(true);
                } else {
                    for cf in self.kvdb.cf_names() {
                        self.kvdb
                            .set_options_cf(cf, &[("disable_write_stall", "false")])
                            .unwrap();
                    }
                    self.flow_controller.enable(false);
                }
            }
        } else if let Some(v) = change.get("scheduler_worker_pool_size") {
            let pool_size: usize = v.into();
            self.scheduler.scale_pool_size(pool_size);
        }
        if let Some(ConfigValue::Module(mut io_rate_limit)) = change.remove("io_rate_limit") {
            let limiter = match get_io_rate_limiter() {
                None => return Err("IO rate limiter is not present".into()),
                Some(limiter) => limiter,
            };
            if let Some(limit) = io_rate_limit.remove("max_bytes_per_sec") {
                let limit: ReadableSize = limit.into();
                limiter.set_io_rate_limit(limit.0 as usize);
            }

            for t in IOType::iter() {
                if let Some(priority) = io_rate_limit.remove(&(t.as_str().to_owned() + "_priority"))
                {
                    let priority: IOPriority = priority.into();
                    limiter.set_io_priority(t, priority);
                }
            }
        }
        Ok(())
    }
}
