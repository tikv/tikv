// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage online config manager.

use std::{convert::TryInto, sync::Arc};

use engine_traits::{ALL_CFS, CF_DEFAULT};
use file_system::{get_io_rate_limiter, IoPriority, IoType};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use strum::IntoEnumIterator;
use tikv_kv::Engine;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    worker::Scheduler,
};

use crate::{
    config::ConfigurableDb,
    server::{ttl::TtlCheckerTask, CONFIG_ROCKSDB_GAUGE},
    storage::{lock_manager::LockManager, txn::flow_controller::FlowController, TxnScheduler},
};

pub struct StorageConfigManger<E: Engine, K, L: LockManager> {
    configurable_db: K,
    ttl_checker_scheduler: Scheduler<TtlCheckerTask>,
    flow_controller: Arc<FlowController>,
    scheduler: TxnScheduler<E, L>,
}

unsafe impl<E: Engine, K, L: LockManager> Send for StorageConfigManger<E, K, L> {}
unsafe impl<E: Engine, K, L: LockManager> Sync for StorageConfigManger<E, K, L> {}

impl<E: Engine, K, L: LockManager> StorageConfigManger<E, K, L> {
    pub fn new(
        configurable_db: K,
        ttl_checker_scheduler: Scheduler<TtlCheckerTask>,
        flow_controller: Arc<FlowController>,
        scheduler: TxnScheduler<E, L>,
    ) -> Self {
        StorageConfigManger {
            configurable_db,
            ttl_checker_scheduler,
            flow_controller,
            scheduler,
        }
    }
}

impl<EK: Engine, K: ConfigurableDb, L: LockManager> ConfigManager
    for StorageConfigManger<EK, K, L>
{
    fn dispatch(&mut self, mut change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(mut block_cache)) = change.remove("block_cache") {
            if let Some(size) = block_cache.remove("capacity") {
                if size != ConfigValue::None {
                    let s: ReadableSize = size.into();
                    self.configurable_db
                        .set_shared_block_cache_capacity(s.0 as usize)?;
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
        } else if let Some(ConfigValue::Module(flow_control)) = change.remove("flow_control") {
            if let Some(v) = flow_control.get("enable") {
                let enable: bool = v.into();
                let enable_str = if enable { "true" } else { "false" };
                for cf in ALL_CFS {
                    self.configurable_db
                        .set_cf_config(cf, &[("disable_write_stall", enable_str)])
                        .unwrap();
                }
                self.flow_controller.enable(enable);
            }
            self.flow_controller.update_config(flow_control)?;
        } else if let Some(v) = change.get("scheduler_worker_pool_size") {
            let pool_size: usize = v.into();
            self.scheduler.scale_pool_size(pool_size);
        } else if let Some(v) = change.remove("memory_quota") {
            let cap: ReadableSize = v.into();
            self.scheduler.set_memory_quota_capacity(cap.0 as usize);
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

            for t in IoType::iter() {
                if let Some(priority) = io_rate_limit.remove(&(t.as_str().to_owned() + "_priority"))
                {
                    let priority: IoPriority = priority.try_into()?;
                    limiter.set_io_priority(t, priority);
                }
            }
        }
        Ok(())
    }
}
