// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage online config manager.

use std::{convert::TryInto, sync::Arc};

use engine_traits::{KvEngine, TabletFactory, CF_DEFAULT};
use file_system::{get_io_rate_limiter, IoPriority, IoType};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use strum::IntoEnumIterator;
use tikv_kv::Engine;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    worker::Scheduler,
};

use crate::{
    server::{ttl::TtlCheckerTask, CONFIG_ROCKSDB_GAUGE},
    storage::{lock_manager::LockManager, txn::flow_controller::FlowController, TxnScheduler},
};

pub struct StorageConfigManger<E: Engine, K: KvEngine, L: LockManager> {
    tablet_factory: Arc<dyn TabletFactory<K> + Send + Sync>,
    shared_block_cache: bool,
    ttl_checker_scheduler: Scheduler<TtlCheckerTask>,
    flow_controller: Arc<FlowController>,
    scheduler: TxnScheduler<E, L>,
}

unsafe impl<E: Engine, K: KvEngine, L: LockManager> Send for StorageConfigManger<E, K, L> {}
unsafe impl<E: Engine, K: KvEngine, L: LockManager> Sync for StorageConfigManger<E, K, L> {}

impl<E: Engine, K: KvEngine, L: LockManager> StorageConfigManger<E, K, L> {
    pub fn new(
        tablet_factory: Arc<dyn TabletFactory<K> + Send + Sync>,
        shared_block_cache: bool,
        ttl_checker_scheduler: Scheduler<TtlCheckerTask>,
        flow_controller: Arc<FlowController>,
        scheduler: TxnScheduler<E, L>,
    ) -> Self {
        StorageConfigManger {
            tablet_factory,
            shared_block_cache,
            ttl_checker_scheduler,
            flow_controller,
            scheduler,
        }
    }
}

impl<EK: Engine, K: KvEngine, L: LockManager> ConfigManager for StorageConfigManger<EK, K, L> {
    fn dispatch(&mut self, mut change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(mut block_cache)) = change.remove("block_cache") {
            if !self.shared_block_cache {
                return Err("shared block cache is disabled".into());
            }
            if let Some(size) = block_cache.remove("capacity") {
                if size != ConfigValue::None {
                    let s: ReadableSize = size.into();
                    self.tablet_factory.set_shared_block_cache_capacity(s.0)?;
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
                let enable_str = if enable { "true" } else { "false" };
                self.tablet_factory.for_each_opened_tablet(
                    &mut |_region_id, _suffix, tablet: &K| {
                        for cf in tablet.cf_names() {
                            tablet
                                .set_options_cf(cf, &[("disable_write_stall", enable_str)])
                                .unwrap();
                        }
                    },
                );
                self.flow_controller.enable(enable);
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
