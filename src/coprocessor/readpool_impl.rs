// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use file_system::{set_io_type, IOType};
use tikv_util::yatp_pool::{Config, DefaultTicker, FuturePool, PoolTicker, YatpPoolBuilder};

use super::metrics::*;
use crate::{
    config::CoprReadPoolConfig,
    storage::{
        kv::{destroy_tls_engine, set_tls_engine},
        Engine, FlowStatsReporter,
    },
};

#[derive(Clone)]
struct FuturePoolTicker<R: FlowStatsReporter> {
    pub reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for FuturePoolTicker<R> {
    fn on_tick(&mut self) {
        tls_flush(&self.reporter);
    }
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &CoprReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["cop-low", "cop-normal", "cop-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(FuturePoolTicker { reporter })
                .config(config)
                .name_prefix(name)
                .after_start(move || {
                    set_tls_engine(engine.lock().unwrap().clone());
                    set_io_type(IOType::ForegroundRead);
                })
                .before_stop(move || unsafe {
                    // Safety: we call `set_` and `destroy_` with the same engine type.
                    destroy_tls_engine::<E>();
                })
                .build_future_pool()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(
    config: &CoprReadPoolConfig,
    engine: E,
) -> Vec<FuturePool> {
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .map(|config| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(DefaultTicker::default())
                .config(config)
                .after_start(move || {
                    set_tls_engine(engine.lock().unwrap().clone());
                    set_io_type(IOType::ForegroundRead);
                })
                // Safety: we call `set_` and `destroy_` with the same engine type.
                .before_stop(|| unsafe { destroy_tls_engine::<E>() })
                .build_future_pool()
        })
        .collect()
}
