// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::StorageReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter};
use crate::storage::metrics;
use std::sync::{Arc, Mutex};
use tikv_util::future_pool::{Builder, Config, FuturePool};

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &StorageReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_future_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let reporter2 = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .name_prefix(name)
                .on_tick(move || metrics::tls_flush(&reporter))
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                .before_stop(move || {
                    // Safety: we call `set_` and `destroy_` with the same engine type.
                    unsafe {
                        destroy_tls_engine::<E>();
                    }
                    metrics::tls_flush(&reporter2)
                })
                .build()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(
    config: &StorageReadPoolConfig,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_future_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .name_prefix(name)
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                // Safety: we call `set_` and `destroy_` with the same engine type.
                .before_stop(|| unsafe { destroy_tls_engine::<E>() })
                .build()
        })
        .collect()
}
