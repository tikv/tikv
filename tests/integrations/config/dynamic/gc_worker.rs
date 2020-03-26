// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::sync::mpsc::channel;
use std::time::Duration;
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv::server::gc_worker::GcConfig;
use tikv::server::gc_worker::{GcTask, GcWorker};
use tikv::storage::kv::TestEngineBuilder;
use tikv_util::config::ReadableSize;
use tikv_util::time::Limiter;
use tikv_util::worker::FutureScheduler;

#[test]
fn test_gc_config_validate() {
    let cfg = GcConfig::default();
    cfg.validate().unwrap();

    let mut invalid_cfg = GcConfig::default();
    invalid_cfg.batch_keys = 0;
    assert!(invalid_cfg.validate().is_err());
}

fn setup_cfg_controller(
    cfg: TiKvConfig,
) -> (GcWorker<tikv::storage::kv::RocksEngine>, ConfigController) {
    let engine = TestEngineBuilder::new().build().unwrap();
    let mut gc_worker = GcWorker::new(engine, None, None, None, cfg.gc.clone());
    gc_worker.start().unwrap();

    let mut cfg_controller = ConfigController::new(cfg, Default::default(), false);
    cfg_controller.register(Module::Gc, Box::new(gc_worker.get_config_manager()));

    (gc_worker, cfg_controller)
}

fn validate<F>(scheduler: &FutureScheduler<GcTask>, f: F)
where
    F: FnOnce(&GcConfig, &Limiter) + Send + 'static,
{
    let (tx, rx) = channel();
    scheduler
        .schedule(GcTask::Validate(Box::new(
            move |cfg: &GcConfig, limiter: &Limiter| {
                f(cfg, limiter);
                tx.send(()).unwrap();
            },
        )))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[allow(clippy::float_cmp)]
#[test]
fn test_gc_worker_config_update() {
    let mut cfg = TiKvConfig::default();
    cfg.validate().unwrap();
    let (gc_worker, mut cfg_controller) = setup_cfg_controller(cfg.clone());
    let scheduler = gc_worker.scheduler();

    // update of other module's config should not effect gc worker config
    let mut incoming = cfg.clone();
    incoming.raft_store.raft_log_gc_threshold = 2000;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert!(rollback.right().unwrap());
    validate(&scheduler, move |cfg: &GcConfig, _| {
        assert_eq!(cfg, &GcConfig::default());
    });

    // Update gc worker config
    let mut incoming = cfg;
    incoming.gc.ratio_threshold = 1.23;
    incoming.gc.batch_keys = 1234;
    incoming.gc.max_write_bytes_per_sec = ReadableSize(1024);
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert!(rollback.right().unwrap());
    validate(&scheduler, move |cfg: &GcConfig, _| {
        assert_eq!(cfg.ratio_threshold, 1.23);
        assert_eq!(cfg.batch_keys, 1234);
        assert_eq!(cfg.max_write_bytes_per_sec, ReadableSize(1024));
    });
}

#[test]
#[allow(clippy::float_cmp)]
fn test_change_io_limit_by_config_manager() {
    let mut cfg = TiKvConfig::default();
    cfg.validate().unwrap();
    let (gc_worker, mut cfg_controller) = setup_cfg_controller(cfg.clone());
    let scheduler = gc_worker.scheduler();

    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });

    // Enable io iolimit
    let mut incoming = cfg.clone();
    incoming.gc.max_write_bytes_per_sec = ReadableSize(1024);
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert!(rollback.right().unwrap());
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 1024.0);
    });

    // Change io iolimit
    let mut incoming = cfg.clone();
    incoming.gc.max_write_bytes_per_sec = ReadableSize(2048);
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert!(rollback.right().unwrap());
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 2048.0);
    });

    // Disable io iolimit
    let mut incoming = cfg;
    incoming.gc.max_write_bytes_per_sec = ReadableSize(0);
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert!(rollback.right().unwrap());
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });
}

#[test]
#[allow(clippy::float_cmp)]
fn test_change_io_limit_by_debugger() {
    // Debugger use GcWorkerConfigManager to change io limit
    let mut cfg = TiKvConfig::default();
    cfg.validate().unwrap();
    let (gc_worker, _) = setup_cfg_controller(cfg);
    let scheduler = gc_worker.scheduler();
    let config_manager = gc_worker.get_config_manager();

    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });

    // Enable io iolimit
    config_manager.update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(1024));
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 1024.0);
    });

    // Change io iolimit
    config_manager.update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(2048));
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), 2048.0);
    });

    // Disable io iolimit
    config_manager.update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(0));
    validate(&scheduler, move |_, limiter: &Limiter| {
        assert_eq!(limiter.speed_limit(), INFINITY);
    });
}
