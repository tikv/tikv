// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use configuration::{ConfigChange, Configuration};
use std::sync::Arc;
use tikv_util::config::{ReadableSize, VersionTrack};

use crate::config::ConfigManager;

const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct GcConfig {
    pub ratio_threshold: f64,
    pub batch_keys: usize,
    pub max_write_bytes_per_sec: ReadableSize,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
        }
    }
}

impl GcConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.batch_keys == 0 {
            return Err(("gc.batch_keys should not be 0.").into());
        }
        Ok(())
    }
}

pub type GcWorkerConfigManager = Arc<VersionTrack<GcConfig>>;

impl ConfigManager for GcWorkerConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.update(move |cfg: &mut GcConfig| cfg.update(change));
        }
        info!(
            "GC worker config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::gc_worker::{GcTask, GcWorker};
    use super::*;
    use crate::config::{ConfigController, TiKvConfig};
    use crate::storage::kv::TestEngineBuilder;
    use std::f64::INFINITY;
    use std::sync::mpsc::channel;
    use std::time::Duration;
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
    ) -> (GcWorker<crate::storage::kv::RocksEngine>, ConfigController) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut gc_worker = GcWorker::new(engine, None, None, None, cfg.gc.clone());
        gc_worker.start().unwrap();

        let mut cfg_controller = ConfigController::new(cfg, Default::default());
        cfg_controller.register("gc", Box::new(gc_worker.get_config_manager()));

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
        config_manager
            .update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(1024));
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), 1024.0);
        });

        // Change io iolimit
        config_manager
            .update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(2048));
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), 2048.0);
        });

        // Disable io iolimit
        config_manager.update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(0));
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), INFINITY);
        });
    }
}
