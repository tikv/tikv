// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    error::Error,
    result::Result,
    sync::{Arc, Mutex, RwLock, Weak},
};

use online_config::{self, OnlineConfig};
use tikv_util::{HandyRwLock, config::ReadableDuration, resizable_threadpool::ResizableRuntime};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub num_threads: usize,
    #[online_config(skip)]
    pub stream_channel_window: usize,
    /// The timeout for going back into normal mode from import mode.
    ///
    /// Default is 10m.
    #[online_config(skip)]
    pub import_mode_timeout: ReadableDuration,
    /// the ratio of system memory used for import.
    pub memory_use_ratio: f64,
    /// Write ingested SSTs with direct I/O, and verify their checksums with
    /// direct I/O too, so the ingest does not populate the OS page cache and
    /// evict the working set that concurrent foreground reads depend on.
    ///
    /// Only applies to DDL-sourced ingest (an `ImportSST.Write` whose
    /// `request_source` task name is `ddl`); all other ingest, and the
    /// download/restore path, stay buffered.
    ///
    /// Requires a data directory on a filesystem that supports direct I/O
    /// (`O_DIRECT` on Linux). Enabling it elsewhere will fail SST writes, so it
    /// is off by default.
    ///
    /// Read at `SstImporter` construction, so changing it needs a restart.
    #[online_config(skip)]
    pub use_direct_io_for_ingest: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            num_threads: 8,
            stream_channel_window: 128,
            import_mode_timeout: ReadableDuration::minutes(10),
            memory_use_ratio: 0.3,
            use_direct_io_for_ingest: false,
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        let default_cfg = Config::default();
        if self.num_threads == 0 {
            warn!(
                "import.num_threads can not be 0, change it to {}",
                default_cfg.num_threads
            );
            self.num_threads = default_cfg.num_threads;
        }
        if self.stream_channel_window == 0 {
            warn!(
                "import.stream_channel_window can not be 0, change it to {}",
                default_cfg.stream_channel_window
            );
            self.stream_channel_window = default_cfg.stream_channel_window;
        }
        if self.memory_use_ratio > 0.5 || self.memory_use_ratio < 0.0 {
            return Err("import.mem_ratio should belong to [0.0, 0.5].".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConfigManager {
    pub config: Arc<RwLock<Config>>,
    pool: Weak<Mutex<ResizableRuntime>>,
}

impl ConfigManager {
    pub fn new(cfg: Config, pool: Weak<Mutex<ResizableRuntime>>) -> Self {
        ConfigManager {
            config: Arc::new(RwLock::new(cfg)),
            pool,
        }
    }
}

impl online_config::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: online_config::ConfigChange) -> online_config::Result<()> {
        info!(
            "import config changed";
            "change" => ?change,
        );

        let mut cfg = self.rl().clone();
        cfg.update(change)?;

        if let Err(e) = cfg.validate() {
            warn!(
                "import config changed";
                "change" => ?cfg,
            );
            return Err(e);
        }

        if let Some(pool) = self.pool.upgrade() {
            let mut pool = pool.lock().unwrap();
            pool.adjust_with(cfg.num_threads);
        }

        *self.wl() = cfg;
        Ok(())
    }
}

impl std::ops::Deref for ConfigManager {
    type Target = RwLock<Config>;

    fn deref(&self) -> &Self::Target {
        self.config.as_ref()
    }
}
