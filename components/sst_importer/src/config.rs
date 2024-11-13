// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    error::Error,
    result::Result,
    sync::{Arc, Mutex, RwLock},
};

use online_config::{self, OnlineConfig};
use tikv_util::{config::ReadableDuration, resizable_threadpool::ResizableRuntime, HandyRwLock};

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
}

impl Default for Config {
    fn default() -> Config {
        Config {
            num_threads: 8,
            stream_channel_window: 128,
            import_mode_timeout: ReadableDuration::minutes(10),
            memory_use_ratio: 0.3,
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
    pool: Arc<Mutex<ResizableRuntime>>,
}

impl ConfigManager {
    pub fn new(cfg: Config, pool: Arc<Mutex<ResizableRuntime>>) -> Self {
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

        self.pool.lock().unwrap().adjust_with(cfg.num_threads);

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
