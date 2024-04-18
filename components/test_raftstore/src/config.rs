// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Deref, DerefMut};

use tikv::config::TikvConfig;

pub struct Config {
    // temp dir to store the persisted configuration.
    // We use a temp dir to ensure the original `common-test.toml` won't be
    // changed by online config.
    _cfg_dir: Option<tempfile::TempDir>,
    pub tikv: TikvConfig,
    pub prefer_mem: bool,
}

impl Config {
    pub fn new(mut tikv: TikvConfig, prefer_mem: bool) -> Self {
        let cfg_dir = test_util::temp_dir("test-cfg", prefer_mem);
        tikv.cfg_path = cfg_dir.path().join("tikv.toml").display().to_string();
        Self {
            _cfg_dir: Some(cfg_dir),
            tikv,
            prefer_mem,
        }
    }
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            _cfg_dir: None,
            tikv: self.tikv.clone(),
            prefer_mem: self.prefer_mem,
        }
    }
}

impl Deref for Config {
    type Target = TikvConfig;
    #[inline]
    fn deref(&self) -> &TikvConfig {
        &self.tikv
    }
}

impl DerefMut for Config {
    #[inline]
    fn deref_mut(&mut self) -> &mut TikvConfig {
        &mut self.tikv
    }
}
