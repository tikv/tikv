// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicBool, Arc},
};

use tikv::config::TiKvConfig;

use crate::ProxyConfig;

#[derive(Clone, Default)]
pub struct MockConfig {
    pub panic_when_flush_no_found: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct Config {
    pub tikv: TiKvConfig,
    pub prefer_mem: bool,
    pub proxy_cfg: ProxyConfig,
    /// Whether our mock server should compat new proxy.
    pub proxy_compat: bool,
    pub mock_cfg: MockConfig,
}

impl Deref for Config {
    type Target = TiKvConfig;
    #[inline]
    fn deref(&self) -> &TiKvConfig {
        &self.tikv
    }
}

impl DerefMut for Config {
    #[inline]
    fn deref_mut(&mut self) -> &mut TiKvConfig {
        &mut self.tikv
    }
}
