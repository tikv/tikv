// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Deref, DerefMut};

use tikv::config::TiKvConfig;

use crate::ProxyConfig;

#[derive(Clone)]
pub struct Config {
    pub tikv: TiKvConfig,
    pub prefer_mem: bool,
    pub proxy_cfg: ProxyConfig,
    /// Whether our mock server should compat new proxy.
    pub proxy_compat: bool,
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
