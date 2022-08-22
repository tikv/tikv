// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Deref, DerefMut};

use tikv::config::TikvConfig;

#[derive(Clone)]
pub struct Config {
    pub tikv: TikvConfig,
    pub prefer_mem: bool,
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
