// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStoreConfig {
    pub enable_fast_add_peer: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for EngineStoreConfig {
    fn default() -> Self {
        Self {
            enable_fast_add_peer: false,
        }
    }
}

#[derive(Default, Debug)]
pub struct ProxyConfigSet {
    pub engine_store: EngineStoreConfig,
}
