// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use pd_client::{Config, RpcClient};
use security::{SecurityConfig, SecurityManager};
use tikv_util::config::ReadableDuration;

pub fn new_config(eps: Vec<(String, u16)>) -> Config {
    Config {
        endpoints: eps
            .into_iter()
            .map(|addr| format!("{}:{}", addr.0, addr.1))
            .collect(),
        ..Default::default()
    }
}

pub fn new_client(eps: Vec<(String, u16)>, mgr: Option<Arc<SecurityManager>>) -> RpcClient {
    let cfg = new_config(eps);
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&cfg, None, mgr).unwrap()
}

pub fn new_client_with_update_interval(
    eps: Vec<(String, u16)>,
    mgr: Option<Arc<SecurityManager>>,
    interval: ReadableDuration,
) -> RpcClient {
    let mut cfg = new_config(eps);
    cfg.update_interval = interval;
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&cfg, None, mgr).unwrap()
}
