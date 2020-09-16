// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

extern crate slog_global;

#[macro_use]
extern crate tikv_util;

#[macro_use]
pub mod setup;
pub mod server;

mod tiflash_raft_proxy;

fn proxy_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "Git Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}\
         \nProfile:           {}",
        option_env!("PROXY_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("PROXY_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("PROXY_BUILD_TIME").unwrap_or(fallback),
        option_env!("PROXY_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        option_env!("PROXY_PROFILE").unwrap_or(fallback),
    )
}

fn log_proxy_info() {
    info!("Welcome To TiFlash Raft Proxy");
    for line in proxy_version_info().lines() {
        info!("{}", line);
    }
}
