// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

extern crate slog_global;

#[macro_use]
extern crate tikv_util;

#[macro_use]
pub mod config;
pub mod proxy;
pub mod run;
pub mod setup;
pub mod util;

pub use server::fatal;

fn proxy_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "Git Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}\
         \nStorage Engine:    {}\
         \nPrometheus Prefix: {}\
         \nProfile:           {}",
        option_env!("PROXY_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("PROXY_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("PROXY_BUILD_TIME").unwrap_or(fallback),
        option_env!("PROXY_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        option_env!("ENGINE_LABEL_VALUE").unwrap_or(fallback),
        option_env!("PROMETHEUS_METRIC_NAME_PREFIX").unwrap_or(fallback),
        option_env!("PROXY_PROFILE").unwrap_or(fallback),
    )
}

fn log_proxy_info() {
    info!("Welcome To RaftStore Proxy");
    for line in proxy_version_info().lines() {
        info!("{}", line);
    }
}

pub fn print_proxy_version() {
    println!("{}", proxy_version_info());
}

pub use proxy::run_proxy;
