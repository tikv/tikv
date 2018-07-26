// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_export]
macro_rules! fatal {
    ($lvl:expr, $($arg:tt)+) => ({
        if LOG_INITIALIZED.load(Ordering::SeqCst) {
            error!($lvl, $($arg)+);
        } else {
            eprintln!($lvl, $($arg)+);
        }
        process::exit(1)
    })
}

#[cfg(unix)]
pub mod profiling;
#[macro_use]
pub mod setup;
pub mod signal_handler;

pub fn build_info() -> String {
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        env!("TIKV_BUILD_COMMIT"),
        env!("TIKV_BUILD_BRANCH"),
        env!("TIKV_BUILD_TIME"),
        env!("TIKV_BUILD_RUSTC")
    )
}

pub fn print_tikv_info() {
    info!("Welcome to TiKV.\n{}", build_info());
}
