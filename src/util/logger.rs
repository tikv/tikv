// Copyright 2016 PingCAP, Inc.
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

use log::LogLevelFilter;
pub fn get_level_by_string(lv: &str) -> LogLevelFilter {
    #![allow(match_same_arms)]
    match &*lv.to_owned().to_lowercase() {
        "trace" => LogLevelFilter::Trace,
        "debug" => LogLevelFilter::Debug,
        "info" => LogLevelFilter::Info,
        "warn" => LogLevelFilter::Warn,
        "error" => LogLevelFilter::Error,
        "off" => LogLevelFilter::Off,
        _ => LogLevelFilter::Info,
    }
}
