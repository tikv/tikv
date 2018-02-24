// Copyright 2017 PingCAP, Inc.
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

use sys_info;

use util::config::{ReadableDuration, ReadableSize};
use super::DEFAULT_REQUEST_MAX_HANDLE_SECS;
use super::Result;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
pub const DEFAULT_MAX_RUNNING_TASK_COUNT: usize = 2 as usize * 1000;

// Enpoints may occur very deep recursion,
// so enlarge their stack size to 10 MB.
const DEFAULT_ENDPOINT_STACK_SIZE_MB: u64 = 10;

// Number of rows in each chunk.
pub const DEFAULT_ENDPOINT_BATCH_ROW_LIMIT: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub end_point_concurrency: usize,
    pub end_point_max_tasks: usize,
    pub end_point_stack_size: ReadableSize,
    pub end_point_recursion_limit: u32,
    pub end_point_batch_row_limit: usize,
    pub end_point_request_max_handle_secs: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = sys_info::cpu_num().unwrap();
        let concurrency = if cpu_num > 8 {
            (f64::from(cpu_num) * 0.8) as usize
        } else {
            4
        };
        Config {
            end_point_concurrency: concurrency,
            end_point_max_tasks: DEFAULT_MAX_RUNNING_TASK_COUNT,
            end_point_stack_size: ReadableSize::mb(DEFAULT_ENDPOINT_STACK_SIZE_MB),
            end_point_recursion_limit: 1000,
            end_point_batch_row_limit: DEFAULT_ENDPOINT_BATCH_ROW_LIMIT,
            end_point_request_max_handle_secs: ReadableDuration::secs(
                DEFAULT_REQUEST_MAX_HANDLE_SECS,
            ),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.end_point_concurrency == 0 {
            return Err(box_err!(
                "coprocessor.end-point-concurrency should not be 0."
            ));
        }

        if self.end_point_max_tasks == 0 {
            return Err(box_err!("coprocessor.end-point-max-tasks should not be 0."));
        }

        // 2MB is the default stack size for threads in rust, but endpoints may occur
        // very deep recursion, 2MB considered too small.
        //
        // See more: https://doc.rust-lang.org/std/thread/struct.Builder.html#method.stack_size
        if self.end_point_stack_size.0 < ReadableSize::mb(2).0 {
            return Err(box_err!("coprocessor.end-point-stack-size is too small."));
        }

        if self.end_point_recursion_limit < 100 {
            return Err(box_err!(
                "coprocessor.end-point-recursion-limit is too small"
            ));
        }

        if self.end_point_request_max_handle_secs.as_secs() < DEFAULT_REQUEST_MAX_HANDLE_SECS {
            return Err(box_err!(
                "coprocessor.end-point-request-max-handle-secs is too small"
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        cfg.validate().unwrap();

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_concurrency = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_stack_size = ReadableSize::mb(1);
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_max_tasks = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_recursion_limit = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_request_max_handle_secs = ReadableDuration::secs(10);
        assert!(invalid_cfg.validate().is_err());
    }
}
