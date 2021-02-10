// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: usize,
    pub reschedule_duration: ReadableDuration,
    pub pool_size: usize,
    pub io_pool_size: usize,
    pub io_max_wait_us: u64,
    pub io_queue_size: usize,
    pub io_queue_init_bytes: usize,
    pub io_queue_bytes_step: f64,
    pub io_queue_sample_quantile: f64,
    pub io_queue_adaptive_gain: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: 256,
            reschedule_duration: ReadableDuration::secs(5),
            pool_size: 2,
            io_pool_size: 1,
            io_max_wait_us: 500,
            io_queue_size: 64,
            io_queue_init_bytes: 256 * 1024,
            io_queue_bytes_step: 1.414213562373095,
            io_queue_adaptive_gain: 1,
            io_queue_sample_quantile: 0.99,
        }
    }
}
