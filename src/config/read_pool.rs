// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableSize;

macro_rules! readpool_config {
    ($struct_name:ident, $test_mod_name:ident, $display_name:expr) => {
        #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
            pub high_concurrency: usize,
            pub normal_concurrency: usize,
            pub low_concurrency: usize,
            pub max_tasks_per_worker_high: usize,
            pub max_tasks_per_worker_normal: usize,
            pub max_tasks_per_worker_low: usize,
            pub stack_size: ReadableSize,
        }

        impl $struct_name {
            /// Builds configurations for low, normal and high priority pools.
            pub fn to_future_pool_configs(&self) -> Vec<future_pool::Config> {
                vec![
                    future_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    future_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    future_pool::Config {
                        workers: self.high_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_high,
                        stack_size: self.stack_size.0 as usize,
                    },
                ]
            }

            pub fn default_for_test() -> Self {
                Self {
                    high_concurrency: 2,
                    normal_concurrency: 2,
                    low_concurrency: 2,
                    max_tasks_per_worker_high: 2000,
                    max_tasks_per_worker_normal: 2000,
                    max_tasks_per_worker_low: 2000,
                    stack_size: ReadableSize::mb(1),
                }
            }

            pub fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.high_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.high-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.normal_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.normal-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.low_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.low-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.stack_size.0 < ReadableSize::mb(2).0 {
                    return Err(
                        format!("readpool.{}.stack-size should be >= 2mb", $display_name).into(),
                    );
                }
                if self.max_tasks_per_worker_high <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-high should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_normal <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-normal should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_low <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-low should be > 1",
                        $display_name
                    )
                    .into());
                }

                Ok(())
            }
        }

        #[cfg(test)]
        mod $test_mod_name {
            use super::*;

            #[test]
            fn test_validate() {
                let cfg = $struct_name::default();
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.high_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.normal_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.low_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.stack_size = ReadableSize::mb(1);
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_high = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_high = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_high = 100;
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_normal = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 100;
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_low = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 100;
                assert!(cfg.validate().is_ok());
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY: usize = 4;
const DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
const DEFAULT_READPOOL_MAX_TASKS_PER_WORKER: usize = 2 as usize * 1000;

const DEFAULT_READPOOL_STACK_SIZE_MB: u64 = 10;

readpool_config!(StorageReadPoolConfig, storage_read_pool_test, "storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        let cpu_num = sys_info::cpu_num().unwrap();
        let mut concurrency = (f64::from(cpu_num) * 0.5) as usize;
        concurrency = cmp::max(DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY, concurrency);
        concurrency = cmp::min(DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY, concurrency);
        Self {
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

const DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY: usize = 2;

readpool_config!(
    CoprReadPoolConfig,
    coprocessor_read_pool_test,
    "coprocessor"
);

impl Default for CoprReadPoolConfig {
    fn default() -> Self {
        let cpu_num = sys_info::cpu_num().unwrap();
        let mut concurrency = (f64::from(cpu_num) * 0.8) as usize;
        concurrency = cmp::max(DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    pub storage: StorageReadPoolConfig,
    pub coprocessor: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.storage.validate()?;
        self.coprocessor.validate()?;
        Ok(())
    }
}
