// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error, io::Write};

use engine_rocks::RocksEngine;
use engine_traits::{
    CachedTablet, CfOptions, CfOptionsExt, DbOptions, DbOptionsExt, TabletRegistry, CF_DEFAULT,
};

pub type ConfigRes = Result<(), Box<dyn Error>>;

pub trait ConfigurableDb {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> ConfigRes;
    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> ConfigRes;
    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> ConfigRes;
    fn set_rate_limiter_auto_tuned(&self, auto_tuned: bool) -> ConfigRes;
    fn set_flush_size(&self, f: usize) -> ConfigRes;
    fn set_cf_flush_size(&self, cf: &str, f: usize) -> ConfigRes;
    fn set_flush_oldest_first(&self, f: bool) -> ConfigRes;
    fn set_shared_block_cache_capacity(&self, capacity: usize) -> ConfigRes;
    fn set_high_priority_background_threads(&self, n: i32, allow_reduce: bool) -> ConfigRes;
}

impl ConfigurableDb for RocksEngine {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> ConfigRes {
        self.set_db_options(opts).map_err(Box::from)
    }

    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> ConfigRes {
        self.set_options_cf(cf, opts).map_err(Box::from)
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> ConfigRes {
        let mut opt = self.get_db_options();
        opt.set_rate_bytes_per_sec(rate_bytes_per_sec)
            .map_err(Box::from)
    }

    fn set_rate_limiter_auto_tuned(&self, auto_tuned: bool) -> ConfigRes {
        let mut opt = self.get_db_options();
        opt.set_rate_limiter_auto_tuned(auto_tuned)
            .map_err(Box::new)?;
        // double check the new state
        let new_auto_tuned = opt.get_rate_limiter_auto_tuned();
        if new_auto_tuned == Some(auto_tuned) {
            Ok(())
        } else {
            Err(engine_traits::Status::with_error(
                engine_traits::Code::IoError,
                "fail to set rate_limiter_auto_tuned",
            )
            .into())
        }
    }

    fn set_flush_size(&self, f: usize) -> ConfigRes {
        let mut opt = self.get_db_options();
        opt.set_flush_size(f).map_err(Box::from)
    }

    fn set_cf_flush_size(&self, cf: &str, f: usize) -> ConfigRes {
        let mut cf_option = self.get_options_cf(cf)?;
        cf_option.set_flush_size(f).map_err(Box::from)
    }

    fn set_flush_oldest_first(&self, f: bool) -> ConfigRes {
        let mut opt = self.get_db_options();
        opt.set_flush_oldest_first(f).map_err(Box::from)
    }

    fn set_shared_block_cache_capacity(&self, capacity: usize) -> ConfigRes {
        let opt = self.get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
        opt.set_block_cache_capacity(capacity as u64)
            .map_err(Box::from)
    }

    fn set_high_priority_background_threads(&self, n: i32, allow_reduce: bool) -> ConfigRes {
        assert!(n > 0);
        if let Some(env) = self.as_inner().as_ref().env() {
            let origin_threads = env.get_high_priority_background_threads();
            if n > origin_threads || allow_reduce {
                env.set_high_priority_background_threads(n);
            }
            Ok(())
        } else {
            Err(Box::from(
                "set high priority background threads failed as env is not set".to_string(),
            ))
        }
    }
}

pub fn loop_registry(
    registry: &TabletRegistry<RocksEngine>,
    mut f: impl FnMut(&mut CachedTablet<RocksEngine>) -> std::result::Result<bool, Box<dyn Error>>,
) -> ConfigRes {
    let mut error_count = 0;
    let mut res = Ok(());
    let mut error_samples: Vec<u8> = vec![];
    registry.for_each_opened_tablet(|id, cache| match f(cache) {
        Ok(b) => b,
        Err(e) => {
            error_count += 1;
            res = Err(e);
            if error_count <= 3 {
                writeln!(
                    error_samples,
                    "Tablet {} {:?} encountered error: {:?}.",
                    id,
                    cache.cache().map(|c| c.as_inner().path()),
                    res
                )
                .unwrap();
            }
            true
        }
    });
    if error_count > 0 {
        error!(
            "Total count {}. Sample errors: {}",
            error_count,
            std::str::from_utf8(&error_samples).unwrap()
        );
    }
    res
}

impl ConfigurableDb for TabletRegistry<RocksEngine> {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_db_config(opts)?;
            }
            Ok(true)
        })
    }

    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_cf_config(cf, opts)?;
            }
            Ok(true)
        })
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_rate_bytes_per_sec(rate_bytes_per_sec)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }

    fn set_rate_limiter_auto_tuned(&self, auto_tuned: bool) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_rate_limiter_auto_tuned(auto_tuned)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }

    fn set_flush_size(&self, f: usize) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_flush_size(f)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }

    fn set_cf_flush_size(&self, cf: &str, f: usize) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_cf_flush_size(cf, f)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }

    fn set_flush_oldest_first(&self, f: bool) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_flush_oldest_first(f)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }

    fn set_shared_block_cache_capacity(&self, capacity: usize) -> ConfigRes {
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_shared_block_cache_capacity(capacity)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }

    fn set_high_priority_background_threads(&self, n: i32, allow_reduce: bool) -> ConfigRes {
        assert!(n > 0);
        loop_registry(self, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_high_priority_background_threads(n, allow_reduce)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
    }
}
