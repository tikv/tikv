use std::sync::Arc;

use engine_store_ffi::TiFlashEngine;
use engine_tiflash::DB;
use engine_traits::{CfOptionsExt, DbOptions, DbOptionsExt, CF_DEFAULT};
use tikv::config::ConfigurableDb;

#[derive(Clone, Debug)]
pub struct ProxyRocksEngine {
    pub inner: TiFlashEngine,
}

impl ProxyRocksEngine {
    pub(crate) fn new(engine: TiFlashEngine) -> ProxyRocksEngine {
        ProxyRocksEngine { inner: engine }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.inner.as_inner()
    }
}

pub type ConfigRes = std::result::Result<(), Box<dyn std::error::Error>>;

impl DbOptionsExt for ProxyRocksEngine {
    type DbOptions = engine_tiflash::RocksDbOptions;

    fn get_db_options(&self) -> Self::DbOptions {
        self.inner.get_db_options()
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> engine_traits::Result<()> {
        self.inner.set_db_options(options)
    }
}

impl ConfigurableDb for ProxyRocksEngine {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> ConfigRes {
        self.inner.set_db_options(opts).map_err(Box::from)
    }

    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> ConfigRes {
        self.inner.set_options_cf(cf, opts).map_err(Box::from)
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> ConfigRes {
        let mut opt = self.inner.get_db_options();
        opt.set_rate_bytes_per_sec(rate_bytes_per_sec)
            .map_err(Box::from)
    }

    fn set_rate_limiter_auto_tuned(&self, auto_tuned: bool) -> ConfigRes {
        let mut opt = self.inner.get_db_options();
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
        let mut cf_option = self.inner.get_options_cf(cf)?;
        cf_option.set_flush_size(f).map_err(Box::from)
    }

    fn set_flush_oldest_first(&self, f: bool) -> ConfigRes {
        let mut opt = self.get_db_options();
        opt.set_flush_oldest_first(f).map_err(Box::from)
    }

    fn set_shared_block_cache_capacity(&self, capacity: usize) -> ConfigRes {
        let opt = self.inner.get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
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
