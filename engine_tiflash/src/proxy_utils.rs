use crate::util::get_cf_handle;

pub fn do_write(cf: &str, key: &[u8]) -> bool {
    fail::fail_point!("before_tiflash_do_write", |_| true);
    match cf {
        engine_traits::CF_RAFT => true,
        engine_traits::CF_DEFAULT => {
            key == keys::PREPARE_BOOTSTRAP_KEY || key == keys::STORE_IDENT_KEY
        }
        _ => false,
    }
}

fn cf_to_name(batch: &crate::RocksWriteBatchVec, cf: u32) -> &'static str {
    // d 0 w 2 l 1
    let handle_default = get_cf_handle(batch.db.as_ref(), engine_traits::CF_DEFAULT).unwrap();
    let d = handle_default.id();
    let handle_write = get_cf_handle(batch.db.as_ref(), engine_traits::CF_WRITE).unwrap();
    let w = handle_write.id();
    let handle_lock = get_cf_handle(batch.db.as_ref(), engine_traits::CF_LOCK).unwrap();
    let l = handle_lock.id();
    if cf == l {
        engine_traits::CF_LOCK
    } else if cf == w {
        engine_traits::CF_WRITE
    } else if cf == d {
        engine_traits::CF_DEFAULT
    } else {
        engine_traits::CF_RAFT
    }
}
#[cfg(any(test, feature = "testexport"))]
fn check_double_write(batch: &crate::RocksWriteBatchVec) {
    // It will fire if we write by both observer(compat_old_proxy is not enabled)
    // and TiKV's WriteBatch.
    fail::fail_point!("before_tiflash_check_double_write", |_| {});
    tikv_util::debug!("check if double write happens");
    for wb in batch.wbs.iter() {
        for (_, cf, k, _) in wb.iter() {
            let handle = batch.db.cf_handle_by_id(cf as usize).unwrap();
            let cf_name = cf_to_name(&batch, handle.id());
            match cf_name {
                engine_traits::CF_DEFAULT | engine_traits::CF_LOCK | engine_traits::CF_WRITE => {
                    assert_eq!(crate::do_write(cf_name, k), true);
                }
                _ => (),
            };
        }
    }
}
#[cfg(not(any(test, feature = "testexport")))]
fn check_double_write(batch: &crate::RocksWriteBatchVec) {}

pub fn log_check_double_write(batch: &crate::RocksWriteBatchVec) -> bool {
    check_double_write(batch);
    // TODO(tiflash) re-support this tracker.
    let mut e = true;
    for wb in batch.wbs.iter() {
        if !wb.is_empty() {
            e = false;
            break;
        }
    }
    if e {
        let bt = std::backtrace::Backtrace::capture();
        tikv_util::info!("abnormal empty write batch";
            "backtrace" => ?bt
        );
        // We don't return true here, since new version TiKV will not cause
        // deadlock here.
    }
    false
}

use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStoreConfig {
    pub enable_fast_add_peer: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for EngineStoreConfig {
    fn default() -> Self {
        Self {
            enable_fast_add_peer: false,
        }
    }
}

#[derive(Default, Debug)]
pub struct ProxyConfigSet {
    pub engine_store: EngineStoreConfig,
}
