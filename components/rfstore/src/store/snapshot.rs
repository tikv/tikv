// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::Error as EngineError;
use engine_traits::{CfName, IterOptions, Peekable, ReadOptions, Result as EngineResult};
use kvengine::SnapAccess;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::Region;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tikv_util::error;
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};
use txn_types::{Key, Value};

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
#[derive(Debug)]
pub struct RegionSnapshot {
    pub snap: Arc<SnapAccess>,
    // `None` means the snapshot does not care about max_ts
    pub max_ts_sync_status: Option<Arc<AtomicU64>>,
    pub term: Option<NonZeroU64>,
    pub txn_extra_op: TxnExtraOp,
}

impl RegionSnapshot {
    pub fn from_raw(db: &kvengine::Engine, region: &Region) -> RegionSnapshot {
        let snap = db.get_snap_access(region.get_id()).unwrap();
        RegionSnapshot::from_snapshot(snap)
    }

    pub fn from_snapshot(snap: Arc<SnapAccess>) -> RegionSnapshot {
        RegionSnapshot {
            snap,
            max_ts_sync_status: None,
            term: None,
            txn_extra_op: TxnExtraOp::Noop,
        }
    }

    #[inline]
    pub fn get_start_key(&self) -> &[u8] {
        self.snap.get_start_key()
    }

    #[inline]
    pub fn get_end_key(&self) -> &[u8] {
        self.snap.get_end_key()
    }
}

impl Clone for RegionSnapshot {
    fn clone(&self) -> Self {
        RegionSnapshot {
            snap: self.snap.clone(),
            max_ts_sync_status: self.max_ts_sync_status.clone(),
            term: self.term,
            txn_extra_op: self.txn_extra_op.clone(),
        }
    }
}

impl RegionSnapshot {
    #[inline(never)]
    fn handle_get_value_error(&self, e: EngineError, cf: &str, key: &[u8]) -> EngineError {
        CRITICAL_ERROR.with_label_values(&["rocksdb get"]).inc();
        if panic_when_unexpected_key_or_data() {
            set_panic_mark();
            panic!(
                "failed to get value of key {} in region {}: {:?}",
                log_wrappers::Value::key(&key),
                self.snap.get_id(),
                e,
            );
        } else {
            error!(
                "failed to get value of key in cf";
                "key" => log_wrappers::Value::key(&key),
                "region" => self.snap.get_id(),
                "cf" => cf,
                "error" => ?e,
            );
            e
        }
    }
}

/// `SnapshotIterator` is dummy implementation for the tikv_kv::Iterator.
pub struct RegionSnapshotIterator {}

#[inline(never)]
fn handle_check_key_in_region_error(e: crate::Error) -> crate::Result<()> {
    // Split out the error case to reduce hot-path code size.
    CRITICAL_ERROR
        .with_label_values(&["key not in region"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!("key exceed bound: {:?}", e);
    } else {
        Err(e)
    }
}
