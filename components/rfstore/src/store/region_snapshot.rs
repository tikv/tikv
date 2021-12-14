// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    IterOptions, KvEngine, Peekable, ReadOptions, Result as EngineResult, Snapshot,
};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftApplyState;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::store::{util, PeerStorage};
use crate::{Error, Result};
use engine_traits::util::check_key_in_range;
use engine_traits::RaftEngine;
use engine_traits::CF_RAFT;
use engine_traits::{Error as EngineError, Iterable, Iterator};
use fail::fail_point;
use kvengine::SnapAccess;
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{box_err, error};
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};

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
    pub fn get_snapshot(&self) -> &Arc<SnapAccess> {
        &self.snap
    }

    #[inline]
    pub fn get_apply_index(&self) -> u64 {
        self.snap.get_write_sequence()
    }

    pub fn iter(&self, iter_opt: IterOptions, reverse: bool) -> RegionIterator {
        RegionIterator::new(self.snap.clone(), iter_opt, reverse)
    }

    pub fn iter_cf(
        &self,
        cf: &str,
        iter_opt: IterOptions,
        reverse: bool,
    ) -> Result<RegionIterator> {
        Ok(RegionIterator::new_cf(
            self.snap.clone(),
            iter_opt,
            cf,
            reverse,
        ))
    }

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, 0, 0);
        let end = KeyBuilder::from_slice(end_key, 0, 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter(iter_opt, false), start_key, f)
    }

    // like `scan`, only on a specific column family.
    pub fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, 0, 0);
        let end = KeyBuilder::from_slice(end_key, 0, 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter_cf(cf, iter_opt, false)?, start_key, f)
    }

    fn scan_impl<F>(&self, mut it: RegionIterator, start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut it_valid = it.seek(start_key);
        while it_valid {
            let item = it.item();
            it_valid = f(it.key(), item.get_value())? && it.next();
        }
        Ok(())
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

impl Peekable for RegionSnapshot {
    type DBVector = kvengine::engine_trait::EngineDBVector;

    fn get_value_opt(
        &self,
        opts: &ReadOptions,
        key: &[u8],
    ) -> EngineResult<Option<Self::DBVector>> {
        panic!("not supported")
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> EngineResult<Option<Self::DBVector>> {
        panic!("not supported")
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

/// `RegionIterator` wrap a rocksdb iterator and only allow it to
/// iterate in the region. It behaves as if underlying
/// db only contains one region.
pub struct RegionIterator {
    iter: kvengine::read::Iterator,
}

// we use engine::rocks's style iterator, doesn't need to impl std iterator.
impl RegionIterator {
    pub fn new(snap: Arc<SnapAccess>, mut iter_opt: IterOptions, reverse: bool) -> RegionIterator {
        Self::new_cf(snap, iter_opt, "write", reverse)
    }

    pub fn new_cf(
        snap: Arc<SnapAccess>,
        mut iter_opt: IterOptions,
        cf: &str,
        reverse: bool,
    ) -> RegionIterator {
        let cf_num = match cf {
            "write" => 0,
            "lock" => 1,
            "extra" => 2,
            _ => 0,
        };
        RegionIterator {
            iter: snap.new_iterator(cf_num, reverse, false),
        }
    }

    pub fn rewind(&mut self) -> bool {
        self.iter.rewind();
        self.iter.valid()
    }

    pub fn seek(&mut self, key: &[u8]) -> bool {
        self.iter.seek(key);
        self.iter.valid()
    }

    pub fn next(&mut self) -> bool {
        self.iter.next();
        self.iter.valid()
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        self.iter.key()
    }

    #[inline]
    pub fn item(&self) -> kvengine::Item {
        self.iter.item()
    }

    pub fn is_reverse(&self) -> bool {
        self.iter.is_reverse()
    }
}

#[inline(never)]
fn handle_check_key_in_region_error(e: crate::Error) -> Result<()> {
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
