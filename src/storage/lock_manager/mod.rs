// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::ProcessResult;
use crate::storage::StorageCb;
use std::sync::atomic::{AtomicBool, Ordering};

mod util;

pub use self::util::{
    extract_lock_from_result, extract_raw_key_from_process_result, gen_key_hash, gen_key_hashes,
};

// If it is true, there is no need to calculate keys' hashes and wake up waiters.
pub static WAIT_TABLE_IS_EMPTY: AtomicBool = AtomicBool::new(true);

pub fn store_wait_table_is_empty(is_empty: bool) {
    WAIT_TABLE_IS_EMPTY.store(is_empty, Ordering::Relaxed);
}

pub fn wait_table_is_empty() -> bool {
    WAIT_TABLE_IS_EMPTY.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Lock {
    pub ts: u64,
    pub hash: u64,
}

pub trait Detector: Clone + Send + 'static {
    /// Removes the entries of the transaction.
    fn clean_up(&self, txn_ts: u64) {}
}

pub trait WaiterMgr: Clone + Send + 'static {
    fn wake_up(&self, lock_ts: u64, hashes: Vec<u64>, commit_ts: u64) {}
    fn wait_for(
        &self,
        start_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        is_first_lock: bool,
    ) {
    }
}

#[derive(Clone)]
pub struct DummyDetector {}

impl Detector for DummyDetector {}

#[derive(Clone)]
pub struct DummyWaiterMgr {}

impl WaiterMgr for DummyWaiterMgr {}
