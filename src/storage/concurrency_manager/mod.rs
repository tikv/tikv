// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod lock_table;
mod memory_lock;

pub use self::memory_lock::{MemoryLock, TxnMutexGuard};

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use txn_types::TimeStamp;

type OrderedLockMap = Mutex<BTreeMap<Vec<u8>, Arc<MemoryLock>>>;
pub type LockTable = self::lock_table::LockTable<OrderedLockMap>;

#[derive(Clone)]
pub struct ConcurrencyManager {
    max_read_ts: Arc<AtomicU64>,
    lock_table: LockTable,
}

impl ConcurrencyManager {
    pub fn new(latest_ts: TimeStamp) -> Self {
        ConcurrencyManager {
            max_read_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            lock_table: LockTable::default(),
        }
    }

    pub async fn lock_key(&self, key: &[u8]) -> TxnMutexGuard<'_, OrderedLockMap> {
        self.lock_table.lock_key(key).await
    }

    pub fn read_check_key(&self, key: &[u8], ts: TimeStamp) -> Result<(), LockInfo> {
        self.max_read_ts
            .fetch_max(ts.into_inner(), Ordering::SeqCst);
        self.lock_table
            .check_key(key, |_lock_info| todo!("check SI constraints"))
    }

    pub fn read_check_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        ts: TimeStamp,
    ) -> Result<(), LockInfo> {
        self.max_read_ts
            .fetch_max(ts.into_inner(), Ordering::SeqCst);
        self.lock_table
            .check_range(start_key, end_key, |_lock_info| {
                todo!("check SI constraints")
            })
    }
}
