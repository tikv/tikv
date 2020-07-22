// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{server::lock_manager::DetectorScheduler, storage::lock_manager::Lock};
use futures03::channel::oneshot;
use std::{future::Future, pin::Pin};
use txn_types::TimeStamp;

pub trait DeadlockDetector {
    fn detect(
        &self,
        txn_ts: TimeStamp,
        lock: Lock,
    ) -> Pin<Box<dyn Future<Output = Option<u64>> + Send>>;

    fn clean_up(&self, txn_ts: TimeStamp);
}

impl DeadlockDetector for DetectorScheduler {
    fn detect(
        &self,
        txn_ts: TimeStamp,
        lock: Lock,
    ) -> Pin<Box<dyn Future<Output = Option<u64>> + Send>> {
        let (tx, rx) = oneshot::channel();
        self.detect_callback(txn_ts, lock, move |deadlock_key_hash| {
            let _ = tx.send(deadlock_key_hash);
        });
        Box::pin(async move { rx.await.ok() })
    }

    fn clean_up(&self, txn_ts: TimeStamp) {
        DetectorScheduler::clean_up(self, txn_ts)
    }
}
