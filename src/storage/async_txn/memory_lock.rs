// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock_table::OrderedMap;

use event_listener::{Event, EventListener};
use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

const UNLOCKED: u64 = 0;
const LOCKED: u64 = 1;
const OBSOLETE: u64 = 2;

#[derive(Default)]
pub struct MemoryLock {
    pub key: Vec<u8>,
    pub mutex_state: AtomicU64,
    pub mutex_notifier: Notifier,
    pub lock_info: Mutex<Option<LockInfo>>,
    pub pessimistic_notifier: Notifier,
}

impl MemoryLock {
    pub async fn mutex_lock(self: Arc<Self>) -> Result<MemoryLockGuard, ObseleteLock> {
        loop {
            match self
                .mutex_state
                .compare_and_swap(UNLOCKED, LOCKED, Ordering::SeqCst)
            {
                UNLOCKED => {
                    return Ok(MemoryLockGuard(self.clone()));
                }
                OBSOLETE => {
                    return Err(ObseleteLock);
                }
                _ => {
                    self.mutex_notifier.listen().await;
                }
            }
        }
    }
}

pub struct MemoryLockGuard(pub Arc<MemoryLock>);

impl Drop for MemoryLockGuard {
    fn drop(&mut self) {
        if self.0.mutex_state.swap(UNLOCKED, Ordering::SeqCst) == LOCKED {
            self.0.mutex_notifier.notify(1);
        }
    }
}

#[derive(Debug)]
pub struct ObseleteLock;

pub struct TxnMutexGuard<M: OrderedMap> {
    map: Arc<M>,
    lock: MemoryLockGuard,
}

impl<M: OrderedMap> TxnMutexGuard<M> {
    pub fn new(map: Arc<M>, lock: MemoryLockGuard) -> Self {
        TxnMutexGuard { map, lock }
    }

    pub fn set_lock_info(&self, mut lock_info: LockInfo, modifier: impl FnOnce(&mut LockInfo)) {
        let lock = &self.lock.0;
        let mut guard = lock.lock_info.lock();
        modifier(&mut lock_info);
        *guard = Some(lock_info);
    }
}

impl<M: OrderedMap> Drop for TxnMutexGuard<M> {
    fn drop(&mut self) {
        let lock = &self.lock.0;
        if lock.mutex_notifier.count.load(Ordering::SeqCst) == 0 {
            assert_eq!(lock.mutex_state.swap(OBSOLETE, Ordering::SeqCst), LOCKED);
            lock.mutex_notifier.notify(usize::MAX);
            self.map.remove(&lock.key);
        }
    }
}

#[derive(Default)]
pub struct Notifier {
    count: AtomicU64,
    event: Event,
}

impl Notifier {
    pub fn notify(&self, n: usize) {
        self.event.notify(n);
    }

    pub async fn listen(&self) -> Listener<'_> {
        self.count.fetch_add(1, Ordering::SeqCst);
        let listener = self.event.listen();
        Listener {
            count: &self.count,
            listener,
        }
    }
}

#[pin_project]
pub struct Listener<'a> {
    count: &'a AtomicU64,
    #[pin]
    listener: EventListener,
}

impl<'a> Future for Listener<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().listener.poll(cx)
    }
}

impl<'a> Drop for Listener<'a> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_txn_mutex() {
        let memory_lock = Arc::new(MemoryLock::default());
    }
}
