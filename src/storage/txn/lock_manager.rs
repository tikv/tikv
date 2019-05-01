// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::process::{execute_callback, ProcessResult};
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::Error as TxnError;
use crate::storage::{Error as StorageError, Key, StorageCb};
use crate::tikv_util::collections::HashMap;
use crate::tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};
use futures::Future;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tokio_core::reactor::Handle;
use tokio_timer::Delay;

#[derive(Clone)]
pub struct Lock {
    ts: u64,
    hash: u64,
}

pub fn extract_lock(res: &Result<(), StorageError>) -> Lock {
    match res {
        Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked { key, ts, .. }))) => Lock {
            ts: *ts,
            hash: gen_key_hash(&Key::from_raw(&key)),
        },
        _ => panic!("unsupported mvcc error"),
    }
}

pub enum Task {
    WaitFor {
        // which txn waiting for the lock
        start_ts: u64,
        for_update_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        timeout: u64,
    },
    WakeUp {
        // lock info
        lock_ts: u64,
        hashes: Vec<u64>,
        commit_ts: u64,
    },
    Timeout {
        // which txn is timeout
        start_ts: u64,
        lock: Lock,
    },
}

/// Debug for task.
impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Display for task.
impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::WaitFor { start_ts, lock, .. } => {
                write!(f, "txn:{} waiting for {}:{}", start_ts, lock.ts, lock.hash)
            }
            Task::WakeUp { lock_ts, .. } => write!(f, "waking up txns waiting for {}", lock_ts),
            Task::Timeout { start_ts, lock } => write!(
                f,
                "txn:{} waiting for {}:{} timeout",
                start_ts, lock.ts, lock.hash,
            ),
        }
    }
}

struct Waiter {
    start_ts: u64,
    for_update_ts: u64,
    cb: StorageCb,
    pr: ProcessResult,
    lock: Lock,
    timeout: u64,
}

type Waiters = Vec<Waiter>;

struct WaitTable {
    wait_table: HashMap<u64, Waiters>,
}

impl WaitTable {
    fn new() -> Self {
        Self {
            wait_table: HashMap::new(),
        }
    }

    fn size(&self) -> usize {
        self.wait_table.iter().map(|(_, v)| v.len()).sum()
    }

    fn add_waiter(&mut self, ts: u64, waiter: Waiter) -> bool {
        self.wait_table.entry(ts).or_insert(vec![]).push(waiter);
        true
    }

    fn get_ready_waiters(&mut self, ts: u64, mut hashes: Vec<u64>) -> Waiters {
        hashes.sort_unstable();
        let mut ready_waiters = vec![];
        if let Some(waiters) = self.wait_table.get_mut(&ts) {
            let mut i = 0;
            let mut count = waiters.len();
            while count > 0 {
                if hashes.binary_search(&waiters[i].lock.hash).is_ok() {
                    ready_waiters.push(waiters.swap_remove(i));
                } else {
                    i += 1;
                }
                count -= 1;
            }
            if waiters.is_empty() {
                self.wait_table.remove(&ts);
            }
        }
        ready_waiters
    }

    fn remove_waiter(&mut self, start_ts: u64, lock: Lock) -> Option<Waiter> {
        if let Some(waiters) = self.wait_table.get_mut(&lock.ts) {
            let idx = waiters
                .iter()
                .position(|waiter| waiter.start_ts == start_ts && waiter.lock.hash == lock.hash);
            if let Some(idx) = idx {
                let waiter = waiters.remove(idx);
                if waiters.is_empty() {
                    self.wait_table.remove(&lock.ts);
                }
                return Some(waiter);
            }
        }
        None
    }
}

#[derive(Clone)]
pub struct LockManagerScheduler(FutureScheduler<Task>);

fn notify_scheduler(scheduler: &FutureScheduler<Task>, task: Task) {
    if let Err(Stopped(task)) = scheduler.schedule(task) {
        error!("failed to send task to lock_manager: {}", task);
        match task {
            Task::WaitFor { cb, pr, .. } => execute_callback(cb, pr),
            // TODO: how to handle Timeout error?
            _ => (),
        }
    }
}

impl LockManagerScheduler {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self(scheduler)
    }

    pub fn wait_for(
        &self,
        start_ts: u64,
        for_update_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        timeout: u64,
    ) {
        notify_scheduler(
            &self.0,
            Task::WaitFor {
                start_ts,
                for_update_ts,
                cb,
                pr,
                lock,
                timeout,
            },
        );
    }

    pub fn wake_up(&self, lock_ts: u64, hashes: Vec<u64>, commit_ts: u64) {
        notify_scheduler(
            &self.0,
            Task::WakeUp {
                lock_ts,
                hashes,
                commit_ts,
            },
        );
    }
}

/// LockManager handles waiting and wake-up of pessimistic lock
pub struct LockManager {
    wait_table: WaitTable,
    scheduler: FutureScheduler<Task>,
}

impl LockManager {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self {
            wait_table: WaitTable::new(),
            scheduler,
        }
    }

    pub fn scheduler(&self) -> LockManagerScheduler {
        LockManagerScheduler::new(self.scheduler.clone())
    }

    fn add_waiter(&mut self, waiter: Waiter, handle: &Handle) {
        let lock = waiter.lock.clone();
        let start_ts = waiter.start_ts;
        let timeout = waiter.timeout;
        if self.wait_table.add_waiter(lock.ts, waiter) {
            let scheduler = self.scheduler.clone();
            let when = Instant::now() + Duration::from_millis(timeout);
            // TODO: cancel timer when wake up?
            let timer = Delay::new(when)
                .and_then(move |_| {
                    notify_scheduler(&scheduler, Task::Timeout { start_ts, lock });
                    Ok(())
                })
                .map_err(|e| info!("timeout timer delay errored"; "err" => ?e));
            handle.spawn(timer);
        }
    }

    fn wake_up(&mut self, lock_ts: u64, hashes: Vec<u64>, commit_ts: u64, handle: &Handle) {
        let mut ready_waiters = self.wait_table.get_ready_waiters(lock_ts, hashes);
        ready_waiters.sort_unstable_by_key(|waiter| waiter.start_ts);
        for (i, waiter) in ready_waiters.into_iter().enumerate() {
            // Sleep a little so the transaction with small start_ts will more likely get the lock.
            let when = Instant::now() + Duration::from_millis(10 * (i as u64));
            let timer = Delay::new(when)
                .and_then(move |_| {
                    // Maybe we can store the latest commit_ts in TiKV, and use
                    // it as conflict_commit_ts when waker's commit_ts is smaller
                    // than waiter's for_update_ts.
                    //
                    // If so TiDB can use this conflict_commit_ts as for_update_ts
                    // directly, there is no need to get a ts from PD.
                    let mvcc_err = MvccError::WriteConflict {
                        start_ts: waiter.for_update_ts,
                        conflict_start_ts: lock_ts,
                        conflict_commit_ts: commit_ts,
                        key: vec![],
                        primary: vec![],
                    };
                    let pr = ProcessResult::Failed {
                        err: StorageError::from(TxnError::from(mvcc_err)),
                    };
                    execute_callback(waiter.cb, pr);
                    Ok(())
                })
                .map_err(|e| info!("wake-up timer delay errored"; "err" => ?e));
            handle.spawn(timer);
        }
    }

    fn timeout(&mut self, start_ts: u64, lock: Lock) {
        self.wait_table
            .remove_waiter(start_ts, lock)
            .and_then(|waiter| {
                execute_callback(waiter.cb, waiter.pr);
                Some(())
            });
    }
}

impl FutureRunnable<Task> for LockManager {
    fn run(&mut self, task: Task, handle: &Handle) {
        match task {
            Task::WaitFor {
                start_ts,
                for_update_ts,
                cb,
                pr,
                lock,
                timeout,
            } => {
                self.add_waiter(
                    Waiter {
                        start_ts,
                        for_update_ts,
                        cb,
                        pr,
                        lock,
                        timeout,
                    },
                    handle,
                );
            }
            Task::WakeUp {
                lock_ts,
                hashes,
                commit_ts,
            } => {
                self.wake_up(lock_ts, hashes, commit_ts, handle);
            }
            Task::Timeout { start_ts, lock } => {
                self.timeout(start_ts, lock);
            }
        }
    }
}

pub fn gen_key_hash(key: &Key) -> u64 {
    let mut s = DefaultHasher::new();
    key.hash(&mut s);
    s.finish()
}

pub fn gen_key_hashes(keys: &[Key]) -> Vec<u64> {
    keys.iter().map(|key| gen_key_hash(key)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_util::KvGenerator;

    #[test]
    fn test_extract_lock() {
        let raw_key = b"key".to_vec();
        let key = Key::from_raw(&raw_key);
        let ts = 100;
        let case = StorageError::from(TxnError::from(MvccError::KeyIsLocked {
            key: raw_key,
            primary: vec![],
            ts,
            ttl: 0,
        }));
        let lock = extract_lock(&Err(case));
        assert_eq!(lock.ts, ts);
        assert_eq!(lock.hash, gen_key_hash(&key));
    }

    fn dummy_waiter(ts: u64, hash: u64) -> Waiter {
        Waiter {
            start_ts: 0,
            for_update_ts: 0,
            cb: StorageCb::Boolean(Box::new(|_| ())),
            pr: ProcessResult::Res,
            lock: Lock { ts, hash },
            timeout: 0,
        }
    }

    #[test]
    fn test_wait_table_add_and_remove() {
        let mut wait_table = WaitTable::new();
        for i in 0..10 {
            let n = i as u64;
            wait_table.add_waiter(n, dummy_waiter(n, n));
        }
        assert_eq!(10, wait_table.size());
        for i in (0..10).rev() {
            let n = i as u64;
            assert!(wait_table
                .remove_waiter(0, Lock { ts: n, hash: n })
                .is_some());
        }
        assert_eq!(0, wait_table.size());
        assert!(wait_table
            .remove_waiter(0, Lock { ts: 0, hash: 0 })
            .is_none());
    }

    #[test]
    fn test_wait_table_get_ready_waiters() {
        let mut wait_table = WaitTable::new();
        let ts = 100;
        let mut hashes: Vec<u64> = KvGenerator::new(64, 0)
            .generate(10)
            .into_iter()
            .map(|(key, _)| gen_key_hash(&Key::from_raw(&key)))
            .collect();
        for hash in hashes.iter() {
            wait_table.add_waiter(ts, dummy_waiter(ts, *hash));
        }
        hashes.sort();

        let not_ready = hashes.split_off(hashes.len() / 2);
        let ready_waiters = wait_table.get_ready_waiters(ts, hashes.clone());
        assert_eq!(hashes.len(), ready_waiters.len());
        assert_eq!(not_ready.len(), wait_table.size());

        let ready_waiters = wait_table.get_ready_waiters(ts, hashes.clone());
        assert!(ready_waiters.is_empty());

        let ready_waiters = wait_table.get_ready_waiters(ts, not_ready.clone());
        assert_eq!(not_ready.len(), ready_waiters.len());
        assert_eq!(0, wait_table.size());
    }

    #[test]
    fn test_lock_manager() {
        use crate::tikv_util::worker::FutureWorker;
        use std::sync::mpsc;

        let mut lm_worker = FutureWorker::new("lock-manager");
        let lm_runner = LockManager::new(lm_worker.scheduler());
        let lm_scheduler = lm_runner.scheduler();
        lm_worker.start(lm_runner).unwrap();

        // timeout
        let (tx, rx) = mpsc::channel();
        let cb = Box::new(move |result| {
            tx.send(result).unwrap();
        });
        let pr = ProcessResult::Res;
        lm_scheduler.wait_for(
            0,
            0,
            StorageCb::Boolean(cb),
            pr,
            Lock { ts: 0, hash: 0 },
            100,
        );
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(150))
                .unwrap()
                .unwrap(),
            ()
        );

        // wake-up
        let (tx, rx) = mpsc::channel();
        let cb = Box::new(move |result| {
            tx.send(result).unwrap();
        });
        lm_scheduler.wait_for(
            0,
            0,
            StorageCb::Boolean(cb),
            ProcessResult::Res,
            Lock { ts: 1, hash: 1 },
            100,
        );
        lm_scheduler.wake_up(1, vec![3, 1, 2], 1);
        assert!(rx.recv_timeout(Duration::from_millis(50)).unwrap().is_err());
    }
}
