// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::process::{execute_callback, ProcessResult};
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::Error as TxnError;
use crate::storage::{Error as StorageError, Key, StorageCb};
use crate::util::collections::HashMap;
use crate::util::worker::{FutureRunnable, FutureScheduler, Stopped};
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
            // Sleep a little so the transaction in lower position will more likely get the lock.
            let when = Instant::now() + Duration::from_millis(10 * (i as u64));
            let timer = Delay::new(when)
                .and_then(move |_| {
                    let mut conflict_ts = commit_ts;
                    if conflict_ts < waiter.for_update_ts {
                        conflict_ts = std::u64::MAX;
                    }
                    let mvcc_err = MvccError::WriteConflict {
                        start_ts: waiter.for_update_ts,
                        conflict_start_ts: lock_ts,
                        conflict_commit_ts: conflict_ts,
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
