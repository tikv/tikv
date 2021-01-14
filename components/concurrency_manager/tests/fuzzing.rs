// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use futures::executor::block_on;
use parking_lot::Mutex;
use rand::prelude::*;
use rayon::prelude::*;
use std::{
    borrow::Cow,
    cmp, fmt,
    fmt::Debug,
    mem,
    mem::ManuallyDrop,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Barrier,
    },
    thread,
    time::Duration,
};
use txn_types::{Key, Lock, LockType, TimeStamp};

#[derive(Clone, PartialOrd, PartialEq)]
struct ArcKey(Arc<[u8]>);

impl From<Key> for ArcKey {
    fn from(key: Key) -> ArcKey {
        ArcKey(Arc::from(key.into_encoded().into_boxed_slice()))
    }
}

impl ArcKey {
    fn to_key(&self) -> ManuallyDrop<Key> {
        // UB!!! But we don't care it now because it's a test that doesn't run daily.
        unsafe {
            let len = self.0.len();
            let v = Vec::from_raw_parts(self.0.as_ptr() as _, len, len);
            ManuallyDrop::new(Key::from_encoded(v))
        }
    }
}

impl Debug for ArcKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &*self.0 {
            write!(f, "{:02X}", b)?;
        }
        Ok(())
    }
}

struct KeyPool {
    mutable: Mutex<Vec<ArcKey>>,
    all: Vec<ArcKey>,
}

impl KeyPool {
    fn new(short_key: usize, long_key: usize) -> KeyPool {
        let mut v = Vec::new();
        let mut rng = thread_rng();
        let mut buf = [0u8; 1024];

        // short keys
        for _ in 0..short_key {
            rng.fill_bytes(&mut buf[..16]);
            v.push(Key::from_raw(&buf[..16]).into());
        }

        // long keys
        for _ in 0..long_key {
            let len = rng.gen_range(512, 1024);
            rng.fill_bytes(&mut buf[..len]);
            v.push(Key::from_raw(&buf[..len]).into());
        }

        KeyPool {
            mutable: Mutex::new(v.clone()),
            all: v,
        }
    }

    fn get_one(&self) -> ArcKey {
        let index = thread_rng().gen_range(0, self.all.len());
        self.all[index].clone()
    }

    fn take_one(&self) -> Option<ArcKey> {
        let mut this = self.mutable.lock();
        if this.is_empty() {
            None
        } else {
            let index = thread_rng().gen_range(0, this.len());
            Some(this.swap_remove(index))
        }
    }

    fn add_key(&self, key: ArcKey) {
        self.mutable.lock().push(key)
    }
}

struct Tso(AtomicU64);

impl Tso {
    fn new() -> Tso {
        Tso(AtomicU64::new(1))
    }

    fn get_ts(&self) -> TimeStamp {
        TimeStamp::from(self.0.fetch_add(1, Ordering::AcqRel))
    }
}

#[derive(Debug)]
enum OperationLog {
    PointRead {
        start_ts: TimeStamp,
        key: ArcKey,
        locked: bool,
    },
    RangeRead {
        start_ts: TimeStamp,
        start_key: ArcKey,
        end_key: ArcKey,
        locked: bool,
    },
    Write {
        min_commit_ts: TimeStamp,
        key: ArcKey,
    },
}

// This test is heavy so we shouldn't run it daily.
// Run it with the following command (recommending release mode) and see the printed stats:
//
// ```
// cargo test --package concurrency_manager --test fuzzing --release -- test_memory_usage --exact --ignored --nocapture
// ```
#[test]
#[ignore]
fn fuzzing() {
    const ROUND_NUM: usize = 1000;
    const ROUND_TIME: Duration = Duration::from_secs(10);
    const WRITER_NUM: usize = 8;
    const READER_NUM: usize = 64;

    let key_pool = Arc::new(KeyPool::new(32, 32));
    let cm = ConcurrencyManager::new(1.into());
    let finished = Arc::new(AtomicBool::new(false));
    let (guard_tx, guard_rx) =
        mpsc::sync_channel::<(KeyHandleGuard, TimeStamp, ArcKey, bool)>(1024);
    let barrier = Arc::new(Barrier::new(WRITER_NUM + READER_NUM + 1));
    let tso = Arc::new(Tso::new());
    let logs = Arc::new(Mutex::new(Vec::new()));

    {
        let logs = logs.clone();
        let key_pool = key_pool.clone();
        thread::Builder::new()
            .name("callback".into())
            .spawn(move || {
                for (guard, min_commit_ts, key, key_from_pool) in guard_rx {
                    logs.lock().push(OperationLog::Write {
                        min_commit_ts,
                        key: key.clone(),
                    });
                    drop(guard);
                    if key_from_pool {
                        key_pool.add_key(key);
                    }
                }
            })
            .unwrap();
    }

    for round in 0..ROUND_NUM {
        println!("start round {}", round);

        let mut threads = Vec::new();
        finished.store(false, Ordering::Release);
        logs.lock().clear();

        for _ in 0..WRITER_NUM {
            let key_pool = key_pool.clone();
            let cm = cm.clone();
            let finished = finished.clone();
            let tso = tso.clone();
            let guard_tx = guard_tx.clone();
            let barrier = barrier.clone();
            let th = thread::Builder::new()
                .name("writer".into())
                .spawn(move || {
                    barrier.wait();
                    while !finished.load(Ordering::Acquire) {
                        thread::yield_now();
                        let start_ts = tso.get_ts();
                        let (key, key_from_pool) = match key_pool.take_one() {
                            Some(key) => (key, true),
                            None => {
                                let mut buf = [0u8; 64];
                                let len = thread_rng().gen_range(8, 64);
                                thread_rng().fill_bytes(&mut buf[..len]);
                                (Key::from_raw(&buf[..len]).into(), false)
                            }
                        };
                        let mut min_commit_ts = TimeStamp::default();
                        let guard = block_on(cm.lock_key(&key.to_key()));
                        guard.with_lock(|lock| {
                            min_commit_ts = cmp::max(start_ts, cm.max_ts()).next();
                            *lock = Some(Lock::new(
                                LockType::Put,
                                vec![],
                                start_ts,
                                1,
                                None,
                                start_ts,
                                1,
                                min_commit_ts,
                            ));
                        });
                        guard_tx
                            .send((guard, min_commit_ts, key, key_from_pool))
                            .unwrap();
                    }
                })
                .unwrap();
            threads.push(th);
        }

        for _ in 0..READER_NUM {
            let key_pool = key_pool.clone();
            let cm = cm.clone();
            let finished = finished.clone();
            let tso = tso.clone();
            let logs = logs.clone();
            let barrier = barrier.clone();
            let th = thread::Builder::new()
                .name("reader".into())
                .spawn(move || {
                    barrier.wait();
                    while !finished.load(Ordering::Acquire) {
                        thread::yield_now();
                        let start_ts = tso.get_ts();
                        cm.update_max_ts(start_ts);
                        if thread_rng().gen_bool(0.8) {
                            // Point read
                            let key = key_pool.get_one();
                            let locked = {
                                let key = key.to_key();
                                cm.read_key_check(&key, |l| {
                                    Lock::check_ts_conflict(
                                        Cow::Borrowed(&l),
                                        &key,
                                        start_ts,
                                        &Default::default(),
                                    )
                                })
                                .is_err()
                            };
                            logs.lock().push(OperationLog::PointRead {
                                start_ts,
                                key,
                                locked,
                            })
                        } else {
                            // Range read
                            let (mut k1, mut k2) = (key_pool.get_one(), key_pool.get_one());
                            if k2 < k1 {
                                mem::swap(&mut k1, &mut k2);
                            }
                            let locked = {
                                let (k1, k2) = (k1.to_key(), k2.to_key());
                                cm.read_range_check(Some(&k1), Some(&k2), |key, l| {
                                    Lock::check_ts_conflict(
                                        Cow::Borrowed(&l),
                                        &key,
                                        start_ts,
                                        &Default::default(),
                                    )
                                })
                                .is_err()
                            };
                            logs.lock().push(OperationLog::RangeRead {
                                start_ts,
                                start_key: k1,
                                end_key: k2,
                                locked,
                            })
                        }
                    }
                })
                .unwrap();
            threads.push(th);
        }

        barrier.wait();
        thread::sleep(ROUND_TIME);
        finished.store(true, Ordering::Release);
        for th in threads {
            th.join().unwrap();
        }

        // validate
        let logs = logs.lock();
        println!("start validating {} logs", logs.len());
        let mut max_ts_idx = Vec::new();
        let mut max_ts = TimeStamp::zero();
        let mut write_logs = Vec::new();
        // preprocess
        for (log_idx, log) in logs.iter().enumerate() {
            match log {
                OperationLog::PointRead { start_ts, .. }
                | OperationLog::RangeRead { start_ts, .. } => {
                    if *start_ts > max_ts {
                        max_ts = *start_ts;
                        max_ts_idx.push((*start_ts, log_idx));
                    }
                }
                w => {
                    write_logs.push((log_idx, w));
                }
            }
        }
        // for each write log, check all read logs whose read_ts > min_commit_ts but read_log_idx < write_log_idx
        write_logs.into_par_iter().for_each(|(log_idx, log_w)| {
            let (min_commit_ts, write_key) = match log_w {
                OperationLog::Write { min_commit_ts, key } => (min_commit_ts, key),
                _ => unreachable!(),
            };
            let max_ts_i =
                match max_ts_idx.binary_search_by_key(&min_commit_ts, |(max_ts, _)| max_ts) {
                    Ok(idx) | Err(idx) => idx,
                };
            if max_ts_i == max_ts_idx.len() {
                return;
            }
            let start_idx = max_ts_idx[max_ts_i].1;
            if start_idx >= log_idx {
                return;
            }
            for log_r in &logs[start_idx..log_idx] {
                match log_r {
                    OperationLog::PointRead {
                        start_ts,
                        key,
                        locked,
                    } => {
                        let should_be_locked = start_ts >= min_commit_ts && write_key == key;
                        assert!(*locked || !should_be_locked, "{:?} {:?}", log_w, log_r);
                    }
                    OperationLog::RangeRead {
                        start_ts,
                        start_key,
                        end_key,
                        locked,
                    } => {
                        let should_be_locked = start_ts >= min_commit_ts
                            && write_key >= start_key
                            && write_key < end_key;
                        assert!(*locked || !should_be_locked, "{:?} {:?}", log_w, log_r);
                    }
                    _ => {}
                }
            }
        });
    }
}
