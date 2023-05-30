// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use futures::future::{Future, FutureExt};
use tokio::sync::oneshot;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub struct WaitGroup {
    running: AtomicUsize,
    on_finish_all: std::sync::Mutex<Vec<Box<dyn FnOnce(bool) + Send + 'static>>>,
    run_success: AtomicBool,
}

impl WaitGroup {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            running: AtomicUsize::new(0),
            on_finish_all: std::sync::Mutex::default(),
            run_success: true.into(),
        })
    }

    fn work_done(&self) {
        let last = self.running.fetch_sub(1, Ordering::SeqCst);
        if last == 1 {
            self.on_finish_all
                .lock()
                .unwrap()
                .drain(..)
                .for_each(|x| x(true))
        }
    }

    pub fn work_fail(&self) {
        self.run_success.store(false, Ordering::SeqCst);
        self.on_finish_all
            .lock()
            .unwrap()
            .drain(..)
            .for_each(|x| x(false))
    }

    /// wait until all running tasks done.
    pub fn wait(&self) -> BoxFuture<bool> {
        if !self.run_success.load(Ordering::SeqCst) {
            return Box::pin(futures::future::ready(false));
        }
        // Fast path: no uploading.
        if self.running.load(Ordering::SeqCst) == 0 {
            return Box::pin(futures::future::ready(
                self.run_success.load(Ordering::SeqCst),
            ));
        }

        let (tx, rx) = oneshot::channel();
        self.on_finish_all
            .lock()
            .unwrap()
            .push(Box::new(move |success| {
                // The waiter may timed out.
                let _ = tx.send(success);
            }));
        // try to acquire the lock again.
        if self.running.load(Ordering::SeqCst) == 0 {
            return Box::pin(futures::future::ready(
                self.run_success.load(Ordering::SeqCst),
            ));
        }
        Box::pin(rx.map(|result| result.ok().unwrap_or(false)))
    }

    /// make a work, as long as the return value held, mark a work in the group
    /// is running.
    pub fn work(self: Arc<Self>) -> Option<Work> {
        if !self.run_success.load(Ordering::SeqCst) {
            return None;
        }
        self.running.fetch_add(1, Ordering::SeqCst);
        Some(Work(self))
    }
}

pub struct Work(Arc<WaitGroup>);

impl Drop for Work {
    fn drop(&mut self) {
        self.0.work_done();
    }
}

impl std::fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let running = self.running.load(Ordering::Relaxed);
        f.debug_struct("WaitGroup")
            .field("running", &running)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use futures::executor::block_on;

    use crate::waitgroup::WaitGroup;

    #[test]
    fn test_wait_group() {
        #[derive(Debug)]
        struct Case {
            bg_task: usize,
            repeat: usize,
        }

        fn run_case(c: Case) {
            for i in 0..c.repeat {
                let wg = WaitGroup::new();
                let cnt = Arc::new(AtomicUsize::new(c.bg_task));
                for _ in 0..c.bg_task {
                    let cnt = cnt.clone();
                    let work = wg.clone().work();
                    tokio::spawn(async move {
                        cnt.fetch_sub(1, Ordering::SeqCst);
                        drop(work);
                    });
                }
                block_on(tokio::time::timeout(Duration::from_secs(20), wg.wait())).unwrap();
                assert_eq!(cnt.load(Ordering::SeqCst), 0, "{:?}@{}", c, i);
            }
        }

        let cases = [
            Case {
                bg_task: 200000,
                repeat: 1,
            },
            Case {
                bg_task: 65535,
                repeat: 1,
            },
            Case {
                bg_task: 512,
                repeat: 1,
            },
            Case {
                bg_task: 2,
                repeat: 100000,
            },
            Case {
                bg_task: 1,
                repeat: 100000,
            },
            Case {
                bg_task: 0,
                repeat: 1,
            },
        ];

        let pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        let _guard = pool.handle().enter();
        for case in cases {
            run_case(case)
        }
    }

    #[test]
    fn test_failed() {
        let pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        let _guard = pool.handle().enter();

        let wg = WaitGroup::new();
        const TESTNUM: usize = 100;
        let cnt = Arc::new(AtomicUsize::new(TESTNUM));
        for i in 0..TESTNUM {
            let wg_clone = wg.clone();
            if i == TESTNUM / 2 {
                wg_clone.work_fail();
            }
            let cnt = cnt.clone();
            let work = wg_clone.clone().work();
            if work.is_some() {
                cnt.fetch_sub(1, Ordering::SeqCst);
            }
            tokio::spawn(async move {
                drop(work);
            });
        }
        if let Ok(success) = block_on(tokio::time::timeout(Duration::from_secs(20), wg.wait())) {
            assert!(!success);
            assert_eq!(cnt.load(Ordering::SeqCst), TESTNUM / 2);
        };
    }
}
