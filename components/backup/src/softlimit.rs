// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::Result;
use collections::HashMap;
use crossbeam::channel::{Receiver, Sender};

use tikv_util::error;
use tikv_util::metrics::ThreadInfoStatistics;
use tikv_util::sys::SysQuota;

/// SoftLimit is an simple "worker pool" just for
/// restricting the number of workers can running concurrently.
/// It is just a synchronous version of [tokio::sync::Semaphore].
/// (With a poor(has O(n) space complex) but simple implementation.(not involved lock conditions))
/// We should replace it with [tokio::sync::Semaphore] once backup has been refactored to asynchronous.
#[derive(Clone)]
pub struct SoftLimit {
    sx: Sender<()>,
    rx: Receiver<()>,
    cap: Arc<AtomicUsize>,
}

pub struct Guard {
    sx: Sender<()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Err(err) = self.sx.send(()) {
            error!("failed to drop soft limit guard."; "err" => %err);
        }
    }
}

impl SoftLimit {
    fn enter(&self) -> Result<()> {
        self.rx.recv()?;
        Ok(())
    }

    /// Makes a guard for one concurrent executing task.
    /// drop the guard for releasing the resource.
    pub fn guard(&self) -> Result<Guard> {
        self.enter()?;
        Ok(Guard {
            sx: self.sx.clone(),
        })
    }

    fn take_tokens(&self, n: usize) -> Result<()> {
        for _ in 0..n {
            self.rx.recv()?;
        }
        Ok(())
    }

    fn grant_tokens(&self, n: usize) -> Result<()> {
        for _ in 0..n {
            self.sx.send(())?;
        }
        Ok(())
    }

    /// Shrinks the tasks can be executed concurrently by n
    /// would block until the quota applied.
    #[cfg(test)]
    pub fn shrink(&self, n: usize) -> Result<()> {
        self.cap.fetch_sub(n, Ordering::SeqCst);
        self.take_tokens(n)?;
        Ok(())
    }

    /// Grows the tasks can be executed concurrently by n
    #[cfg(test)]
    pub fn grow(&self, n: usize) -> Result<()> {
        self.cap.fetch_add(n, Ordering::SeqCst);
        self.grant_tokens(n)?;
        Ok(())
    }

    /// resize the tasks available concurrently.
    pub fn resize(&self, target: usize) -> Result<()> {
        let current = self.cap.swap(target, Ordering::SeqCst);
        match current.cmp(&target) {
            CmpOrder::Greater => {
                self.take_tokens(current - target)?;
            }
            CmpOrder::Less => {
                self.grant_tokens(target - current)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// returns the maxmium of tasks can be executed concurrently for now.
    pub fn current_cap(&self) -> usize {
        self.cap.load(Ordering::SeqCst)
    }

    /// make a new softlimit.
    pub fn new(initial_size: usize) -> Self {
        let (sx, rx) = crossbeam::channel::unbounded();
        let cap = Arc::new(AtomicUsize::new(initial_size));
        // should never fail.
        (0..initial_size).for_each(|_| sx.send(()).unwrap());
        Self { sx, rx, cap }
    }
}

pub trait CpuStatistics {
    type Container: IntoIterator<Item = (String, u64)>;
    fn get_cpu_usages(&self) -> Self::Container;
}

pub struct SoftLimitByCpu<Statistics> {
    pub(crate) metrics: Statistics,
    total_time: f64,
    keep_remain: usize,
}

impl CpuStatistics for RefCell<ThreadInfoStatistics> {
    type Container = HashMap<String, u64>;

    fn get_cpu_usages(&self) -> Self::Container {
        self.borrow_mut().record();
        ThreadInfoStatistics::get_cpu_usages(&*self.borrow())
    }
}

impl<Statistics: CpuStatistics> SoftLimitByCpu<Statistics> {
    /// returns the current idle processor.
    /// **note that this might get inaccuracy if there are other processes
    /// running in the same CPU.**
    #[cfg(test)]
    fn current_idle(&self) -> f64 {
        self.current_idle_exclude(|_| false)
    }

    /// returns the current idle processor, ignoring threads with name matches the predicate.
    fn current_idle_exclude(&self, mut exclude: impl FnMut(&str) -> bool) -> f64 {
        let usages = self.metrics.get_cpu_usages();
        let used = usages
            .into_iter()
            .filter(|(s, _)| !exclude(s))
            .fold(0f64, |data, (_, usage)| data + (usage as f64 / 100f64));
        self.total_time - used
    }

    /// apply the limit to the soft limit accroding to the current CPU remaining.
    #[cfg(test)]
    pub fn exec_over(&self, limit: &SoftLimit) -> Result<()> {
        self.exec_over_with_exclude(limit, |_| false)
    }

    /// apply the limit to the soft limit accroding to the current CPU remaining.
    /// when cacluating the CPU usage, ignore threads with name matched by the exclude predicate.
    /// This would keep at least one thread working.
    pub fn exec_over_with_exclude(
        &self,
        limit: &SoftLimit,
        exclude: impl FnMut(&str) -> bool,
    ) -> Result<()> {
        let idle = self.current_idle_exclude(exclude) as usize;
        limit.resize(idle.saturating_sub(self.keep_remain).max(1))?;
        Ok(())
    }

    /// set the keep_remain to the keeper.
    /// this applies to subquent `exec_over` calls.
    pub fn set_remain(&mut self, remain: usize) {
        self.keep_remain = remain;
    }
}

impl SoftLimitByCpu<RefCell<ThreadInfoStatistics>> {
    pub fn with_remain(remain: usize) -> Self {
        let total = SysQuota::cpu_cores_quota();
        let metrics = RefCell::new(ThreadInfoStatistics::new());
        Self {
            metrics,
            total_time: total,
            keep_remain: remain,
        }
    }
}

#[cfg(test)]
mod softlimit_test {
    use std::{
        sync::{
            atomic::{AtomicU8, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use crossbeam::channel;
    use tikv_util::{defer, sys::SysQuota};

    use super::{CpuStatistics, SoftLimit, SoftLimitByCpu};

    #[derive(Default)]
    struct TestCpuEnv {
        used: u64,
    }

    impl CpuStatistics for TestCpuEnv {
        type Container = Vec<(String, u64)>;

        fn get_cpu_usages(&self) -> Self::Container {
            (0..self.used)
                .map(|i| (format!("thread-{}", i), 100))
                .collect()
        }
    }

    impl TestCpuEnv {
        fn mock_busy(n: usize) -> SoftLimitByCpu<Self> {
            let env = Self { used: n as u64 };
            SoftLimitByCpu {
                metrics: env,
                total_time: SysQuota::cpu_cores_quota(),
                keep_remain: 0,
            }
        }

        fn stop_busy(&mut self) {
            self.used = 0;
        }
    }

    fn busy<F>(n: usize, f: F) -> impl FnOnce()
    where
        F: FnMut() + Send + Clone + 'static,
    {
        let i = (0..n)
            .map(|i| {
                let (sx, rx) = channel::bounded(1);
                let mut f = f.clone();
                thread::Builder::new()
                    .name(format!("busy_{}", i))
                    .spawn(move || {
                        while rx.try_recv().is_err() {
                            f()
                        }
                    })
                    .unwrap();
                sx
            })
            .collect::<Vec<_>>();
        move || i.into_iter().for_each(|sx| sx.send(()).unwrap())
    }

    fn should_finish_in(d: Duration, name: &str, f: impl FnOnce() + Send + 'static) {
        let (sx, rx) = channel::bounded(1);
        thread::spawn(move || {
            f();
            sx.send(())
        });
        match rx.recv_timeout(d) {
            Ok(()) => {}
            Err(e) => panic!(
                "test failed: execution of {} timed out or disconnected. (timeout = {:?}, error = {})",
                name, d, e
            ),
        }
    }

    fn should_satify_in(d: Duration, name: &str, mut f: impl FnMut() -> bool + Send + 'static) {
        should_finish_in(d, name, move || {
            while !f() {
                thread::sleep(d.div_f32(50.0));
            }
        })
    }

    #[test]
    // FIXME: this test might be unstable :(
    fn test_current_idle() {
        let mut cpu = TestCpuEnv::mock_busy(2);
        let total_after_busy = cpu.current_idle();
        cpu.metrics.stop_busy();
        let total = cpu.current_idle();
        assert!(
            (total - total_after_busy - 2f64).abs() < 0.5f64,
            "total and total_after_busy diff too huge: {} vs {}",
            total,
            total_after_busy,
        );
    }

    #[test]
    fn test_limit() {
        let limit = SoftLimit::new(4);
        let limit_cloned = limit.clone();
        let working = Arc::new(AtomicU8::new(0));
        let working_cloned = working.clone();

        let stop = busy(4, move || {
            let _guard = limit.guard();
            working_cloned.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(10));
            working_cloned.fetch_sub(1, Ordering::SeqCst);
        });
        defer! {stop()}
        let working_cloned = working.clone();
        limit_cloned.shrink(2).unwrap();
        should_satify_in(
            Duration::from_secs(10),
            "waiting for worker shrink to 2",
            move || working_cloned.load(Ordering::SeqCst) == 2,
        );

        limit_cloned.grow(1).unwrap();
        let working_cloned = working.clone();
        should_satify_in(
            Duration::from_secs(10),
            "waiting for worker grow to 3",
            move || working_cloned.load(Ordering::SeqCst) == 3,
        );

        let working_cloned = working;
        limit_cloned.grow(2).unwrap();
        should_satify_in(
            Duration::from_secs(10),
            "waiting for worker grow to 4",
            move || working_cloned.load(Ordering::SeqCst) == 4,
        )
    }

    #[test]
    fn test_cpu_limit() {
        let mut cpu_limit = TestCpuEnv::mock_busy(2);
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(
            limit.current_cap(),
            cpu_count - 2,
            "map = {:?}",
            cpu_limit.metrics.get_cpu_usages()
        );
        cpu_limit.metrics.stop_busy();
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count);
    }

    #[test]
    fn test_cpu_limit_remain() {
        let mut cpu_limit = TestCpuEnv::mock_busy(1);
        cpu_limit.set_remain(1);
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 2);
        cpu_limit.metrics.stop_busy();
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 1);
    }
}
