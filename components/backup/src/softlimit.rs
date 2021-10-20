use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{Error, Result};
use crossbeam::channel::{Receiver, Sender};
use tikv_util::metrics::ThreadInfoStatistics;
use tikv_util::sys::SysQuota;
use tikv_util::{error, warn};

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

/// SoftLimit is an simple "worker pool" just for
/// restricting the number of workers can running concurrently.
impl SoftLimit {
    fn enter(&self) -> Result<()> {
        self.rx.recv()?;
        Ok(())
    }

    /// guard make a guard for one concurrent executing task.
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

    /// shrink shrinks the tasks can be executed concurrently by n
    /// would block until the quota applied.
    #[cfg(test)]
    pub fn shrink(&self, n: usize) -> Result<()> {
        self.cap.fetch_sub(n, Ordering::SeqCst);
        self.take_tokens(n)?;
        Ok(())
    }

    /// grow grows the tasks can be executed concurrently by n
    #[cfg(test)]
    pub fn grow(&self, n: usize) -> Result<()> {
        self.cap.fetch_add(n, Ordering::SeqCst);
        self.grant_tokens(n)?;
        Ok(())
    }

    /// resize the tasks available concurrently.
    pub fn resize(&self, target: usize) -> Result<()> {
        let current = loop {
            let current = self.cap.load(Ordering::SeqCst);
            if self
                .cap
                .compare_exchange(current, target, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break current;
            }
        };
        if current > target {
            self.take_tokens(current - target)?;
        } else if current < target {
            self.grant_tokens(target - current)?;
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

pub struct SoftLimitByCPU {
    pub(crate) metrics: RefCell<ThreadInfoStatistics>,
    total_time: f64,
    keep_remain: usize,
}

impl SoftLimitByCPU {
    /// returns the current idle processor.
    /// **note that this might get inaccuracy if there are other processes
    /// running in the same CPU.**
    fn current_idle(&self) -> f64 {
        self.current_idle_exclude(|_| false)
    }

    /// returns the current idle processor, ignoring threads with name matches the predicate.
    fn current_idle_exclude(&self, mut exclude: impl FnMut(&str) -> bool) -> f64 {
        self.metrics.borrow_mut().record();
        let usages = self.metrics.borrow().get_cpu_usages();
        let used = usages
            .into_iter()
            .filter(|(s, _)| !exclude(s))
            .fold(0f64, |data, (_, usage)| data + (usage as f64 / 100f64));
        self.total_time - used
    }

    /// apply the limit to the soft limit accroding to the current CPU remaining.
    pub fn exec_over(&self, limit: &SoftLimit) -> Result<()> {
        self.exec_over_with_exclude(limit, |_| true)
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
        limit.resize(idle.checked_sub(self.keep_remain).unwrap_or(0).max(1))?;
        Ok(())
    }

    /// set the keep_remain to the keeper.
    /// this applies to subquent `exec_over` calls.
    pub fn set_remain(&mut self, remain: usize) {
        self.keep_remain = remain;
    }

    pub fn new() -> Self {
        Self::with_remain(0)
    }

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

    use super::{SoftLimit, SoftLimitByCPU};

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
                        while let Err(_) = rx.try_recv() {
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
                thread::sleep(d.div_f32(10.0));
            }
        })
    }

    #[test]
    // FIXME: this test might be unstable :(
    fn test_current_idle() {
        let stop = busy(2, || {});
        defer! { stop() }
        let cpu = SoftLimitByCPU::new();
        let total = cpu.current_idle();
        thread::sleep(Duration::from_secs(1));
        let total_after_busy = cpu.current_idle();
        assert!(
            (total - total_after_busy - 2f64).abs() < 0.5f64,
            "total and total_after_busy diff too huge: {} vs {} usage map = {:?}",
            total,
            total_after_busy,
            cpu.metrics.borrow().get_cpu_usages()
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
            thread::sleep(Duration::from_millis(100));
            working_cloned.fetch_sub(1, Ordering::SeqCst);
        });
        defer! {stop()}
        thread::sleep(Duration::from_millis(1000));
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

        let working_cloned = working.clone();
        limit_cloned.grow(2).unwrap();
        should_satify_in(
            Duration::from_secs(10),
            "waiting for worker grow to 4",
            move || working_cloned.load(Ordering::SeqCst) == 4,
        )
    }

    #[test]
    fn test_cpu_limit() {
        let stop = busy(2, || {});
        let cpu_limit = SoftLimitByCPU::new();
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        thread::sleep(Duration::from_millis(1000));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(
            limit.current_cap(),
            cpu_count - 2,
            "map = {:?}",
            cpu_limit.metrics.borrow().get_cpu_usages()
        );
        stop();
        thread::sleep(Duration::from_millis(1000));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count);
    }

    #[test]
    fn test_cpu_limit_remain() {
        let stop = busy(1, || {});
        let cpu_limit = SoftLimitByCPU::with_remain(1);
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        thread::sleep(Duration::from_millis(100));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 2);
        stop();
        thread::sleep(Duration::from_millis(100));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 1);
    }
}
