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

    /// shrink shrinks the tasks can be executed concurrently by n
    /// would block until the quota applied.
    pub fn shrink(&self, n: usize) -> Result<()> {
        for _ in 0..n {
            self.rx.recv()?;
        }
        self.cap.fetch_sub(n, Ordering::SeqCst);
        Ok(())
    }

    /// grow grows the tasks can be executed concurrently by n
    pub fn grow(&self, n: usize) -> Result<()> {
        for _ in 0..n {
            self.sx.send(())?;
        }
        self.cap.fetch_add(n, Ordering::SeqCst);
        Ok(())
    }

    /// resize the tasks avaliable.
    pub fn resize(&self, target: usize) -> Result<()> {
        if target < 0 {
            warn!("trying to resize softlimit to negtive number!"; "target" => %target);
            return Ok(());
        }
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
        let diff = current as isize - target as isize;
        if diff > 0 {
            self.shrink(diff as usize)?;
        } else if diff < 0 {
            self.grow((-diff) as usize)?;
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
    metrics: RefCell<ThreadInfoStatistics>,
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
        // TODO don't make it so MAGIC!
        let idle = self.current_idle_exclude(|s| s.contains("bkwkr")) as usize;
        limit.resize(idle - self.keep_remain)?;
        Ok(())
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
        time::Duration,
    };

    use tikv_util::{defer, sys::SysQuota};

    use super::{SoftLimit, SoftLimitByCPU};

    fn busy<F>(n: usize, f: F) -> impl FnOnce()
    where
        F: FnMut() + Send + Clone + 'static,
    {
        let i = (0..n)
            .map(|i| {
                let (sx, rx) = crossbeam::channel::bounded(1);
                let mut f = f.clone();
                std::thread::Builder::new()
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

    #[test]
    // FIXME: this test might be unstable :(
    fn test_current_idle() {
        let stop = busy(2, || {});
        defer! { stop() }
        let cpu = SoftLimitByCPU::new();
        let total = cpu.current_idle();
        std::thread::sleep(Duration::from_secs(1));
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
            working.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
            working.fetch_sub(1, Ordering::SeqCst);
        });
        defer! {stop()}
        std::thread::sleep(Duration::from_millis(1000));
        limit_cloned.shrink(2).unwrap();
        while working_cloned.load(Ordering::SeqCst) != 2 {
            println!("waiting for worker shrink to 2...");
            std::thread::sleep(Duration::from_millis(1000));
        }

        limit_cloned.grow(1).unwrap();
        while working_cloned.load(Ordering::SeqCst) != 3 {
            println!("waiting for worker grow to 3...");
            std::thread::sleep(Duration::from_millis(1000));
        }

        limit_cloned.grow(2).unwrap();
        while working_cloned.load(Ordering::SeqCst) != 4 {
            println!("waiting for worker grow to 4...");
            std::thread::sleep(Duration::from_millis(1000));
        }
    }

    #[test]
    fn test_cpu_limit() {
        let stop = busy(2, || {});
        let cpu_limit = SoftLimitByCPU::new();
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        std::thread::sleep(Duration::from_millis(1000));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 2);
        stop();
        std::thread::sleep(Duration::from_millis(1000));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count);
    }

    #[test]
    fn test_cpu_limit_remain() {
        let stop = busy(1, || {});
        let cpu_limit = SoftLimitByCPU::with_remain(1);
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        std::thread::sleep(Duration::from_millis(1000));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 2);
        stop();
        std::thread::sleep(Duration::from_millis(1000));
        cpu_limit.exec_over(&limit).unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 1);
    }
}
