// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering as CmpOrder,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use collections::HashMap;
use tikv_util::{metrics::ThreadInfoStatistics, sys::SysQuota};
use tokio::sync::{Semaphore, SemaphorePermit};

use super::Result;

/// SoftLimit is an simple "worker pool" just for
/// restricting the number of workers can running concurrently.
/// It is simply a wrapper over [tokio::sync::Semaphore],
/// with a counter recording the current permits already and would grant.
struct SoftLimitInner {
    semaphore: Semaphore,
    cap: AtomicUsize,
}

#[derive(Clone)]
pub struct SoftLimit(Arc<SoftLimitInner>);

impl SoftLimit {
    /// Makes a guard for one concurrent executing task.
    /// drop the guard for releasing the resource.
    pub async fn guard(&self) -> Result<SemaphorePermit<'_>> {
        Ok(self.0.semaphore.acquire().await?)
    }

    async fn take_tokens(&self, n: usize) -> Result<()> {
        self.0.semaphore.acquire_many(n as _).await?.forget();
        Ok(())
    }

    async fn grant_tokens(&self, n: usize) {
        self.0.semaphore.add_permits(n);
    }

    /// Shrinks the tasks can be executed concurrently by n
    /// would block until the quota applied.
    #[cfg(test)]
    pub async fn shrink(&self, n: usize) -> Result<()> {
        self.0.cap.fetch_sub(n, Ordering::SeqCst);
        self.take_tokens(n).await?;
        Ok(())
    }

    /// Grows the tasks can be executed concurrently by n
    #[cfg(test)]
    pub async fn grow(&self, n: usize) {
        self.0.cap.fetch_add(n, Ordering::SeqCst);
        self.grant_tokens(n).await;
    }

    /// resize the tasks available concurrently.
    pub async fn resize(&self, target: usize) -> Result<()> {
        let current = self.0.cap.swap(target, Ordering::SeqCst);
        match current.cmp(&target) {
            CmpOrder::Greater => {
                self.take_tokens(current - target).await?;
            }
            CmpOrder::Less => {
                self.grant_tokens(target - current).await;
            }
            _ => {}
        }
        Ok(())
    }

    /// returns the maxmium of tasks can be executed concurrently for now.
    pub fn current_cap(&self) -> usize {
        self.0.cap.load(Ordering::SeqCst)
    }

    /// make a new softlimit.
    pub fn new(initial_size: usize) -> Self {
        let semaphore = Semaphore::new(initial_size);
        let cap = AtomicUsize::new(initial_size);
        Self(Arc::new(SoftLimitInner { semaphore, cap }))
    }
}

pub trait CpuStatistics {
    type Container: IntoIterator<Item = (String, u64)>;
    // ThreadInfoStatistics needs &mut self to record the thread information.
    // RefCell(internal mutability) would make SoftLimitByCpu !Sync, hence futures contains it become !Send (WHY?)
    // Mutex would make this function async or blocking.
    // Anyway, &mut here is acceptable, since SoftLimitByCpu won't be shared. (Even the &mut here is a little weird...)
    fn get_cpu_usages(&mut self) -> Self::Container;
}

pub struct SoftLimitByCpu<Statistics> {
    pub(crate) metrics: Statistics,
    total_time: f64,
    keep_remain: usize,
}

impl CpuStatistics for ThreadInfoStatistics {
    type Container = HashMap<String, u64>;

    fn get_cpu_usages(&mut self) -> Self::Container {
        self.record();
        ThreadInfoStatistics::get_cpu_usages(self)
    }
}

impl<Statistics: CpuStatistics> SoftLimitByCpu<Statistics> {
    /// returns the current idle processor.
    /// **note that this might get inaccuracy if there are other processes
    /// running in the same CPU.**
    #[cfg(test)]
    fn current_idle(&mut self) -> f64 {
        self.current_idle_exclude(|_| false)
    }

    /// returns the current idle processor, ignoring threads with name matches the predicate.
    fn current_idle_exclude(&mut self, mut exclude: impl FnMut(&str) -> bool) -> f64 {
        let usages = self.metrics.get_cpu_usages();
        let used = usages
            .into_iter()
            .filter(|(s, _)| !exclude(s))
            .fold(0f64, |data, (_, usage)| data + (usage as f64 / 100f64));
        self.total_time - used
    }

    /// apply the limit to the soft limit according to the current CPU remaining.
    #[cfg(test)]
    pub async fn exec_over(&mut self, limit: &SoftLimit) -> Result<()> {
        self.exec_over_with_exclude(limit, |_| false).await
    }

    /// apply the limit to the soft limit according to the current CPU remaining.
    /// when calculating the CPU usage, ignore threads with name matched by the exclude predicate.
    /// This would keep at least one thread working.
    #[cfg(test)]
    pub async fn exec_over_with_exclude(
        &mut self,
        limit: &SoftLimit,
        exclude: impl FnMut(&str) -> bool,
    ) -> Result<()> {
        limit.resize(self.get_quota(exclude)).await?;
        Ok(())
    }

    /// get the current quota.
    pub fn get_quota(&mut self, exclude: impl FnMut(&str) -> bool) -> usize {
        let idle = self.current_idle_exclude(exclude) as usize;
        idle.saturating_sub(self.keep_remain).max(1)
    }

    /// set the keep_remain to the keeper.
    /// this applies to subsequent `exec_over` calls.
    pub fn set_remain(&mut self, remain: usize) {
        self.keep_remain = remain;
    }
}

impl SoftLimitByCpu<ThreadInfoStatistics> {
    pub fn with_remain(remain: usize) -> Self {
        let total = SysQuota::cpu_cores_quota();
        let metrics = ThreadInfoStatistics::new();
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

    use futures::Future;
    use rand::Rng;
    use tikv_util::sys::SysQuota;
    use tokio::{sync::mpsc::channel as async_channel, time};

    use super::{CpuStatistics, SoftLimit, SoftLimitByCpu};

    #[derive(Default)]
    struct TestCpuEnv {
        used: u64,
    }

    impl CpuStatistics for TestCpuEnv {
        type Container = Vec<(String, u64)>;

        fn get_cpu_usages(&mut self) -> Self::Container {
            let mut remain = self.used * 100;
            let mut result = vec![];
            // randomly make some threads, used the cpu of self.used%.
            for idx in 0.. {
                let thread_usage = rand::thread_rng().gen_range(50..100).min(remain);
                remain -= thread_usage;
                result.push((format!("worker-{}", idx), thread_usage));
                if remain == 0 {
                    break;
                }
            }
            result
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

    async fn should_finish_in(d: Duration, name: &str, f: impl Future + Send + 'static) {
        let (sx, mut rx) = async_channel(1);
        tokio::spawn(async move {
            f.await;
            sx.send(()).await.unwrap();
        });
        match time::timeout(d, rx.recv()).await {
            Ok(_) => {}
            Err(e) => panic!(
                "test failed: execution of {} timed out or disconnected. (timeout = {:?}, error = {})",
                name, d, e
            ),
        }
    }

    async fn should_satisfy_in(
        d: Duration,
        name: &str,
        mut f: impl FnMut() -> bool + Send + 'static,
    ) {
        should_finish_in(d, name, async move {
            while !f() {
                time::sleep(d / 50).await;
            }
        })
        .await
    }

    #[test]
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

    #[tokio::test]
    async fn test_limit() {
        let limit = SoftLimit::new(4);
        let working = Arc::new(AtomicU8::new(0));

        for _ in 0..4 {
            let working_cloned = working.clone();
            let limit_cloned = limit.clone();
            tokio::spawn(async move {
                loop {
                    let _guard = limit_cloned.guard().await.unwrap();
                    working_cloned.fetch_add(1, Ordering::SeqCst);
                    let backup_cost = rand::thread_rng().gen_range(10..100);
                    time::sleep(Duration::from_millis(backup_cost)).await;
                    working_cloned.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }

        let working_cloned = working.clone();
        let limit_cloned = limit.clone();
        limit_cloned.shrink(2).await.unwrap();
        should_satisfy_in(
            Duration::from_secs(10),
            "waiting for worker shrink to 2",
            move || working_cloned.load(Ordering::SeqCst) == 2,
        )
        .await;

        limit_cloned.grow(1).await;
        let working_cloned = working.clone();
        should_satisfy_in(
            Duration::from_secs(10),
            "waiting for worker grow to 3",
            move || working_cloned.load(Ordering::SeqCst) == 3,
        )
        .await;

        let working_cloned = working.clone();
        limit_cloned.grow(2).await;
        should_satisfy_in(
            Duration::from_secs(10),
            "waiting for worker grow to 4",
            move || working_cloned.load(Ordering::SeqCst) == 4,
        )
        .await;

        let working_cloned = working;
        limit_cloned.resize(1).await.unwrap();
        should_satisfy_in(
            Duration::from_secs(10),
            "waiting for worker shrink to 1",
            move || working_cloned.load(Ordering::SeqCst) == 1,
        )
        .await;
    }

    #[tokio::test]
    async fn test_cpu_limit() {
        let mut cpu_limit = TestCpuEnv::mock_busy(2);
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        cpu_limit.exec_over(&limit).await.unwrap();
        assert_eq!(
            limit.current_cap(),
            cpu_count - 2,
            "map = {:?}",
            cpu_limit.metrics.get_cpu_usages()
        );
        cpu_limit.metrics.stop_busy();
        cpu_limit.exec_over(&limit).await.unwrap();
        assert_eq!(limit.current_cap(), cpu_count);
    }

    #[tokio::test]
    async fn test_cpu_limit_remain() {
        let mut cpu_limit = TestCpuEnv::mock_busy(1);
        cpu_limit.set_remain(1);
        let cpu_count = SysQuota::cpu_cores_quota() as usize;
        let limit = SoftLimit::new(cpu_count);
        cpu_limit.exec_over(&limit).await.unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 2);
        cpu_limit.metrics.stop_busy();
        cpu_limit.exec_over(&limit).await.unwrap();
        assert_eq!(limit.current_cap(), cpu_count - 1);
    }
}
