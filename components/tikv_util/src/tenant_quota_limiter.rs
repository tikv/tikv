// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides util functions to manage share properties across threads.

use super::time::Limiter;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub struct ReadQuotaLimiter(Limiter);
pub struct WriteQuotaLimiter(Limiter);

impl ReadQuotaLimiter {
    // millicpu same as the k8s
    pub fn new(milli_cpu: u32) -> Self {
        Self(Limiter::new(milli_cpu as f64 * 1000_f64))
    }

    // Consume read cpu quota
    // If the quota is not enough, the returned duration will > 0, or return Duration::ZERO
    pub fn consume_read(&self, time_slice_micro_secs: u32) -> Duration {
        self.0.consume_duration(time_slice_micro_secs as usize)
    }
}

impl WriteQuotaLimiter {
    pub fn new(speed_limit: u64) -> Self {
        Self(Limiter::new(speed_limit as f64))
    }

    // Consume write bytes quota
    // If the quota is not enough, the returned duration will > 0, or return Duration::ZERO
    pub fn consume_write(&self, write_bytes: u64) -> Duration {
        self.0.consume_duration(write_bytes as usize)
    }
}

pub struct TenantQuotaLimiter {
    // tenant_id -> limiter
    tenant_write_limiters: RwLock<HashMap<u32, Arc<WriteQuotaLimiter>>>,
    tenant_read_limiters: RwLock<HashMap<u32, Arc<ReadQuotaLimiter>>>,
    quota_is_updating: AtomicBool,
}

impl Default for TenantQuotaLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantQuotaLimiter {
    pub fn new() -> Self {
        Self {
            tenant_write_limiters: RwLock::new(Default::default()),
            tenant_read_limiters: RwLock::new(Default::default()),
            quota_is_updating: AtomicBool::new(false),
        }
    }

    pub fn get_read_quota_limiter(&self, tenant_id: u32) -> Option<Arc<ReadQuotaLimiter>> {
        if let Some(rlimiter) = self.tenant_read_limiters.read().unwrap().get(&tenant_id) {
            return Some(Arc::clone(rlimiter));
        }
        None
    }

    pub fn get_write_quota_limiter(&self, tenant_id: u32) -> Option<Arc<WriteQuotaLimiter>> {
        if let Some(wlimiter) = self.tenant_write_limiters.read().unwrap().get(&tenant_id) {
            return Some(Arc::clone(wlimiter));
        }
        None
    }

    // Consume write bytes quota
    // if quota is not enough, the returned duration will > 0, or return Duration::ZERO
    pub fn consume_write(&self, tenant_id: u32, write_bytes: u64) -> Duration {
        if let Some(limiter) = self.tenant_write_limiters.read().unwrap().get(&tenant_id) {
            return limiter.as_ref().consume_write(write_bytes);
        }
        Duration::ZERO
    }

    // Consume read cpu quota
    // if quota is not enough, the returned duration will > 0, or return Duration::ZERO
    pub fn consume_read(&self, tenant_id: u32, time_slice_micro_secs: u32) -> Duration {
        if let Some(limiter) = self.tenant_read_limiters.read().unwrap().get(&tenant_id) {
            return limiter.as_ref().consume_read(time_slice_micro_secs);
        }
        Duration::ZERO
    }

    // Refresh quota if there are some changes
    // tenant_quotas: tenant_id -> (write_bytes_per_sec, read_milli_cpu)
    pub fn refresh_quota(&self, tenant_quotas: Vec<(u32, (u64, u32))>) {
        if self
            .quota_is_updating
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Acuire read lock to check if there is any change
            let mut idxs_write = vec![];
            let mut idxs_read = vec![];
            {
                let wlimiters = self.tenant_write_limiters.read().unwrap();
                let rlimiters = self.tenant_read_limiters.read().unwrap();
                for (idx, quota) in tenant_quotas.iter().enumerate() {
                    if let Some(l) = wlimiters.get(&quota.0) {
                        if l.0.speed_limit() as u64 != quota.1.0 {
                            idxs_write.push(idx);
                        }
                    } else if quota.1.0 > 0 {
                        idxs_write.push(idx);
                    }

                    if let Some(l) = rlimiters.get(&quota.0) {
                        if l.0.speed_limit() as u64 != quota.1.1 as u64 * 1000 {
                            idxs_read.push(idx)
                        }
                    } else if quota.1.1 > 0 {
                        idxs_read.push(idx);
                    }
                }
            }
            // Acquire write lock to update changes
            if !idxs_write.is_empty() {
                let mut wlimiters = self.tenant_write_limiters.write().unwrap();
                for idx in idxs_write {
                    let quota = tenant_quotas[idx];
                    let limiter = wlimiters
                        .entry(quota.0)
                        .or_insert_with(|| Arc::new(WriteQuotaLimiter::new(quota.1.0)));
                    if quota.1.0 == 0 {
                        (*limiter).0.set_speed_limit(f64::INFINITY);
                    } else {
                        (*limiter).0.set_speed_limit(quota.1.0 as f64);
                    }
                }
            }
            if !idxs_read.is_empty() {
                let mut rlimiters = self.tenant_read_limiters.write().unwrap();
                for idx in idxs_read {
                    let quota = tenant_quotas[idx];
                    let limiter = rlimiters
                        .entry(quota.0)
                        .or_insert_with(|| Arc::new(ReadQuotaLimiter::new(quota.1.1)));
                    if quota.1.1 == 0 {
                        (*limiter).0.set_speed_limit(f64::INFINITY);
                    } else {
                        (*limiter).0.set_speed_limit(quota.1.1 as f64 * 1000_f64);
                    }
                }
            }

            self.quota_is_updating.store(false, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_quota() {
        let quota_limiter = TenantQuotaLimiter::new();
        let quota = vec![(1, (100, 200)), (2, (300, 400))];
        quota_limiter.refresh_quota(quota);

        let wlimiter1 = quota_limiter.get_write_quota_limiter(1).unwrap();
        assert!((wlimiter1.0.speed_limit() - 100_f64).abs() < f64::EPSILON);
        let rlimiter1 = quota_limiter.get_read_quota_limiter(1).unwrap();
        assert!((rlimiter1.0.speed_limit() - 200_f64 * 1000_f64).abs() < f64::EPSILON);

        let wlimiter2 = quota_limiter.get_write_quota_limiter(2).unwrap();
        assert!((wlimiter2.0.speed_limit() - 300_f64).abs() < f64::EPSILON);
        let rlimiter2 = quota_limiter.get_read_quota_limiter(2).unwrap();
        assert!((rlimiter2.0.speed_limit() - 400_f64 * 1000_f64).abs() < f64::EPSILON);

        assert!(quota_limiter.get_write_quota_limiter(3).is_none());
        assert!(quota_limiter.get_read_quota_limiter(3).is_none());
    }
}
