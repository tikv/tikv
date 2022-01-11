// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides util functions to manage share properties across threads.

use super::time::Limiter;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Copy, Clone)]
pub struct TenantQuota {
    cputime_quota: usize,
    bandwidth_quota: usize,
}

impl TenantQuota {
    pub fn new(cputime_quota: usize, bandwidth_quota: usize) -> Self {
        Self {
            cputime_quota,
            bandwidth_quota,
        }
    }
}

pub enum QType {
    KvGet,
    CoprScan,
    Others,
}

pub struct QuotaLimiter {
    tenant_id: u32,
    cputime_limiter: Limiter,
    bandwidth_limiter: Limiter,
}

impl QuotaLimiter {
    // 1000 millicpu equals to 1vCPU, 0 means unlimited
    pub fn new(tenant_id: u32, milli_cpu: usize, bandwidth: usize) -> Self {
        let cputime_limiter = if milli_cpu == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            // transfer milli cpu to micro cpu
            Limiter::new(milli_cpu as f64 * 1000_f64)
        };
        let bandwidth_limiter = if bandwidth == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::new(bandwidth as f64)
        };
        Self {
            tenant_id,
            cputime_limiter,
            bandwidth_limiter,
        }
    }

    pub fn consume_write(&self, req_cnt: usize, kv_cnt: usize, bytes: usize) -> Duration {
        let cost_micro_cpu: usize = req_cnt * 100 + kv_cnt * 50;
        let cpu_dur = self.cputime_limiter.consume_duration(cost_micro_cpu);
        let bw_dur = self.bandwidth_limiter.consume_duration(bytes);
        cpu_dur + bw_dur
    }

    pub fn consume_read(
        &self,
        time_micro_secs: usize,
        bytes: usize,
        query_type: QType,
    ) -> Duration {
        let cpu_dur = match query_type {
            QType::KvGet => self
                .cputime_limiter
                .consume_duration(time_micro_secs as usize + 50),
            QType::CoprScan => self
                .cputime_limiter
                .consume_duration(time_micro_secs as usize + 30),
            _ => self
                .cputime_limiter
                .consume_duration(time_micro_secs as usize),
        };
        let bw_dur = self.bandwidth_limiter.consume_duration(bytes);
        cpu_dur + bw_dur
    }
}

pub struct TenantQuotaLimiter {
    // tenant_id -> limiter
    tenant_quota_limiters: RwLock<HashMap<u32, Arc<QuotaLimiter>>>,
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
            tenant_quota_limiters: RwLock::new(Default::default()),
            quota_is_updating: AtomicBool::new(false),
        }
    }

    pub fn get_quota_limiter(&self, tenant_id: u32) -> Option<Arc<QuotaLimiter>> {
        if let Some(limiter) = self.tenant_quota_limiters.read().unwrap().get(&tenant_id) {
            return Some(Arc::clone(limiter));
        }
        None
    }

    // Refresh quota if there are some changes
    // tenant_quotas: tenant_id -> (millicpu_quota, bandwidth_quota)
    pub fn refresh_quota(&self, tenant_quotas: Vec<(u32, TenantQuota)>) {
        if self
            .quota_is_updating
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Acuire read lock to check if there is any change
            let mut tenants = HashSet::<u32>::default();
            let mut idxs_update = vec![];
            {
                let limiters = self.tenant_quota_limiters.read().unwrap();
                for (idx, quota) in tenant_quotas.iter().enumerate() {
                    match limiters.get(&quota.0) {
                        Some(limiter) => {
                            if limiter.cputime_limiter.speed_limit() as usize
                                != quota.1.cputime_quota
                                || limiter.bandwidth_limiter.speed_limit() as usize
                                    != quota.1.bandwidth_quota
                            {
                                idxs_update.push(idx);
                            }
                        }
                        None => {
                            if quota.1.cputime_quota > 0 || quota.1.bandwidth_quota > 0 {
                                idxs_update.push(idx)
                            }
                        }
                    }
                    tenants.insert(quota.0);
                }
            }
            // Acquire write lock to update changes
            if !idxs_update.is_empty() {
                let mut limiters = self.tenant_quota_limiters.write().unwrap();
                for idx in idxs_update {
                    let quota = tenant_quotas[idx];
                    let limiter = limiters.entry(quota.0).or_insert_with(|| {
                        Arc::new(QuotaLimiter::new(
                            quota.0,
                            quota.1.cputime_quota,
                            quota.1.bandwidth_quota,
                        ))
                    });
                    // update cputime quota
                    if quota.1.cputime_quota == 0 {
                        (*limiter).cputime_limiter.set_speed_limit(f64::INFINITY);
                    } else {
                        (*limiter)
                            .cputime_limiter
                            .set_speed_limit(quota.1.cputime_quota as f64 * 1000_f64);
                    }
                    // update bandwidth quota
                    if quota.1.bandwidth_quota == 0 {
                        (*limiter).bandwidth_limiter.set_speed_limit(f64::INFINITY);
                    } else {
                        (*limiter)
                            .bandwidth_limiter
                            .set_speed_limit(quota.1.bandwidth_quota as f64);
                    }
                }

                // Remove deleted tenant quota
                let mut removed_tenants = vec![];
                if limiters.len() != tenants.len() {
                    for key in limiters.keys() {
                        if !tenants.contains(key) {
                            removed_tenants.push(*key);
                        }
                    }
                }
                for tenant in removed_tenants {
                    limiters.remove(&tenant);
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
        let quota = vec![
            (1, TenantQuota::new(100, 200)),
            (2, TenantQuota::new(300, 400)),
        ];
        quota_limiter.refresh_quota(quota);

        let limiter1 = quota_limiter.get_quota_limiter(1).unwrap();
        assert!((limiter1.cputime_limiter.speed_limit() - 100_f64 * 1000_f64).abs() < f64::EPSILON);
        assert!((limiter1.bandwidth_limiter.speed_limit() - 200_f64).abs() < f64::EPSILON);

        let limiter2 = quota_limiter.get_quota_limiter(2).unwrap();
        assert!((limiter2.cputime_limiter.speed_limit() - 300_f64 * 1000_f64).abs() < f64::EPSILON);
        assert!((limiter2.bandwidth_limiter.speed_limit() - 400_f64).abs() < f64::EPSILON);

        assert!(quota_limiter.get_quota_limiter(3).is_none());

        // remove tenant2, update tenant1
        let quota = vec![(1, TenantQuota::new(200, 400))];
        quota_limiter.refresh_quota(quota);

        assert!(quota_limiter.get_quota_limiter(2).is_none());

        let limiter1 = quota_limiter.get_quota_limiter(1).unwrap();
        assert!((limiter1.cputime_limiter.speed_limit() - 200_f64 * 1000_f64).abs() < f64::EPSILON);
        assert!((limiter1.bandwidth_limiter.speed_limit() - 400_f64).abs() < f64::EPSILON);
    }
}
