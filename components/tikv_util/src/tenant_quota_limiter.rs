// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides util functions to manage share properties across threads.

use super::time::Limiter;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::time::Duration;

pub struct TenantQuotaLimiter {
    // tenant_id -> limiter
    tenant_write_limiters: RwLock<HashMap<u32, Limiter>>,
    tenant_read_limiters: RwLock<HashMap<u32, Limiter>>,
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

    // Consume write bytes quota
    // if quota is not enough, the returned duration will > 0, or return Duration::ZERO
    pub fn consume_write(&self, tenant_id: u32, write_bytes: u64) -> Duration {
        if let Some(limiter) = self.tenant_write_limiters.read().unwrap().get(&tenant_id) {
            return limiter.consume_duration(write_bytes as usize);
        }
        Duration::ZERO
    }

    // Consume read cpu quota
    // if quota is not enough, the returned duration will > 0, or return Duration::ZERO
    pub fn consume_read(&self, tenant_id: u32, time_slice_micro_secs: u32) -> Duration {
        if let Some(limiter) = self.tenant_read_limiters.read().unwrap().get(&tenant_id) {
            return limiter.consume_duration(time_slice_micro_secs as usize);
        }
        Duration::ZERO
    }

    // Update quota if there are some changes
    // tenant_quotas: tenant_id -> (write_bytes_per_sec, read_milli_cpu)
    pub fn refresh_quota(&mut self, tenant_quotas: Vec<(u32, (u64, u32))>) {
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
                        if l.speed_limit() as u64 != quota.1.0 {
                            idxs_write.push(idx);
                        }
                    } else if quota.1.0 > 0 {
                        idxs_write.push(idx);
                    }

                    if let Some(l) = rlimiters.get(&quota.0) {
                        if l.speed_limit() as u64 != quota.1.1 as u64 * 1000 {
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
                        .or_insert_with(|| Limiter::new(quota.1.0 as f64));
                    (*limiter).set_speed_limit(quota.1.0 as f64);
                }
            }
            if !idxs_read.is_empty() {
                let mut rlimiters = self.tenant_read_limiters.write().unwrap();
                for idx in idxs_read {
                    let quota = tenant_quotas[idx];
                    let limiter = rlimiters
                        .entry(quota.0)
                        .or_insert_with(|| Limiter::new(quota.1.0 as f64 * 1000_f64));
                    (*limiter).set_speed_limit(quota.1.0 as f64 * 1000_f64);
                }
            }

            self.quota_is_updating.store(false, Ordering::SeqCst);
        }
    }
}
