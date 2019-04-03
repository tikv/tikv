// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::time::Duration;

use prometheus::local::*;

use crate::pd::PdTask;
use crate::server::readpool::{self, Builder, ReadPool};
use crate::util::collections::HashMap;
use crate::util::worker::FutureScheduler;

use super::metrics::*;

thread_local! {
    static LOCAL_SCHED_HISTOGRAM_VEC: RefCell<LocalHistogramVec> =
        RefCell::new(SCHED_HISTOGRAM_VEC.local());

    static LOCAL_SCHED_PROCESSING_READ_HISTOGRAM_VEC: RefCell<LocalHistogramVec> =
        RefCell::new(SCHED_PROCESSING_READ_HISTOGRAM_VEC.local());

    static LOCAL_KV_COMMAND_KEYREAD_HISTOGRAM_VEC: RefCell<LocalHistogramVec> =
        RefCell::new(KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local());

    static LOCAL_KV_COMMAND_COUNTER_VEC: RefCell<LocalIntCounterVec> =
        RefCell::new(KV_COMMAND_COUNTER_VEC.local());

    static LOCAL_SCHED_COMMANDS_PRI_COUNTER_VEC: RefCell<LocalIntCounterVec> =
        RefCell::new(SCHED_COMMANDS_PRI_COUNTER_VEC.local());

    static LOCAL_KV_COMMAND_SCAN_DETAILS: RefCell<LocalIntCounterVec> =
        RefCell::new(KV_COMMAND_SCAN_DETAILS.local());

    static LOCAL_PD_SENDER: RefCell<Option<FutureScheduler<PdTask>>> =
        RefCell::new(None);

    static LOCAL_READ_FLOW_STATS: RefCell<HashMap<u64, crate::storage::FlowStatistics>> =
        RefCell::new(HashMap::default());
}

pub struct ReadPoolImpl;

impl ReadPoolImpl {
    #[inline]
    pub fn build_read_pool(
        config: &readpool::Config,
        pd_sender: FutureScheduler<PdTask>,
        name_prefix: &str,
    ) -> ReadPool {
        let pd_sender2 = pd_sender.clone();

        Builder::from_config(config)
            .name_prefix(name_prefix)
            .on_tick(move || ReadPoolImpl::thread_local_flush(&pd_sender))
            .before_stop(move || ReadPoolImpl::thread_local_flush(&pd_sender2))
            .build()
    }

    #[inline]
    fn thread_local_flush(pd_sender: &FutureScheduler<PdTask>) {
        // Flush Prometheus metrics
        LOCAL_SCHED_HISTOGRAM_VEC.with(|m| m.borrow_mut().flush());
        LOCAL_SCHED_PROCESSING_READ_HISTOGRAM_VEC.with(|m| m.borrow_mut().flush());
        LOCAL_KV_COMMAND_KEYREAD_HISTOGRAM_VEC.with(|m| m.borrow_mut().flush());
        LOCAL_KV_COMMAND_COUNTER_VEC.with(|m| m.borrow_mut().flush());
        LOCAL_SCHED_COMMANDS_PRI_COUNTER_VEC.with(|m| m.borrow_mut().flush());
        LOCAL_KV_COMMAND_SCAN_DETAILS.with(|m| m.borrow_mut().flush());

        // Report PD metrics
        LOCAL_READ_FLOW_STATS.with(|local_read_flow_stats| {
            if local_read_flow_stats.borrow().is_empty() {
                // Stats to report to PD is empty, ignore.
                return;
            }

            let read_stats = local_read_flow_stats.replace(HashMap::default());
            let result = pd_sender.schedule(PdTask::ReadStats { read_stats });
            if let Err(e) = result {
                error!("Failed to send read pool read flow statistics"; "err" => ?e);
            }
        });
    }

    #[inline]
    pub fn thread_local_collect_command_count(cmd: &str, priority: readpool::Priority) {
        LOCAL_KV_COMMAND_COUNTER_VEC.with(|m| m.borrow_mut().with_label_values(&[cmd]).inc());
        LOCAL_SCHED_COMMANDS_PRI_COUNTER_VEC
            .with(|m| m.borrow_mut().with_label_values(&[priority.as_str()]).inc());
    }

    #[inline]
    pub fn thread_local_collect_command_duration(cmd: &str, duration: Duration) {
        LOCAL_SCHED_HISTOGRAM_VEC.with(|m| {
            m.borrow_mut()
                .with_label_values(&[cmd])
                .observe(crate::util::time::duration_to_sec(duration))
        });
    }

    #[inline]
    pub fn thread_local_collect_key_reads(cmd: &str, count: usize) {
        LOCAL_KV_COMMAND_KEYREAD_HISTOGRAM_VEC.with(|m| {
            m.borrow_mut()
                .with_label_values(&[cmd])
                .observe(count as f64)
        });
    }

    #[inline]
    pub fn thread_local_processing_read_observe_duration<F, R>(cmd: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        LOCAL_SCHED_PROCESSING_READ_HISTOGRAM_VEC.with(|m| {
            let now = crate::util::time::Instant::now_coarse();
            let ret = f();
            m.borrow_mut()
                .with_label_values(&[cmd])
                .observe(now.elapsed_secs());
            ret
        })
    }

    #[inline]
    pub fn thread_local_collect_scan_count(cmd: &str, statistics: &crate::storage::Statistics) {
        LOCAL_KV_COMMAND_SCAN_DETAILS.with(|m| {
            let mut histogram = m.borrow_mut();
            for (cf, details) in statistics.details() {
                for (tag, count) in details {
                    histogram
                        .with_label_values(&[cmd, cf, tag])
                        .inc_by(count as i64);
                }
            }
        });
    }

    #[inline]
    pub fn thread_local_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
        LOCAL_READ_FLOW_STATS.with(|m| {
            let mut map = m.borrow_mut();
            let flow_stats = map
                .entry(region_id)
                .or_insert_with(crate::storage::FlowStatistics::default);
            flow_stats.add(&statistics.write.flow_stats);
            flow_stats.add(&statistics.data.flow_stats);
        });
    }
}
