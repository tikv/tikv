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

use crate::pd::PdTask;
use crate::server::readpool::{self, Builder, ReadPool};
use crate::util::collections::HashMap;
use crate::util::worker::FutureScheduler;

use super::metrics::*;
use prometheus::local::*;

use crate::coprocessor::dag::executor::ExecutorMetrics;

thread_local! {
    pub static LOCAL_COPR_REQ_HISTOGRAM_VEC: RefCell<LocalHistogramVec> =
        RefCell::new(COPR_REQ_HISTOGRAM_VEC.local());
    pub static LOCAL_OUTDATED_REQ_WAIT_TIME: RefCell<LocalHistogramVec> =
        RefCell::new(OUTDATED_REQ_WAIT_TIME.local());
    pub static LOCAL_COPR_REQ_HANDLE_TIME: RefCell<LocalHistogramVec> =
        RefCell::new(COPR_REQ_HANDLE_TIME.local());
    pub static LOCAL_COPR_REQ_WAIT_TIME: RefCell<LocalHistogramVec> =
        RefCell::new(COPR_REQ_WAIT_TIME.local());

    pub static LOCAL_COPR_REQ_ERROR: RefCell<LocalIntCounterVec> =
        RefCell::new(COPR_REQ_ERROR.local());
    pub static LOCAL_COPR_SCAN_KEYS: RefCell<LocalHistogramVec> =
        RefCell::new(COPR_SCAN_KEYS.local());
    pub static LOCAL_COPR_SCAN_DETAILS: RefCell<LocalIntCounterVec> =
        RefCell::new(COPR_SCAN_DETAILS.local());
    pub static LOCAL_COPR_ROCKSDB_PERF_COUNTER: RefCell<LocalIntCounterVec> =
        RefCell::new(COPR_ROCKSDB_PERF_COUNTER.local());
    static LOCAL_COPR_EXECUTOR_COUNT: RefCell<LocalIntCounterVec> =
        RefCell::new(COPR_EXECUTOR_COUNT.local());
    static LOCAL_COPR_GET_OR_SCAN_COUNT: RefCell<LocalIntCounterVec> =
        RefCell::new(COPR_GET_OR_SCAN_COUNT.local());

    static LOCAL_COP_FLOW_STATS: RefCell<HashMap<u64, crate::storage::FlowStatistics>> =
        RefCell::new(HashMap::default());
}

pub struct ReadPoolImpl;

impl std::fmt::Debug for ReadPoolImpl {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("coprocessor::ReadPoolImpl").finish()
    }
}

impl ReadPoolImpl {
    pub fn build_read_pool(
        config: &readpool::Config,
        pd_sender: FutureScheduler<PdTask>,
        name_prefix: &str,
    ) -> ReadPool {
        let pd_sender2 = pd_sender.clone();

        Builder::from_config(config)
            .name_prefix(name_prefix)
            .on_tick(move || ReadPoolImpl::tls_flush(&pd_sender))
            .before_stop(move || ReadPoolImpl::tls_flush(&pd_sender2))
            .build()
    }

    #[inline]
    fn tls_flush(pd_sender: &FutureScheduler<PdTask>) {
        // Flush Prometheus metrics
        LOCAL_COPR_REQ_HISTOGRAM_VEC.with(|m| m.borrow_mut().flush());
        LOCAL_OUTDATED_REQ_WAIT_TIME.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_REQ_HANDLE_TIME.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_REQ_WAIT_TIME.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_REQ_ERROR.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_SCAN_KEYS.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_ROCKSDB_PERF_COUNTER.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_SCAN_DETAILS.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_GET_OR_SCAN_COUNT.with(|m| m.borrow_mut().flush());
        LOCAL_COPR_EXECUTOR_COUNT.with(|m| m.borrow_mut().flush());

        // Report PD metrics
        LOCAL_COP_FLOW_STATS.with(|local_cop_flow_stats| {
            if local_cop_flow_stats.borrow().is_empty() {
                // Stats to report to PD is empty, ignore.
                return;
            }

            let read_stats = local_cop_flow_stats.replace(HashMap::default());
            let result = pd_sender.schedule(PdTask::ReadStats { read_stats });
            if let Err(e) = result {
                error!("Failed to send cop pool read flow statistics"; "err" => ?e);
            }
        });
    }

    pub fn tls_collect_executor_metrics(region_id: u64, type_str: &str, metrics: ExecutorMetrics) {
        let stats = &metrics.cf_stats;
        // cf statistics group by type
        for (cf, details) in stats.details() {
            for (tag, count) in details {
                LOCAL_COPR_SCAN_DETAILS.with(|m| {
                    m.borrow_mut()
                        .with_label_values(&[type_str, cf, tag])
                        .inc_by(count as i64);
                });
            }
        }
        // flow statistics group by region
        ReadPoolImpl::tls_collect_read_flow(region_id, stats);

        // scan count
        let scan_counter = metrics.scan_counter;
        let executor_count = metrics.executor_count;
        LOCAL_COPR_GET_OR_SCAN_COUNT.with(|m| scan_counter.consume(&mut m.borrow_mut()));
        // exec count
        LOCAL_COPR_EXECUTOR_COUNT.with(|m| executor_count.consume(&mut m.borrow_mut()));
    }

    #[inline]
    pub fn tls_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
        LOCAL_COP_FLOW_STATS.with(|m| {
            let mut map = m.borrow_mut();
            let flow_stats = map
                .entry(region_id)
                .or_insert_with(crate::storage::FlowStatistics::default);
            flow_stats.add(&statistics.write.flow_stats);
            flow_stats.add(&statistics.data.flow_stats);
        });
    }
}
