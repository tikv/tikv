// Copyright 2018 PingCAP, Inc.
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

use prometheus::local::{LocalHistogramTimer, LocalHistogramVec, LocalIntCounterVec};
use std::fmt;
use std::mem;

use pd;
use server::readpool;
use storage;
use util::collections::HashMap;
use util::futurepool;
use util::worker;

use super::metrics::*;

pub struct Context {
    pd_sender: worker::FutureScheduler<pd::PdTask>,

    // TODO: command_duration, processing_read_duration, command_counter can be merged together.
    command_duration: LocalHistogramVec,
    processing_read_duration: LocalHistogramVec,
    command_keyreads: LocalHistogramVec,
    // TODO: command_counter, command_pri_counter can be merged together.
    command_counter: LocalIntCounterVec,
    command_pri_counter: LocalIntCounterVec,
    scan_details: LocalIntCounterVec,

    read_flow_stats: HashMap<u64, storage::FlowStatistics>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("storage::ReadPoolContext").finish()
    }
}

impl Context {
    pub fn new(pd_sender: worker::FutureScheduler<pd::PdTask>) -> Self {
        Context {
            pd_sender,
            command_duration: SCHED_HISTOGRAM_VEC.local(),
            processing_read_duration: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            command_keyreads: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            command_counter: KV_COMMAND_COUNTER_VEC.local(),
            command_pri_counter: SCHED_COMMANDS_PRI_COUNTER_VEC.local(),
            scan_details: KV_COMMAND_SCAN_DETAILS.local(),
            read_flow_stats: HashMap::default(),
        }
    }

    #[inline]
    pub fn start_command_duration_timer(
        &mut self,
        cmd: &str,
        priority: readpool::Priority,
    ) -> LocalHistogramTimer {
        self.command_counter.with_label_values(&[cmd]).inc();
        self.command_pri_counter
            .with_label_values(&[&priority.to_string()])
            .inc();
        self.command_duration
            .with_label_values(&[cmd])
            .start_coarse_timer()
    }

    #[inline]
    pub fn start_processing_read_duration_timer(&mut self, cmd: &str) -> LocalHistogramTimer {
        self.processing_read_duration
            .with_label_values(&[cmd])
            .start_coarse_timer()
    }

    #[inline]
    pub fn collect_key_reads(&mut self, cmd: &str, count: u64) {
        self.command_keyreads
            .with_label_values(&[cmd])
            .observe(count as f64);
    }

    #[inline]
    pub fn collect_scan_count(&mut self, cmd: &str, statistics: &storage::Statistics) {
        for (cf, details) in statistics.details() {
            for (tag, count) in details {
                self.scan_details
                    .with_label_values(&[cmd, cf, tag])
                    .inc_by(count as i64);
            }
        }
    }

    #[inline]
    pub fn collect_read_flow(&mut self, region_id: u64, statistics: &storage::Statistics) {
        let flow_stats = self
            .read_flow_stats
            .entry(region_id)
            .or_insert_with(storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    }
}

impl futurepool::Context for Context {
    fn on_tick(&mut self) {
        // Flush Prometheus metrics
        self.command_duration.flush();
        self.processing_read_duration.flush();
        self.command_keyreads.flush();
        self.command_counter.flush();
        self.command_pri_counter.flush();
        self.scan_details.flush();

        // Report PD metrics
        if !self.read_flow_stats.is_empty() {
            let mut read_stats = HashMap::default();
            mem::swap(&mut read_stats, &mut self.read_flow_stats);
            let result = self
                .pd_sender
                .schedule(pd::PdTask::ReadStats { read_stats });
            if let Err(e) = result {
                error!("Failed to send readpool read flow statistics: {:?}", e);
            }
        }
    }
}
