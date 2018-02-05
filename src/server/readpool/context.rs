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

use std::fmt;
use std::mem;
use prometheus::local::{LocalCounterVec, LocalHistogramTimer, LocalHistogramVec};

use util::futurepool;
use util::worker;
use util::collections::HashMap;
use pd;
use storage;

use super::metrics::*;

pub struct Context {
    pd_sender: Option<worker::FutureScheduler<pd::PdTask>>,

    command_duration: LocalHistogramVec,
    command_counter: LocalCounterVec,
    key_reads: LocalHistogramVec,
    scan_counter: LocalCounterVec,

    read_flow_stats: HashMap<u64, storage::FlowStatistics>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context").finish()
    }
}

impl Context {
    pub fn new(pd_sender: Option<worker::FutureScheduler<pd::PdTask>>) -> Self {
        Context {
            pd_sender,
            command_duration: COMMAND_HISTOGRAM_VEC.local(),
            command_counter: COMMAND_COUNTER_VEC.local(),
            key_reads: KEY_READ_HISTOGRAM_VEC.local(),
            scan_counter: SCAN_COUNTER_VEC.local(),
            read_flow_stats: HashMap::default(),
        }
    }

    #[inline]
    pub fn collect_command_duration(
        &mut self,
        cmd: &str,
        cmd_type: &str,
        stage: &str,
    ) -> LocalHistogramTimer {
        self.command_duration
            .with_label_values(&[cmd, cmd_type, stage])
            .start_coarse_timer()
    }

    #[inline]
    pub fn collect_command_count(&mut self, cmd: &str, cmd_type: &str, priority: super::Priority) {
        self.command_counter
            .with_label_values(&[cmd, cmd_type, &priority.to_string()])
            .inc();
    }

    #[inline]
    pub fn collect_key_reads(&mut self, cmd: &str, cmd_type: &str, count: u64) {
        self.key_reads
            .with_label_values(&[cmd, cmd_type])
            .observe(count as f64);
    }

    #[inline]
    pub fn collect_scan_count(
        &mut self,
        cmd: &str,
        cmd_type: &str,
        statistics: &storage::Statistics,
    ) {
        for (cf, details) in statistics.details() {
            for (tag, count) in details {
                self.scan_counter
                    .with_label_values(&[cmd, cmd_type, cf, tag])
                    .inc_by(count as f64)
                    .unwrap();
            }
        }
    }

    #[inline]
    pub fn collect_read_flow(&mut self, region_id: u64, statistics: &storage::Statistics) {
        if self.pd_sender.is_none() {
            return;
        }
        let flow_stats = self.read_flow_stats
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
        self.command_counter.flush();
        self.key_reads.flush();
        self.scan_counter.flush();

        // Report PD metrics
        if !self.read_flow_stats.is_empty() {
            if let Some(ref sender) = self.pd_sender {
                let mut read_stats = HashMap::default();
                mem::swap(&mut read_stats, &mut self.read_flow_stats);
                let result = sender.schedule(pd::PdTask::ReadStats { read_stats });
                if let Err(e) = result {
                    error!("Failed to send readpool read flow statistics: {:?}", e);
                }
            }
        }
    }
}
