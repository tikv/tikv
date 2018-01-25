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
use prometheus::local::{LocalCounterVec, LocalHistogramTimer, LocalHistogramVec};

use util::futurepool;
use util::worker;
use pd;
use storage;

use super::metrics::*;

pub struct Context {
    pd_sender: Option<worker::FutureScheduler<pd::PdTask>>,

    command_duration: LocalHistogramVec,
    // keyread_duration: LocalHistogramVec,
    command_counter: LocalCounterVec,
    kvscan_counter: LocalCounterVec,
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
            // keyread_duration: KEYREAD_HISTOGRAM_VEC.local(),
            command_counter: COMMAND_COUNTER_VEC.local(),
            kvscan_counter: KV_SCAN_COUNTER_VEC.local(),
        }
    }

    #[inline]
    pub fn collect_command_duration(&mut self, cmd: &str) -> LocalHistogramTimer {
        self.command_duration
            .with_label_values(&[cmd])
            .start_coarse_timer()
    }

    #[inline]
    pub fn collect_command_count(&mut self, cmd: &str, priority: super::Priority) {
        self.command_counter
            .with_label_values(&[cmd, &priority.to_string()])
            .inc();
    }

    #[inline]
    pub fn collect_kvscan_count(&mut self, cmd: &str, statistics: &storage::Statistics) {
        for (cf, details) in statistics.details() {
            for (tag, count) in details {
                self.kvscan_counter
                    .with_label_values(&[cmd, cf, tag])
                    .inc_by(count as f64)
                    .unwrap();
            }
        }
    }
}

impl futurepool::Context for Context {
    fn on_tick(&mut self) {
        self.command_duration.flush();
        // self.keyread_duration.flush();
        self.command_counter.flush();
        self.kvscan_counter.flush();
    }
}
