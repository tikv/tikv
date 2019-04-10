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

use crate::pd;
use crate::util::futurepool;
use crate::util::worker;

use super::dag::executor::ExecutorMetrics;
use super::local_metrics::{BasicLocalMetrics, ExecLocalMetrics};

pub struct Context {
    // TODO: ExecLocalMetrics can be merged into this file.
    pub exec_local_metrics: ExecLocalMetrics,
    pub basic_local_metrics: BasicLocalMetrics,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("coprocessor::ReadPoolContext").finish()
    }
}

impl Context {
    pub fn new(pd_sender: worker::FutureScheduler<pd::PdTask>) -> Self {
        Context {
            exec_local_metrics: ExecLocalMetrics::new(pd_sender),
            basic_local_metrics: BasicLocalMetrics::default(),
        }
    }

    pub fn collect(&mut self, region_id: u64, scan_tag: &str, metrics: ExecutorMetrics) {
        self.exec_local_metrics
            .collect(scan_tag, region_id, metrics);
    }
}

impl futurepool::Context for Context {
    fn on_tick(&mut self) {
        self.exec_local_metrics.flush();
        self.basic_local_metrics.flush();
    }
}
