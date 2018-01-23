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
use prometheus::local::LocalHistogramVec;

use util::futurepool;
use util::worker;
use pd;

use super::metrics::*;

pub struct Context {
    pd_sender: Option<worker::FutureScheduler<pd::PdTask>>,

    pub command_duration: LocalHistogramVec,
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
        }
    }
}

impl futurepool::Context for Context {
    fn on_tick(&mut self) {
        self.command_duration.flush();
    }
}
