// Copyright 2016 PingCAP, Inc.
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

use log::LogLevel;
use cadence::prelude::{Counted, Timed, Gauged, Metered};
use cadence::{StatsdClient, LoggingMetricSink};

pub struct Metric {
    inner: StatsdClient<LoggingMetricSink>,
}

impl Metric {
    pub fn new(prefix: &str, level: LogLevel) -> Metric {
        let sink = LoggingMetricSink::new(level);
        Metric { inner: StatsdClient::from_sink(prefix, sink) }
    }

    // Increment the counter by `1`
    pub fn incr(&self, key: &str) {
        if let Err(e) = self.inner.incr(key) {
            warn!("{}", e);
        }
    }

    // Decrement the counter by `1`
    pub fn decr(&self, key: &str) {
        if let Err(e) = self.inner.decr(key) {
            warn!("{}", e);
        }
    }

    // Increment or decrement the counter by the given amount
    pub fn count(&self, key: &str, count: i64) {
        if let Err(e) = self.inner.count(key, count) {
            warn!("{}", e);
        }
    }

    // Record a  timing in milliseconds with the given key
    pub fn time(&self, key: &str, time: u64) {
        if let Err(e) = self.inner.time(key, time) {
            warn!("{}", e);
        }
    }

    // Record a gauge value with the given key
    pub fn gauge(&self, key: &str, value: u64) {
        if let Err(e) = self.inner.gauge(key, value) {
            warn!("{}", e);
        }
    }

    // Record a single metered event with the given key
    pub fn mark(&self, key: &str) {
        if let Err(e) = self.inner.mark(key) {
            warn!("{}", e);
        }
    }

    // Record a meter value with the given key
    pub fn meter(&self, key: &str, value: u64) {
        if let Err(e) = self.inner.meter(key, value) {
            warn!("{}", e);
        }
    }
}
