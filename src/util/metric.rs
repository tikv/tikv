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
        let r = self.inner.incr(key);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }

    // Decrement the counter by `1`
    pub fn decr(&self, key: &str) {
        let r = self.inner.decr(key);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }

    // Increment or decrement the counter by the given amount
    pub fn count(&self, key: &str, count: i64) {
        let r = self.inner.count(key, count);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }

    // Record a  timing in milliseconds with the given key
    pub fn time(&self, key: &str, time: u64) {
        let r = self.inner.time(key, time);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }

    // Record a gauge value with the given key
    pub fn gauge(&self, key: &str, value: u64) {
        let r = self.inner.gauge(key, value);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }

    // Record a single metered event with the given key
    pub fn mark(&self, key: &str) {
        let r = self.inner.mark(key);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }

    // Record a meter value with the given key
    pub fn meter(&self, key: &str, value: u64) {
        let r = self.inner.meter(key, value);
        match r {
            Err(e) => warn!("{}", e),
            _ => {}
        }
    }
}
