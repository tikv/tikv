// Copyright 2017 PingCAP, Inc.
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

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGaugeVec, Opts, Result};

use tikv_alloc;

pub fn monitor_jemalloc_stats<S: Into<String>>(namespace: S) -> Result<()> {
    prometheus::register(Box::new(JemallocStatsCollector::new(namespace)?))
}

struct JemallocStatsCollector {
    descs: Vec<Desc>,
    metrics: IntGaugeVec,
}

impl JemallocStatsCollector {
    fn new<S: Into<String>>(namespace: S) -> Result<JemallocStatsCollector> {
        let stats = IntGaugeVec::new(
            Opts::new("jemalloc_stats", "Jemalloc stats").namespace(namespace.into()),
            &["type"],
        )?;
        Ok(JemallocStatsCollector {
            descs: stats.desc().into_iter().cloned().collect(),
            metrics: stats,
        })
    }
}

impl Collector for JemallocStatsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        if let Ok(stats) = tikv_alloc::fetch_stats() {
            self.metrics
                .with_label_values(&["allocated"])
                .set(stats.allocated as i64);
            self.metrics
                .with_label_values(&["active"])
                .set(stats.active as i64);
            self.metrics
                .with_label_values(&["metadata"])
                .set(stats.metadata as i64);
            self.metrics
                .with_label_values(&["resident"])
                .set(stats.resident as i64);
            self.metrics
                .with_label_values(&["mapped"])
                .set(stats.mapped as i64);
            self.metrics
                .with_label_values(&["retained"])
                .set(stats.retained as i64);
        }
        self.metrics.collect()
    }
}
