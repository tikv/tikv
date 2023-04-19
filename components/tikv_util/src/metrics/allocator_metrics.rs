// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::{
    core::{Collector, Desc},
    proto::MetricFamily,
    IntGaugeVec, Opts, Result,
};

pub fn monitor_allocator_stats<S: Into<String>>(namespace: S) -> Result<()> {
    prometheus::register(Box::new(AllocStatsCollector::new(namespace)?))
}

struct AllocStatsCollector {
    descs: Vec<Desc>,
    metrics: IntGaugeVec,
}

impl AllocStatsCollector {
    fn new<S: Into<String>>(namespace: S) -> Result<AllocStatsCollector> {
        let stats = IntGaugeVec::new(
            Opts::new("allocator_stats", "Allocator stats").namespace(namespace.into()),
            &["type"],
        )?;
        Ok(AllocStatsCollector {
            descs: stats.desc().into_iter().cloned().collect(),
            metrics: stats,
        })
    }
}

impl Collector for AllocStatsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        if let Ok(Some(stats)) = tikv_alloc::fetch_stats() {
            for stat in stats {
                self.metrics.with_label_values(&[stat.0]).set(stat.1 as i64);
            }
        }
        self.metrics.collect()
    }
}
