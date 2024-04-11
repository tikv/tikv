// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::{
    core::{Collector, Desc},
    proto::MetricFamily,
    IntGauge, IntGaugeVec, Opts, Result,
};

pub fn monitor_allocator_stats<S: Into<String>>(namespace: S) -> Result<()> {
    prometheus::register(Box::new(AllocStatsCollector::new(namespace)?))
}

struct AllocStatsCollector {
    descs: Vec<Desc>,
    memory_stats: IntGaugeVec,
    thread_stats: IntGaugeVec,
    allocation: IntGaugeVec,
    arena_count: IntGauge,
}

impl AllocStatsCollector {
    fn new<S: Into<String>>(namespace: S) -> Result<AllocStatsCollector> {
        let ns = namespace.into();
        let stats = IntGaugeVec::new(
            Opts::new("allocator_stats", "Allocator stats").namespace(ns.clone()),
            &["type"],
        )?;
        let thread_stats = IntGaugeVec::new(
            Opts::new(
                "allocator_thread_stats",
                "The allocation statistic for threads.",
            )
            .namespace(ns.clone()),
            &["type", "thread_name"],
        )?;
        let allocation = IntGaugeVec::new(
            Opts::new(
                "allocator_thread_allocation",
                "The allocation statistic for threads.",
            )
            .namespace(ns),
            &["type", "thread_name"],
        )?;
        let arena_count = IntGauge::new(
            "allocator_arena_count",
            "The count of arenas in the allocator.",
        )?;
        Ok(AllocStatsCollector {
            descs: [&stats, &thread_stats, &allocation]
                .iter()
                .flat_map(|m| m.desc().into_iter().cloned())
                .collect(),
            memory_stats: stats,
            thread_stats,
            allocation,
            arena_count,
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
                self.memory_stats
                    .with_label_values(&[stat.0])
                    .set(stat.1 as i64);
            }
        }
        self.arena_count.set(tikv_alloc::get_arena_count() as i64);
        tikv_alloc::iterate_arena_allocation_stats(|name, resident, mapped, retained| {
            self.thread_stats
                .with_label_values(&["resident", name])
                .set(resident as _);
            self.thread_stats
                .with_label_values(&["mapped", name])
                .set(mapped as _);
            self.thread_stats
                .with_label_values(&["retained", name])
                .set(retained as _);
        });
        tikv_alloc::iterate_thread_allocation_stats(|name, alloc, dealloc| {
            self.allocation
                .with_label_values(&["alloc", name])
                .set(alloc as _);
            self.allocation
                .with_label_values(&["dealloc", name])
                .set(dealloc as _);
        });
        let mut g = self.memory_stats.collect();
        g.extend(self.thread_stats.collect());
        g.extend(self.allocation.collect());
        g.extend(self.arena_count.collect());
        g
    }
}
