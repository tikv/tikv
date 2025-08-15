// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, time::Duration};

use engine_traits::{Engines, KvEngine, RaftEngine};
use tikv_util::{
    info,
    sys::needs_force_compact,
    worker::{Runnable, RunnableWithTimer},
};

use crate::store::{CasualMessage, transport::CasualRouter};

/// Region classification for force GC and sampling
#[derive(Debug, Clone)]
pub struct RegionClassification {
    pub pinned_regions: HashMap<u64, u64>, // region_id -> growth
    pub gc_candidate_regions: HashMap<u64, u64>, /* region_id -> growth
                                            * pub pinned_regions_sum: u64,
                                            * pub gc_candidates_sum: u64, */
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    engines: Option<Engines<EK, ER>>,
    router: Option<Box<dyn CasualRouter<EK>>>,
    pin_ratio: f64,
    evict_cache_on_memory_ratio: f64,
    force_gc_interval: Duration,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(
        pin_ratio: f64,
        evict_cache_on_memory_ratio: f64,
        force_gc_interval: Duration,
    ) -> Runner<EK, ER> {
        Runner {
            engines: None,
            router: None,
            pin_ratio,
            evict_cache_on_memory_ratio,
            force_gc_interval,
        }
    }

    pub fn setup(&mut self, engines: Engines<EK, ER>, router: Box<dyn CasualRouter<EK>>) {
        self.engines = Some(engines);
        self.router = Some(router);
        info!("raftlog compact worker setup completed with engines and router");
    }

    fn sample_regions(&self, engines: &Engines<EK, ER>) -> RegionClassification {
        let mut all_growth = Vec::new();
        let mut region_last_indexes = HashMap::new();

        // Collect all regions and their growth rates
        let _ = engines
            .raft
            .for_each_raft_group::<engine_traits::Error, _>(&mut |region_id| {
                if let Ok(Some(raft_state)) = engines.raft.get_raft_state(region_id) {
                    let current_last_index = raft_state.get_last_index();
                    let previous_last_index = region_last_indexes.get(&region_id).unwrap_or(&0);
                    let growth = current_last_index.saturating_sub(*previous_last_index);

                    region_last_indexes.insert(region_id, current_last_index);
                    all_growth.push((region_id, growth));
                }
                Ok(())
            });

        if all_growth.is_empty() {
            return RegionClassification {
                pinned_regions: HashMap::new(),
                gc_candidate_regions: HashMap::new(),
            };
        }

        // Sort and classify regions: keep most active regions as pinned
        all_growth.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by growth rate (descending: fastest first)

        let pin_count = std::cmp::max(1, (all_growth.len() as f64 * self.pin_ratio) as usize);

        let mut pinned_regions = HashMap::new();
        let mut gc_candidate_regions = HashMap::new();

        // Keep fastest growing regions as pinned (most active)
        for i in 0..pin_count {
            let (region_id, growth) = all_growth[i];
            pinned_regions.insert(region_id, growth);
        }

        // Rest become GC candidates
        for i in pin_count..all_growth.len() {
            let (region_id, growth) = all_growth[i];
            gc_candidate_regions.insert(region_id, growth);
        }

        let pinned_region_ids: Vec<u64> = pinned_regions.keys().copied().collect();
        info!(
            "region sampling completed: {} pinned regions, {} GC candidate regions; pinned_region_ids: {:?}",
            pinned_regions.len(),
            gc_candidate_regions.len(),
            pinned_region_ids
        );

        RegionClassification {
            pinned_regions,
            gc_candidate_regions,
        }
    }

    fn check_force_gc_needed(&self, engines: &Engines<EK, ER>) -> Option<f64> {
        let mut over_ratio = 0.0;
        let mut usage: f64 = 0.0;
        let raft_memory_usage = engines.raft.get_memory_usage().unwrap() as f64;

        if needs_force_compact(
            &mut over_ratio,
            &mut usage,
            self.evict_cache_on_memory_ratio,
        ) {
            if raft_memory_usage > usage * 0.04 {
                return Some(over_ratio);
            }
        }
        None
    }

    fn force_gc(&self, engines: &Engines<EK, ER>, router: &dyn CasualRouter<EK>, over_ratio: f64) {
        // First, sample regions to get current classification
        let classification = self.sample_regions(engines);

        let mut pinned_regions_compacted = 0;
        let mut gc_candidates_compacted = 0;

        // Send force compact messages to GC candidate regions
        for (region_id, _) in &classification.gc_candidate_regions {
            let _ = router.send(*region_id, CasualMessage::ForceCompactRaftLogs);
            gc_candidates_compacted += 1;
        }

        // If memory pressure is high, also send to some pinned regions
        if over_ratio > 1.0 {
            // Calculate dynamic pin ratio: shrink based on memory pressure
            let dynamic_pin_ratio = self.pin_ratio / over_ratio;
            let total_regions =
                classification.pinned_regions.len() + classification.gc_candidate_regions.len();
            let dynamic_pin_count =
                std::cmp::max(1, (total_regions as f64 * dynamic_pin_ratio) as usize);

            // Convert pinned regions to sorted vector for selection
            let mut sorted_pinned: Vec<(u64, u64)> = classification
                .pinned_regions
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect();
            sorted_pinned.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by growth rate (descending: fastest first)

            // Send to pinned regions that exceed the dynamic pin count
            // Choose the slowest growth pinned regions to minimize impact
            for i in dynamic_pin_count..sorted_pinned.len() {
                let (region_id, _) = sorted_pinned[i];
                let _ = router.send(region_id, CasualMessage::ForceCompactRaftLogs);
                pinned_regions_compacted += 1;
                info!(
                    "sending force compact to pinned region {} due to high memory pressure (over_ratio: {})",
                    region_id, over_ratio
                );
            }
        }

        let regions_compacted = gc_candidates_compacted + pinned_regions_compacted;

        info!(
            "force GC completed: {} total regions, {} GC candidates, {} pinned regions",
            regions_compacted, gc_candidates_compacted, pinned_regions_compacted
        );
    }

    fn check_and_execute_force_gc(&mut self) {
        // Check if force GC is needed and execute it
        if let (Some(engines), Some(router)) = (&self.engines, &self.router) {
            if let Some(over_ratio) = self.check_force_gc_needed(engines) {
                info!(
                    "force GC needed based on memory pressure, over_ratio: {}",
                    over_ratio
                );
                self.force_gc(engines, router.as_ref(), over_ratio);
            }
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = DummyTask;

    fn run(&mut self, _task: DummyTask) {
        // Check if force GC is needed and execute it
        self.check_and_execute_force_gc();
    }
}

#[derive(Debug, Clone)]
pub struct DummyTask;

impl std::fmt::Display for DummyTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DummyTask")
    }
}

impl<EK: KvEngine, ER: RaftEngine> RunnableWithTimer for Runner<EK, ER> {
    fn on_timeout(&mut self) {
        // Check if force GC is needed and execute it
        self.check_and_execute_force_gc();
    }

    fn get_interval(&self) -> Duration {
        // Use the configured interval from config
        self.force_gc_interval
    }
}

#[cfg(test)]
mod tests {
    use engine_test::kv::KvTestEngine;

    use super::*;

    #[test]
    fn test_runner_creation() {
        let runner = Runner::<KvTestEngine, KvTestEngine>::new(0.2, 0.8, Duration::from_secs(60));
        assert!(runner.engines.is_none());
        assert!(runner.router.is_none());
        assert_eq!(runner.pin_ratio, 0.2);
        assert_eq!(runner.evict_cache_on_memory_ratio, 0.8);
        assert_eq!(runner.force_gc_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_region_classification() {
        let classification = RegionClassification {
            pinned_regions: HashMap::new(),
            gc_candidate_regions: HashMap::new(),
        };

        assert_eq!(classification.pinned_regions.len(), 0);
        assert_eq!(classification.gc_candidate_regions.len(), 0);
    }
}
