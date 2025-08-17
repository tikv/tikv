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
    // Access to store for region leader growth information
    store_growth_accessor: Option<Box<dyn Fn() -> HashMap<u64, u64> + Send + Sync>>,
    // Cached region classification from last sampling
    cached_classification: Option<RegionClassification>,
    // Region sampling interval from config
    region_sampling_interval: Duration,
    // Last time when force GC check was performed
    last_force_gc_check_time: Option<std::time::Instant>,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(
        pin_ratio: f64,
        evict_cache_on_memory_ratio: f64,
        force_gc_interval: Duration,
        region_sampling_interval: Duration,
    ) -> Runner<EK, ER> {
        Runner {
            engines: None,
            router: None,
            pin_ratio,
            evict_cache_on_memory_ratio,
            force_gc_interval,
            store_growth_accessor: None,
            cached_classification: None,
            region_sampling_interval,
            last_force_gc_check_time: None,
        }
    }

    pub fn setup(
        &mut self,
        engines: Engines<EK, ER>,
        router: Box<dyn CasualRouter<EK>>,
        growth_accessor: Box<dyn Fn() -> HashMap<u64, u64> + Send + Sync>,
    ) {
        self.engines = Some(engines);
        self.router = Some(router);
        self.store_growth_accessor = Some(growth_accessor);
        info!("raftlog compact worker setup completed with engines, router and growth accessor");
    }

    fn sample_regions(&mut self) -> RegionClassification {
        // Always perform sampling when called (Worker controls the frequency)

        let mut all_growth = Vec::new();

        // Get leader growth information from store
        if let Some(store_growth_accessor) = &self.store_growth_accessor {
            let leader_growth = store_growth_accessor();

            // Use leader growth information to build classification
            for (region_id, growth_rate) in leader_growth {
                all_growth.push((region_id, growth_rate));
            }

            info!(
                "using leader growth information for {} regions",
                all_growth.len()
            );
        } else {
            // Fallback to old method if no growth accessor available
            let mut region_last_indexes = HashMap::new();

            let _ = self
                .engines
                .as_ref()
                .unwrap()
                .raft
                .for_each_raft_group::<engine_traits::Error, _>(&mut |region_id| {
                    if let Ok(Some(raft_state)) = self
                        .engines
                        .as_ref()
                        .unwrap()
                        .raft
                        .get_raft_state(region_id)
                    {
                        let current_last_index = raft_state.get_last_index();
                        let previous_last_index = region_last_indexes.get(&region_id).unwrap_or(&0);
                        let growth = current_last_index.saturating_sub(*previous_last_index);

                        region_last_indexes.insert(region_id, current_last_index);
                        all_growth.push((region_id, growth));
                    }
                    Ok(())
                });
        }

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

        let classification = RegionClassification {
            pinned_regions,
            gc_candidate_regions,
        };

        // Cache the classification for force_gc to use
        self.cached_classification = Some(classification.clone());

        classification
    }

    fn check_force_gc_needed(&self) -> Option<f64> {
        let mut over_ratio = 0.0;
        let mut usage: f64 = 0.0;
        let raft_memory_usage = self
            .engines
            .as_ref()
            .unwrap()
            .raft
            .get_memory_usage()
            .unwrap() as f64;

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

    fn force_gc_with_classification(
        &mut self,
        over_ratio: f64,
        classification: RegionClassification,
    ) {
        // Use the provided classification directly

        let mut pinned_regions_compacted = 0;
        let mut gc_candidates_compacted = 0;

        // Get router from self to send messages
        if let Some(router) = &self.router {
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
    }

    fn process_growth_messages_and_update_cache(&mut self) {
        // This method will be called every region_sampling_interval (60s)
        // to process any new growth messages and update the cache

        if let Some(accessor) = &self.store_growth_accessor {
            let leader_growth = accessor();

            if !leader_growth.is_empty() {
                // Create new classification based on current growth data
                let classification = self.create_classification_from_growth(&leader_growth);

                // Update cache
                self.cached_classification = Some(classification);

                info!(
                    "updated region classification cache with {} regions",
                    leader_growth.len()
                );
            }
        }
    }

    fn create_classification_from_growth(
        &self,
        leader_growth: &HashMap<u64, u64>,
    ) -> RegionClassification {
        let mut all_growth = Vec::new();

        // Convert HashMap to Vec for sorting
        for (region_id, growth_rate) in leader_growth {
            all_growth.push((*region_id, *growth_rate));
        }

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
            "region classification created: {} pinned regions, {} GC candidate regions; pinned_region_ids: {:?}",
            pinned_regions.len(),
            gc_candidate_regions.len(),
            pinned_region_ids
        );

        RegionClassification {
            pinned_regions,
            gc_candidate_regions,
        }
    }

    fn should_check_force_gc(&mut self, now: std::time::Instant) -> bool {
        // Check if we should perform force GC check based on raft_log_force_gc_interval
        if let Some(last_check) = self.last_force_gc_check_time {
            now.duration_since(last_check) >= self.force_gc_interval
        } else {
            // First time check
            true
        }
    }

    fn get_or_create_classification(&mut self) -> RegionClassification {
        // Return cached classification if available, otherwise create new one
        if let Some(cached) = &self.cached_classification {
            cached.clone()
        } else {
            // Fallback: create empty classification
            RegionClassification {
                pinned_regions: HashMap::new(),
                gc_candidate_regions: HashMap::new(),
            }
        }
    }

    fn check_and_execute_force_gc(&mut self, classification: RegionClassification) {
        // Update last check time
        self.last_force_gc_check_time = Some(std::time::Instant::now());

        // Check if force GC is needed and execute it
        if let Some(over_ratio) = self.check_force_gc_needed() {
            info!(
                "force GC needed based on memory pressure, over_ratio: {}",
                over_ratio
            );
            self.force_gc_with_classification(over_ratio, classification);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = DummyTask;

    fn run(&mut self, _task: DummyTask) {
        // This method is no longer used for force GC checking
        // Force GC is now handled in the timer-based on_timeout method
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
        let now = std::time::Instant::now();

        self.process_growth_messages_and_update_cache();

        if self.should_check_force_gc(now) {
            let classification = self.get_or_create_classification();
            self.check_and_execute_force_gc(classification);
        }
    }

    fn get_interval(&self) -> Duration {
        self.region_sampling_interval
    }
}

#[cfg(test)]
mod tests {
    use engine_test::kv::KvTestEngine;

    use super::*;

    #[test]
    fn test_runner_creation() {
        let runner = Runner::<KvTestEngine, KvTestEngine>::new(
            0.2,
            0.8,
            Duration::from_secs(300),
            Duration::from_secs(60),
        );
        assert!(runner.engines.is_none());
        assert!(runner.router.is_none());
        assert_eq!(runner.pin_ratio, 0.2);
        assert_eq!(runner.evict_cache_on_memory_ratio, 0.8);
        assert_eq!(runner.force_gc_interval, Duration::from_secs(300));
        assert_eq!(runner.region_sampling_interval, Duration::from_secs(60));
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
