// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{
    Bound::{Excluded, Unbounded},
    HashSet,
};

use engine_traits::{KvEngine, RaftEngine, CF_DEFAULT, CF_WRITE};
use slog::{debug, error};

use crate::{
    fsm::StoreFsmDelegate, router::StoreTick, worker::cleanup, CompactTask::CheckAndCompact,
};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    pub fn register_compact_check_tick(&mut self) {
        self.schedule_tick(
            StoreTick::CompactCheck,
            self.store_ctx.cfg.region_compact_check_interval.0,
        )
    }

    pub fn on_ompact_check_tick(&mut self) {
        self.register_compact_check_tick();
        if self.store_ctx.schedulers.cleanup.is_busy() {
            info!(
                self.store_ctx.logger,
                "compact worker is busy, check space redundancy next time";
            );
            return;
        }

        // todo: auto compaction disable?

        // Start from last checked key.
        let mut regions_to_check: HashSet<u64> = HashSet::default();
        let (largest_key, last_check_key) = {
            let mut last_check_key = self.fsm.store.last_compact_checked_key();

            let meta = self.store_ctx.store_meta.lock().unwrap();
            if meta.region_ranges.is_empty() {
                debug!(
                    self.store_ctx.logger,
                    "there is no range need to check";
                );
                return;
            }
            // Collect continuous ranges.
            let ranges = meta.region_ranges.range((
                Excluded((last_check_key.clone(), u64::MAX)),
                Unbounded::<(Vec<u8>, u64)>,
            ));

            for region_range in ranges {
                last_check_key = &region_range.0.0;
                regions_to_check.insert(*region_range.1);
                if regions_to_check.len() >= 25 {
                    break;
                }
            }

            (
                meta.region_ranges.keys().last().unwrap().0.to_vec(),
                last_check_key.clone(),
            )
        };

        if last_check_key == largest_key {
            // Next task will start from the very beginning.
            self.fsm
                .store
                .set_last_compact_checked_key(keys::DATA_MIN_KEY.to_vec());
        } else {
            self.fsm.store.set_last_compact_checked_key(last_check_key);
        }

        // Schedule the task.
        let cf_names = vec![CF_DEFAULT.to_owned(), CF_WRITE.to_owned()];
        if let Err(e) = self
            .store_ctx
            .schedulers
            .cleanup
            .schedule(cleanup::Task::Compact(CheckAndCompact {
                cf_names,
                region_ids: regions_to_check.into_iter().collect::<Vec<_>>(),
                tombstones_num_threshold: self.store_ctx.cfg.region_compact_min_tombstones,
                tombstones_percent_threshold: self.store_ctx.cfg.region_compact_tombstones_percent,
            }))
        {
            error!(
                self.store_ctx.logger,
                "schedule space check task failed";
                "err" => ?e,
            );
        }
    }
}
