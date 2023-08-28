// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{
    Bound::{Excluded, Unbounded},
    HashSet,
};

use collections::HashMap;
use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine, CF_DEFAULT, CF_WRITE};
use raftstore::{
    store::{CompactThreshold, TabletSnapKey},
    Result,
};
use slog::{debug, error, info};

use crate::{
    batch::StoreContext,
    fsm::{Store, StoreFsmDelegate},
    router::{PeerMsg, StoreTick},
    worker::{
        cleanup::{self},
        tablet,
    },
    CompactTask::CheckAndCompact,
};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    pub fn register_compact_check_tick(&mut self) {
        self.schedule_tick(
            StoreTick::CompactCheck,
            self.store_ctx.cfg.region_compact_check_interval.0,
        )
    }

    pub fn on_compact_check_tick(&mut self) {
        self.register_compact_check_tick();
        if self.store_ctx.schedulers.cleanup.is_busy() {
            info!(
                self.store_ctx.logger,
                "compact worker is busy, check space redundancy next time";
            );
            return;
        }

        // Use HashSet here as the region end_keys in store_meta is not unique.
        let mut regions_to_check: HashSet<u64> = HashSet::default();

        let (largest_end_key, last_check_key) = {
            // Start from last checked key.
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

                if regions_to_check.len() >= self.store_ctx.cfg.region_compact_check_step() as usize
                {
                    break;
                }
            }

            (
                meta.region_ranges.keys().last().unwrap().0.to_vec(),
                last_check_key.clone(),
            )
        };

        if largest_end_key == last_check_key {
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
                compact_threshold: CompactThreshold::new(
                    self.store_ctx.cfg.region_compact_min_tombstones,
                    self.store_ctx.cfg.region_compact_tombstones_percent,
                    self.store_ctx.cfg.region_compact_min_redundant_rows,
                    self.store_ctx.cfg.region_compact_redundant_rows_percent(),
                ),
            }))
        {
            error!(
                self.store_ctx.logger,
                "schedule space check task failed";
                "err" => ?e,
            );
        }
    }

    #[inline]
    pub fn on_snapshot_gc(&mut self) {
        if let Err(e) = self.fsm.store.on_snapshot_gc(self.store_ctx) {
            error!(self.fsm.store.logger(), "cleanup import sst failed"; "error" => ?e);
        }
        self.schedule_tick(
            StoreTick::SnapGc,
            self.store_ctx.cfg.snap_mgr_gc_tick_interval.0,
        );
    }
}

impl Store {
    #[inline]
    fn on_snapshot_gc<EK: KvEngine, ER: RaftEngine, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
    ) -> Result<()> {
        let paths = ctx.snap_mgr.list_snapshot()?;
        let mut region_keys: HashMap<u64, Vec<TabletSnapKey>> = HashMap::default();
        for path in paths {
            let key = TabletSnapKey::from_path(path)?;
            region_keys.entry(key.region_id).or_default().push(key);
        }
        for (region_id, keys) in region_keys {
            if let Err(TrySendError::Disconnected(msg)) = ctx.router.send(region_id, PeerMsg::SnapGc(keys.into()))
                && !ctx.router.is_shutdown() {
                let PeerMsg::SnapGc(keys) = msg else { unreachable!() };
                let _ = ctx.schedulers.tablet.schedule(tablet::Task::SnapGc(keys));
            }
        }
        Ok(())
    }
}
