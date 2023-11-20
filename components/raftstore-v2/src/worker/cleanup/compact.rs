// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use engine_traits::{KvEngine, RangeStats, TabletRegistry, CF_WRITE};
use fail::fail_point;
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};
use slog::{debug, error, info, warn, Logger};
use thiserror::Error;
use tikv_util::{box_try, worker::Runnable};

pub enum Task {
    CheckAndCompact {
        // Column families need to compact
        cf_names: Vec<String>,
        region_ids: Vec<u64>,
        compact_threshold: CompactThreshold,
    },
}

pub struct CompactThreshold {
    tombstones_num_threshold: u64,
    tombstones_percent_threshold: u64,
    redundant_rows_threshold: u64,
    redundant_rows_percent_threshold: u64,
}

impl CompactThreshold {
    pub fn new(
        tombstones_num_threshold: u64,
        tombstones_percent_threshold: u64,
        redundant_rows_threshold: u64,
        redundant_rows_percent_threshold: u64,
    ) -> Self {
        Self {
            tombstones_num_threshold,
            tombstones_percent_threshold,
            redundant_rows_percent_threshold,
            redundant_rows_threshold,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::CheckAndCompact {
                ref cf_names,
                ref region_ids,
                ref compact_threshold,
            } => f
                .debug_struct("CheckAndCompact")
                .field("cf_names", cf_names)
                .field("regions", region_ids)
                .field(
                    "tombstones_num_threshold",
                    &compact_threshold.tombstones_num_threshold,
                )
                .field(
                    "tombstones_percent_threshold",
                    &compact_threshold.tombstones_percent_threshold,
                )
                .field(
                    "redundant_rows_threshold",
                    &compact_threshold.redundant_rows_threshold,
                )
                .field(
                    "redundant_rows_percent_threshold",
                    &compact_threshold.redundant_rows_percent_threshold,
                )
                .finish(),
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("compact failed {0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub struct Runner<E> {
    logger: Logger,
    tablet_registry: TabletRegistry<E>,
}

impl<E> Runner<E>
where
    E: KvEngine,
{
    pub fn new(tablet_registry: TabletRegistry<E>, logger: Logger) -> Runner<E> {
        Runner {
            logger,
            tablet_registry,
        }
    }
}

impl<E> Runnable for Runner<E>
where
    E: KvEngine,
{
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::CheckAndCompact {
                cf_names,
                region_ids,
                compact_threshold,
            } => match collect_regions_to_compact(
                &self.tablet_registry,
                region_ids,
                compact_threshold,
                &self.logger,
            ) {
                Ok(mut region_ids) => {
                    for region_id in region_ids.drain(..) {
                        let Some(mut tablet_cache) = self.tablet_registry.get(region_id) else {continue};
                        let Some(tablet) = tablet_cache.latest() else {continue};
                        for cf in &cf_names {
                            if let Err(e) =
                                tablet.compact_range_cf(cf, None, None, false, 1 /* threads */)
                            {
                                error!(
                                    self.logger,
                                    "compact range failed";
                                    "region_id" => region_id,
                                    "cf" => cf,
                                    "err" => %e,
                                );
                            }
                        }
                        info!(
                            self.logger,
                            "compaction range finished";
                            "region_id" => region_id,
                        );
                        fail_point!("raftstore-v2::CheckAndCompact::AfterCompact");
                    }
                }
                Err(e) => warn!(
                    self.logger,
                    "check ranges need reclaim failed"; "err" => %e
                ),
            },
        }
    }
}

fn need_compact(range_stats: &RangeStats, compact_threshold: &CompactThreshold) -> bool {
    if range_stats.num_entries < range_stats.num_versions {
        return false;
    }

    // We trigger region compaction when their are to many tombstones as well as
    // redundant keys, both of which can severly impact scan operation:
    let estimate_num_del = range_stats.num_entries - range_stats.num_versions;
    let redundant_keys = range_stats.num_entries - range_stats.num_rows;
    (redundant_keys >= compact_threshold.redundant_rows_threshold
        && redundant_keys * 100
            >= compact_threshold.redundant_rows_percent_threshold * range_stats.num_entries)
        || (estimate_num_del >= compact_threshold.tombstones_num_threshold
            && estimate_num_del * 100
                >= compact_threshold.tombstones_percent_threshold * range_stats.num_entries)
}

fn collect_regions_to_compact<E: KvEngine>(
    reg: &TabletRegistry<E>,
    region_ids: Vec<u64>,
    compact_threshold: CompactThreshold,
    logger: &Logger,
) -> Result<Vec<u64>, Error> {
    fail_point!("on_collect_regions_to_compact");
    debug!(
        logger,
        "received compaction check";
        "regions" => ?region_ids
    );
    let mut regions_to_compact = vec![];
    for id in region_ids {
        let Some(mut tablet_cache) = reg.get(id) else {continue};
        let Some(tablet) = tablet_cache.latest() else {continue};
        if tablet.auto_compactions_is_disabled().expect("cf") {
            info!(
                logger,
                "skip compact check when disabled auto compactions";
                "region_id" => id,
            );
            continue;
        }

        if let Some(range_stats) =
            box_try!(tablet.get_range_stats(CF_WRITE, DATA_MIN_KEY, DATA_MAX_KEY))
        {
            info!(
                logger,
                "get range entries and versions";
                "num_entries" => range_stats.num_entries,
                "num_versions" => range_stats.num_versions,
                "num_rows" => range_stats.num_rows,
                "region_id" => id,
            );
            if need_compact(&range_stats, &compact_threshold) {
                regions_to_compact.push(id);
            }
        }
    }
    Ok(regions_to_compact)
}

#[cfg(test)]
mod tests {
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, TestTabletFactory},
    };
    use engine_traits::{MiscExt, SyncMutable, TabletContext, TabletRegistry, CF_DEFAULT, CF_LOCK};
    use keys::data_key;
    use kvproto::metapb::Region;
    use tempfile::Builder;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

    fn build_test_factory(name: &'static str) -> (tempfile::TempDir, TabletRegistry<KvTestEngine>) {
        let dir = Builder::new().prefix(name).tempdir().unwrap();
        let mut cf_opts = CfOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(8);
        let factory = Box::new(TestTabletFactory::new(
            DbOptions::default(),
            vec![
                (CF_DEFAULT, CfOptions::new()),
                (CF_LOCK, CfOptions::new()),
                (CF_WRITE, cf_opts),
            ],
        ));
        let registry = TabletRegistry::new(factory, dir.path()).unwrap();
        (dir, registry)
    }

    fn mvcc_put(db: &KvTestEngine, k: &[u8], v: &[u8], start_ts: TimeStamp, commit_ts: TimeStamp) {
        let k = Key::from_encoded(data_key(k)).append_ts(commit_ts);
        let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
        db.put_cf(CF_WRITE, k.as_encoded(), &w.as_ref().to_bytes())
            .unwrap();
    }

    fn delete(db: &KvTestEngine, k: &[u8], commit_ts: TimeStamp) {
        let k = Key::from_encoded(data_key(k)).append_ts(commit_ts);
        db.delete_cf(CF_WRITE, k.as_encoded()).unwrap();
    }

    #[test]
    fn test_compact_range() {
        let (_dir, registry) = build_test_factory("compact-range-test");

        let mut region = Region::default();
        region.set_id(2);
        let ctx = TabletContext::new(&region, Some(5));
        let mut cache = registry.load(ctx, true).unwrap();
        let tablet = cache.latest().unwrap();

        // mvcc_put 0..5
        for i in 0..5 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(tablet, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
            mvcc_put(tablet, k.as_bytes(), v.as_bytes(), 3.into(), 4.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        // gc 0..5
        for i in 0..5 {
            let k = format!("k{}", i);
            delete(tablet, k.as_bytes(), 4.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        let (start, end) = (data_key(b"k0"), data_key(b"k5"));
        let range_stats = tablet
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(range_stats.num_entries, 15);
        assert_eq!(range_stats.num_versions, 10);
        assert_eq!(range_stats.num_rows, 5);

        region.set_id(3);
        let ctx = TabletContext::new(&region, Some(5));
        let mut cache = registry.load(ctx, true).unwrap();
        let tablet = cache.latest().unwrap();
        // mvcc_put 5..10
        for i in 5..10 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(tablet, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        for i in 5..8 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(tablet, k.as_bytes(), v.as_bytes(), 3.into(), 4.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let range_stats = tablet.get_range_stats(CF_WRITE, &s, &e).unwrap().unwrap();
        assert_eq!(range_stats.num_entries, 8);
        assert_eq!(range_stats.num_versions, 8);
        assert_eq!(range_stats.num_rows, 5);

        // gc 5..8
        for i in 5..8 {
            let k = format!("k{}", i);
            delete(tablet, k.as_bytes(), 4.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let range_stats = tablet.get_range_stats(CF_WRITE, &s, &e).unwrap().unwrap();
        assert_eq!(range_stats.num_entries, 11);
        assert_eq!(range_stats.num_versions, 8);
        assert_eq!(range_stats.num_rows, 5);

        let logger = slog_global::borrow_global().new(slog::o!());

        // collect regions according to tombstone's parameters
        let regions = collect_regions_to_compact(
            &registry,
            vec![2, 3, 4],
            CompactThreshold::new(4, 30, 100, 100),
            &logger,
        )
        .unwrap();
        assert!(regions.len() == 1 && regions[0] == 2);

        let regions = collect_regions_to_compact(
            &registry,
            vec![2, 3, 4],
            CompactThreshold::new(3, 25, 100, 100),
            &logger,
        )
        .unwrap();
        assert!(regions.len() == 2 && !regions.contains(&4));

        // collect regions accroding to redundant rows' parameter
        let regions = collect_regions_to_compact(
            &registry,
            vec![2, 3, 4],
            CompactThreshold::new(100, 100, 9, 60),
            &logger,
        )
        .unwrap();
        assert!(regions.len() == 1 && regions[0] == 2);

        let regions = collect_regions_to_compact(
            &registry,
            vec![2, 3, 4],
            CompactThreshold::new(100, 100, 5, 50),
            &logger,
        )
        .unwrap();
        assert!(regions.len() == 2 && !regions.contains(&4));
    }
}
