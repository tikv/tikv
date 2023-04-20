// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use engine_traits::{KvEngine, Range, TabletRegistry, CF_WRITE};
use fail::fail_point;
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};
use slog::{error, info, warn, Logger};
use thiserror::Error;
use tikv_util::{box_try, worker::Runnable};

pub enum Task {
    // Compact {},
    CheckAndCompact {
        // Column families need to compact
        cf_names: Vec<String>,
        region_ids: Vec<u64>,
        // The minimum RocksDB tombstones a range that need compacting has
        tombstones_num_threshold: u64,
        tombstones_percent_threshold: u64,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::CheckAndCompact {
                ref cf_names,
                ref region_ids,
                tombstones_num_threshold,
                tombstones_percent_threshold,
            } => f
                .debug_struct("CheckAndCompact")
                .field("cf_names", cf_names)
                .field("regions", region_ids)
                .field("tombstones_num_threshold", &tombstones_num_threshold)
                .field(
                    "tombstones_percent_threshold",
                    &tombstones_percent_threshold,
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
                tombstones_num_threshold,
                tombstones_percent_threshold,
            } => match collect_regions_to_compact(
                &self.tablet_registry,
                region_ids,
                tombstones_num_threshold,
                tombstones_percent_threshold,
                &self.logger,
            ) {
                Ok(mut region_ids) => {
                    for region_id in region_ids.drain(..) {
                        let Some(mut tablet_cache) = self.tablet_registry.get(region_id) else {continue};
                        let Some(tablet) = tablet_cache.latest() else {continue};
                        for cf in &cf_names {
                            // to be removed
                            let approximate_size = tablet
                                .get_range_approximate_size_cf(cf, Range::new(b"", DATA_MAX_KEY), 0)
                                .unwrap_or_default();
                            if let Err(e) =
                                tablet.compact_range_cf(cf, None, None, false, 1 /* threads */)
                            {
                                error!(
                                    self.logger,
                                    "compact range failed";
                                    "regionid" => region_id,
                                    "cf" => cf,
                                    "err" => %e,
                                );
                            }
                            let cur_approximate_size = tablet
                                .get_range_approximate_size_cf(cf, Range::new(b"", DATA_MAX_KEY), 0)
                                .unwrap_or_default();
                            info!(
                                self.logger,
                                "Compaction done";
                                "cf" => cf,
                                "region_id" => region_id,
                                "approximate_size_before" => approximate_size,
                                "approximate_size_after" => cur_approximate_size
                            );
                        }
                        fail_point!("raftstore-v2::CheckAndCompact:AfterCompact");
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

fn need_compact(
    num_rows: u64,
    num_entires: u64,
    num_versions: u64,
    tombstones_num_threshold: u64,
    tombstones_percent_threshold: u64,
) -> bool {
    if num_entires < num_versions {
        return false;
    }

    // When the number of tombstones exceed threshold and ratio, this range need
    // compacting.
    let estimate_num_del = num_entires - num_versions;
    (num_versions - num_rows) >= tombstones_num_threshold
        || (estimate_num_del >= tombstones_num_threshold
            && estimate_num_del * 100 >= tombstones_percent_threshold * num_entires)
}

fn collect_regions_to_compact<E: KvEngine>(
    reg: &TabletRegistry<E>,
    region_ids: Vec<u64>,
    tombstones_num_threshold: u64,
    tombstones_percent_threshold: u64,
    logger: &Logger,
) -> Result<Vec<u64>, Error> {
    info!(
        logger,
        "received compaction check";
        "regions" => ?region_ids
    );
    let mut regions_to_compact = vec![];
    for id in region_ids {
        let Some(mut tablet_cache) = reg.get(id) else {continue};
        let Some(tablet) = tablet_cache.latest() else {continue};
        if let Some((num_ent, num_ver, num_rows)) =
            box_try!(tablet.get_range_entries_and_versions(CF_WRITE, DATA_MIN_KEY, DATA_MAX_KEY))
        {
            // println!(
            //     "num of rows {}, num of ver {}, num_ents {}",
            //     num_rows, num_ver, num_ent
            // );
            info!(
                logger,
                "get range entries and versions";
                "num_entries" => num_ent,
                "num_versions" => num_ver,
                "num_rows" => num_rows,
                "region_id" => id,
            );
            if need_compact(
                num_rows,
                num_ent,
                num_ver,
                tombstones_num_threshold,
                tombstones_percent_threshold,
            ) {
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
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        // gc 0..5
        for i in 0..5 {
            let k = format!("k{}", i);
            delete(tablet, k.as_bytes(), 2.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        let (start, end) = (data_key(b"k0"), data_key(b"k5"));
        let (entries, version) = tablet
            .get_range_entries_and_versions(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(entries, 10);
        assert_eq!(version, 5);

        region.set_id(3);
        let ctx = TabletContext::new(&region, Some(5));
        let mut cache = registry.load(ctx, true).unwrap();
        let tablet = cache.latest().unwrap();
        // mvcc_put 5..10
        for i in 5..10 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(tablet, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let (entries, version) = tablet
            .get_range_entries_and_versions(CF_WRITE, &s, &e)
            .unwrap()
            .unwrap();
        assert_eq!(entries, 5);
        assert_eq!(version, 5);

        // gc 5..8
        for i in 5..8 {
            let k = format!("k{}", i);
            delete(tablet, k.as_bytes(), 2.into());
        }
        tablet.flush_cf(CF_WRITE, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let (entries, version) = tablet
            .get_range_entries_and_versions(CF_WRITE, &s, &e)
            .unwrap()
            .unwrap();
        assert_eq!(entries, 8);
        assert_eq!(version, 5);

        let logger = slog_global::borrow_global().new(slog::o!());

        let regions = collect_regions_to_compact(&registry, vec![2, 3, 4], 4, 50, &logger).unwrap();
        assert!(regions.len() == 1 && regions[0] == 2);

        let regions = collect_regions_to_compact(&registry, vec![2, 3, 4], 3, 30, &logger).unwrap();
        assert!(regions.len() == 2 && !regions.contains(&4));
    }
}
