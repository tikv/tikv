// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use engine_traits::{KvEngine, TabletRegistry, CF_WRITE};
use keys::{MAX_KEY, MIN_KEY};
use slog::{error, warn, Logger};
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
                                    "regionid" => region_id,
                                    "cf" => cf,
                                    "err" => %e,
                                );
                            }
                        }
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
    num_entires: u64,
    num_versions: u64,
    tombstones_num_threshold: u64,
    tombstones_percent_threshold: u64,
) -> bool {
    if num_entires <= num_versions {
        return false;
    }

    // When the number of tombstones exceed threshold and ratio, this range need
    // compacting.
    let estimate_num_del = num_entires - num_versions;
    estimate_num_del >= tombstones_num_threshold
        && estimate_num_del * 100 >= tombstones_percent_threshold * num_entires
}

fn collect_regions_to_compact<E: KvEngine>(
    reg: &TabletRegistry<E>,
    region_ids: Vec<u64>,
    tombstones_num_threshold: u64,
    tombstones_percent_threshold: u64,
) -> Result<Vec<u64>, Error> {
    let mut regions_to_compact = vec![];
    for id in region_ids {
        let Some(mut tablet_cache) = reg.get(id) else {continue};
        let Some(tablet) = tablet_cache.latest() else {continue};
        if let Some((num_ent, num_ver)) =
            box_try!(tablet.get_range_entries_and_versions(CF_WRITE, MIN_KEY, MAX_KEY))
        {
            if need_compact(
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
    use engine_traits::{
        MiscExt, Mutable, TabletContext, TabletFactory, TabletRegistry, WriteBatch, WriteBatchExt,
        CF_DEFAULT,
    };
    use kvproto::metapb::Region;
    use slog::Logger;
    use tempfile::Builder;

    use super::*;

    fn build_test_factory(name: &'static str) -> (tempfile::TempDir, TabletRegistry<KvTestEngine>) {
        let dir = Builder::new().prefix(name).tempdir().unwrap();
        let factory = Box::new(TestTabletFactory::new(
            DbOptions::default(),
            vec![
                ("default", CfOptions::default()),
                ("write", CfOptions::default()),
            ],
        ));
        let registry = TabletRegistry::new(factory, dir.path()).unwrap();
        (dir, registry)
    }

    #[test]
    fn test_compact_range() {
        let (_dir, registry) = build_test_factory("compact-range-test");
        let logger = slog_global::borrow_global().new(slog::o!());
        let mut runner = Runner::new(registry.clone(), logger);

        let mut region = Region::default();
        region.set_id(2);
        let mut ctx = TabletContext::new(&region, Some(5));
        let mut cache = registry.load(ctx, true).unwrap();
        let tablet = cache.latest().unwrap();

        // Generate the first SST file.
        let mut wb = tablet.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        tablet.flush_cf(CF_DEFAULT, true).unwrap();

        // Generate the first SST file.
        let mut wb = tablet.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        tablet.flush_cf(CF_DEFAULT, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = tablet
            .get_total_sst_files_size_cf(CF_DEFAULT)
            .unwrap()
            .unwrap();

        runner.run(Task::CheckAndCompact {
            cf_names: vec![CF_DEFAULT.to_string()],
            region_ids: vec![2],
            tombstones_num_threshold: 2,
            tombstones_percent_threshold: 2,
        })
    }
}
