// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{ImportExt, IngestExternalFileOptions, Result};
use rocksdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
use tikv_util::time::Instant;

use crate::{
    engine::RocksEngine, perf_context_metrics::INGEST_EXTERNAL_FILE_TIME_HISTOGRAM, r2e, util,
};

impl ImportExt for RocksEngine {
    type IngestExternalFileOptions = RocksIngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf_name: &str, files: &[&str]) -> Result<()> {
        let cf = util::get_cf_handle(self.as_inner(), cf_name)?;
        let mut opts = RocksIngestExternalFileOptions::new();
        opts.move_files(true);
        opts.set_write_global_seqno(false);
        // Note: no need reset the global seqno to 0 for compatibility as #16992
        // enable the TiKV to handle the case on applying abnormal snapshot.
        let now = Instant::now_coarse();
        // This is calling a specially optimized version of
        // ingest_external_file_cf. In cases where the memtable needs to be
        // flushed it avoids blocking writers while doing the flush. The unused
        // return value here just indicates whether the fallback path requiring
        // the manual memtable flush was taken.
        let did_nonblocking_memtable_flush = self
            .as_inner()
            .ingest_external_file_optimized(cf, &opts.0, files)
            .map_err(r2e)?;
        let time_cost = now.saturating_elapsed_secs();
        if did_nonblocking_memtable_flush {
            INGEST_EXTERNAL_FILE_TIME_HISTOGRAM
                .get(cf_name.into())
                .non_block
                .observe(time_cost);
        } else {
            INGEST_EXTERNAL_FILE_TIME_HISTOGRAM
                .get(cf_name.into())
                .block
                .observe(time_cost);
        }
        Ok(())
    }
}

pub struct RocksIngestExternalFileOptions(RawIngestExternalFileOptions);

impl IngestExternalFileOptions for RocksIngestExternalFileOptions {
    fn new() -> RocksIngestExternalFileOptions {
        RocksIngestExternalFileOptions(RawIngestExternalFileOptions::new())
    }

    fn move_files(&mut self, f: bool) {
        self.0.move_files(f);
    }

    fn get_write_global_seqno(&self) -> bool {
        self.0.get_write_global_seqno()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        self.0.set_write_global_seqno(f);
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{
        FlowControlFactorsExt, MiscExt, Mutable, SstWriter, SstWriterBuilder, WriteBatch,
        WriteBatchExt, ALL_CFS, CF_DEFAULT,
    };
    use tempfile::Builder;

    use super::*;
    use crate::{util::new_engine_opt, RocksCfOptions, RocksDbOptions, RocksSstWriterBuilder};

    #[test]
    fn test_ingest_multiple_file() {
        let path_dir = Builder::new()
            .prefix("test_ingest_multiple_file")
            .tempdir()
            .unwrap();
        let root_path = path_dir.path();
        let db_path = root_path.join("db");
        let path_str = db_path.to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut opt = RocksCfOptions::default();
                opt.set_force_consistency_checks(true);
                (*cf, opt)
            })
            .collect();
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        let mut wb = db.write_batch();
        for i in 1000..5000 {
            let v = i.to_string();
            wb.put(v.as_bytes(), v.as_bytes()).unwrap();
            if i % 1000 == 100 {
                wb.write().unwrap();
                wb.clear();
            }
        }
        // Flush one memtable to L0 to make sure that the next sst files to be ingested
        //  must locate in L0.
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            1,
            db.get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap()
        );

        let p1 = root_path.join("sst1");
        let p2 = root_path.join("sst2");
        let mut sst1 = RocksSstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(p1.to_str().unwrap())
            .unwrap();
        let mut sst2 = RocksSstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(p2.to_str().unwrap())
            .unwrap();
        for i in 1001..2000 {
            let v = i.to_string();
            sst1.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        sst1.finish().unwrap();
        for i in 2001..3000 {
            let v = i.to_string();
            sst2.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        sst2.finish().unwrap();
        db.ingest_external_file_cf(CF_DEFAULT, &[p1.to_str().unwrap(), p2.to_str().unwrap()])
            .unwrap();
    }
}
