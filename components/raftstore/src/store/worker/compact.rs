// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use engine_traits::{KvEngine, ManualCompactionOptions};
use fail::fail_point;
use thiserror::Error;
use tikv_util::{box_try, error, info, time::Instant, worker::Runnable};

use super::metrics::COMPACT_RANGE_CF;

type Key = Vec<u8>;

pub enum Task {
    Compact {
        cf_name: String,
        start_key: Option<Key>, // None means smallest key
        end_key: Option<Key>,   // None means largest key
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Compact {
                ref cf_name,
                ref start_key,
                ref end_key,
            } => f
                .debug_struct("Compact")
                .field("cf_name", cf_name)
                .field(
                    "start_key",
                    &start_key.as_ref().map(|k| log_wrappers::Value::key(k)),
                )
                .field(
                    "end_key",
                    &end_key.as_ref().map(|k| log_wrappers::Value::key(k)),
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
    engine: E,
}

impl<E> Runner<E>
where
    E: KvEngine,
{
    pub fn new(engine: E) -> Runner<E> {
        Runner { engine }
    }

    /// Sends a compact range command to RocksDB to compact the range of the cf.
    pub fn compact_range_cf(
        &mut self,
        cf_name: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<(), Error> {
        fail_point!("on_compact_range_cf");
        let timer = Instant::now();
        box_try!(self.engine.compact_range_cf(
            cf_name,
            start_key,
            end_key,
            ManualCompactionOptions::new(false, 1, false)
        ));
        info!(
            "compact range finished";
            "range_start" => start_key.map(::log_wrappers::Value::key),
            "range_end" => end_key.map(::log_wrappers::Value::key),
            "cf" => cf_name,
            "time_takes" => ?timer.saturating_elapsed(),
        );
        Ok(())
    }
}

impl<E> Runnable for Runner<E>
where
    E: KvEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact {
                cf_name,
                start_key,
                end_key,
            } => {
                let cf = &cf_name;
                let compact_range_timer = COMPACT_RANGE_CF
                    .with_label_values(&[cf])
                    .start_coarse_timer();
                if let Err(e) = self.compact_range_cf(cf, start_key.as_deref(), end_key.as_deref())
                {
                    error!("execute compact range failed"; "cf" => cf, "err" => %e);
                }
                compact_range_timer.observe_duration();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use engine_test::kv::new_engine;
    use engine_traits::{
        CompactExt, ManualCompactionOptions, MiscExt, Mutable, WriteBatch, WriteBatchExt,
        CF_DEFAULT,
    };
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_disable_manual_compaction() {
        let path = Builder::new()
            .prefix("test_disable_manual_compaction")
            .tempdir()
            .unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

        // Generate the first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Generate another SST file has the same content with first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();

        // Stop the assistant.
        {
            let _ = db.disable_manual_compaction();

            // Manually compact range.
            let _ = db.compact_range_cf(
                CF_DEFAULT,
                None,
                None,
                ManualCompactionOptions::new(false, 1, false),
            );

            // Get the total SST files size after compact range.
            let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
            assert_eq!(old_sst_files_size, new_sst_files_size);
        }
        // Restart the assistant.
        {
            let _ = db.enable_manual_compaction();

            // Manually compact range.
            let _ = db.compact_range_cf(
                CF_DEFAULT,
                None,
                None,
                ManualCompactionOptions::new(false, 1, false),
            );

            // Get the total SST files size after compact range.
            let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
            assert!(old_sst_files_size > new_sst_files_size);
        }
    }

    #[test]
    fn test_compact_range() {
        let path = Builder::new()
            .prefix("compact-range-test")
            .tempdir()
            .unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

        let mut runner = Runner::new(db.clone());

        // Generate the first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Generate another SST file has the same content with first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();

        // Schedule compact range task.
        runner.run(Task::Compact {
            cf_name: String::from(CF_DEFAULT),
            start_key: None,
            end_key: None,
        });
        sleep(Duration::from_secs(5));

        // Get the total SST files size after compact range.
        let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
        assert!(old_sst_files_size > new_sst_files_size);
    }
}
