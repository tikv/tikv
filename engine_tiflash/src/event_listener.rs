// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use file_system::{get_io_type, set_io_type, IOType};
use regex::Regex;
use rocksdb::{
    CompactionJobInfo, DBBackgroundErrorReason, FlushJobInfo, IngestionInfo, MutableStatus,
    SubcompactionJobInfo, WriteStallInfo,
};
use tikv_util::{error, metrics::CRITICAL_ERROR, set_panic_mark, warn, worker::Scheduler};

use crate::rocks_metrics::*;

// Message for RocksDB status subcode kNoSpace.
const NO_SPACE_ERROR: &str = "IO error: No space left on device";

pub struct RocksEventListener {
    db_name: String,
    sst_recovery_scheduler: Option<Scheduler<String>>,
}

impl RocksEventListener {
    pub fn new(
        db_name: &str,
        sst_recovery_scheduler: Option<Scheduler<String>>,
    ) -> RocksEventListener {
        RocksEventListener {
            db_name: db_name.to_owned(),
            sst_recovery_scheduler,
        }
    }
}

impl rocksdb::EventListener for RocksEventListener {
    fn on_flush_begin(&self, _info: &FlushJobInfo) {
        set_io_type(IOType::Flush);
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "flush"])
            .inc();
        if get_io_type() == IOType::Flush {
            set_io_type(IOType::Other);
        }
    }

    fn on_compaction_begin(&self, info: &CompactionJobInfo) {
        if info.base_input_level() == 0 {
            set_io_type(IOType::LevelZeroCompaction);
        } else {
            set_io_type(IOType::Compaction);
        }
    }

    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "compaction"])
            .inc();
        STORE_ENGINE_COMPACTION_DURATIONS_VEC
            .with_label_values(&[&self.db_name, info.cf_name()])
            .observe(info.elapsed_micros() as f64 / 1_000_000.0);
        STORE_ENGINE_COMPACTION_NUM_CORRUPT_KEYS_VEC
            .with_label_values(&[&self.db_name, info.cf_name()])
            .inc_by(info.num_corrupt_keys());
        STORE_ENGINE_COMPACTION_REASON_VEC
            .with_label_values(&[
                &self.db_name,
                info.cf_name(),
                &info.compaction_reason().to_string(),
            ])
            .inc();
        if info.base_input_level() == 0 && get_io_type() == IOType::LevelZeroCompaction
            || info.base_input_level() != 0 && get_io_type() == IOType::Compaction
        {
            set_io_type(IOType::Other);
        }
    }

    fn on_subcompaction_begin(&self, info: &SubcompactionJobInfo) {
        if info.base_input_level() == 0 {
            set_io_type(IOType::LevelZeroCompaction);
        } else {
            set_io_type(IOType::Compaction);
        }
    }

    fn on_subcompaction_completed(&self, info: &SubcompactionJobInfo) {
        if info.base_input_level() == 0 && get_io_type() == IOType::LevelZeroCompaction
            || info.base_input_level() != 0 && get_io_type() == IOType::Compaction
        {
            set_io_type(IOType::Other);
        }
    }

    fn on_external_file_ingested(&self, info: &IngestionInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "ingestion"])
            .inc();
        STORE_ENGINE_INGESTION_PICKED_LEVEL_VEC
            .with_label_values(&[&self.db_name, info.cf_name()])
            .observe(info.picked_level() as f64);
    }

    fn on_background_error(&self, reason: DBBackgroundErrorReason, status: MutableStatus) {
        let result = status.result();
        assert!(result.is_err());
        if let Err(err) = result {
            if matches!(
                reason,
                DBBackgroundErrorReason::Flush | DBBackgroundErrorReason::Compaction
            ) && err.starts_with(NO_SPACE_ERROR)
            {
                // Ignore NoSpace error and let RocksDB automatically recover.
                return;
            }

            let r = match reason {
                DBBackgroundErrorReason::Flush => "flush",
                DBBackgroundErrorReason::Compaction => "compaction",
                DBBackgroundErrorReason::WriteCallback => "write_callback",
                DBBackgroundErrorReason::MemTable => "memtable",
            };

            if err.starts_with("Corruption") || err.starts_with("IO error") {
                if let Some(scheduler) = self.sst_recovery_scheduler.as_ref() {
                    if let Some(path) = resolve_sst_filename_from_err(&err) {
                        warn!(
                            "detected rocksdb background error";
                            "sst" => &path,
                            "err" => &err
                        );
                        match scheduler.schedule(path) {
                            Ok(()) => {
                                status.reset();
                                CRITICAL_ERROR.with_label_values(&["sst_corruption"]).inc();
                                return;
                            }
                            Err(e) => {
                                error!("rocksdb sst recovery job schedule failed, error: {:?}", e);
                            }
                        }
                    }
                }
            }

            // Avoid tikv from restarting if rocksdb get corruption.
            if err.starts_with("Corruption") {
                set_panic_mark();
            }
            panic!(
                "rocksdb background error. db: {}, reason: {}, error: {}",
                self.db_name, r, err
            );
        }
    }

    fn on_stall_conditions_changed(&self, info: &WriteStallInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "stall_conditions_changed"])
            .inc();
    }
}

// Here are some expected error examples:
// 1. Corruption: Sst file size mismatch: /qps/data/tikv-10014/db/000398.sst. Size recorded in manifest 6975, actual size 6959
// 2. Corruption: Bad table magic number: expected 9863518390377041911, found 759105309091689679 in /qps/data/tikv-10014/db/000021.sst
//
// We assume that only the corruption sst file path is printed inside error.
fn resolve_sst_filename_from_err(err: &str) -> Option<String> {
    let r = Regex::new(r"/\w*\.sst").unwrap();
    let matches = match r.captures(err) {
        None => return None,
        Some(v) => v,
    };
    let filename = matches.get(0).unwrap().as_str().to_owned();
    Some(filename)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_sst_filename() {
        let err = "Corruption: Sst file size mismatch: /qps/data/tikv-10014/db/000398.sst. Size recorded in manifest 6975, actual size 6959";
        let filename = resolve_sst_filename_from_err(err).unwrap();
        assert_eq!(filename, "/000398.sst");
    }
}
