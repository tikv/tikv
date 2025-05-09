// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::PersistenceListener;
use file_system::{get_io_type, set_io_type, IoType};
use regex::Regex;
use rocksdb::{
    CompactionJobInfo, DBBackgroundErrorReason, FlushJobInfo, IngestionInfo, MemTableInfo,
    MutableStatus, SubcompactionJobInfo, WriteStallInfo,
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
        set_io_type(IoType::Flush);
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "flush"])
            .inc();
        if get_io_type() == IoType::Flush {
            set_io_type(IoType::Other);
        }
    }

    fn on_compaction_begin(&self, info: &CompactionJobInfo) {
        if info.base_input_level() == 0 {
            set_io_type(IoType::LevelZeroCompaction);
        } else {
            set_io_type(IoType::Compaction);
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
        if info.base_input_level() == 0 && get_io_type() == IoType::LevelZeroCompaction
            || info.base_input_level() != 0 && get_io_type() == IoType::Compaction
        {
            set_io_type(IoType::Other);
        }
    }

    fn on_subcompaction_begin(&self, info: &SubcompactionJobInfo) {
        if info.base_input_level() == 0 {
            set_io_type(IoType::LevelZeroCompaction);
        } else {
            set_io_type(IoType::Compaction);
        }
    }

    fn on_subcompaction_completed(&self, info: &SubcompactionJobInfo) {
        if info.base_input_level() == 0 && get_io_type() == IoType::LevelZeroCompaction
            || info.base_input_level() != 0 && get_io_type() == IoType::Compaction
        {
            set_io_type(IoType::Other);
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
                DBBackgroundErrorReason::ManifestWrite => "manifest_write",
                DBBackgroundErrorReason::FlushNoWAL => "flush_no_wal",
                DBBackgroundErrorReason::ManifestWriteNoWAL => "manifest_write_no_wal",
            };

            if err.starts_with("Corruption") || err.starts_with("IO error") {
                if let Some(scheduler) = self.sst_recovery_scheduler.as_ref() {
                    if let Some(path) = resolve_sst_filename_from_err(&err) {
                        warn!(
                            "detected rocksdb background error";
                            "reason" => r,
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
// ```text
// 1. Corruption: Sst file size mismatch: /qps/data/tikv-10014/db/000398.sst. Size recorded in manifest 6975, actual size 6959
// 2. Corruption: Bad table magic number: expected 9863518390377041911, found 759105309091689679 in /qps/data/tikv-10014/db/000021.sst
// ```
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

pub struct RocksPersistenceListener(PersistenceListener);

impl RocksPersistenceListener {
    pub fn new(listener: PersistenceListener) -> RocksPersistenceListener {
        RocksPersistenceListener(listener)
    }
}

impl rocksdb::EventListener for RocksPersistenceListener {
    fn on_memtable_sealed(&self, info: &MemTableInfo) {
        // Note: first_seqno is effectively the smallest seqno of memtable.
        // earliest_seqno has ambiguous semantics.
        self.0.on_memtable_sealed(
            info.cf_name().to_string(),
            info.first_seqno(),
            info.largest_seqno(),
        );
    }

    fn on_flush_begin(&self, _: &FlushJobInfo) {
        fail::fail_point!("on_flush_begin");
    }

    fn on_flush_completed(&self, job: &FlushJobInfo) {
        let num = match job
            .file_path()
            .file_prefix()
            .and_then(|n| n.to_str())
            .map(|n| n.parse())
        {
            Some(Ok(n)) => n,
            _ => {
                slog_global::error!("failed to parse file number"; "path" => job.file_path().display());
                0
            }
        };
        self.0
            .on_flush_completed(job.cf_name(), job.largest_seqno(), num);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    };

    use engine_traits::{
        ApplyProgress, FlushState, MiscExt, StateStorage, SyncMutable, CF_DEFAULT, DATA_CFS,
    };
    use tempfile::Builder;

    use super::*;
    use crate::{util, RocksCfOptions, RocksDbOptions};

    #[test]
    fn test_resolve_sst_filename() {
        let err = "Corruption: Sst file size mismatch: /qps/data/tikv-10014/db/000398.sst. Size recorded in manifest 6975, actual size 6959";
        let filename = resolve_sst_filename_from_err(err).unwrap();
        assert_eq!(filename, "/000398.sst");
    }

    type Record = (u64, u64, ApplyProgress);

    #[derive(Default)]
    struct MemStorage {
        records: Mutex<Vec<Record>>,
    }

    impl StateStorage for MemStorage {
        fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: ApplyProgress) {
            self.records
                .lock()
                .unwrap()
                .push((region_id, tablet_index, pr));
        }
    }

    struct FlushTrack {
        sealed: Mutex<Sender<()>>,
        block_flush: Arc<Mutex<()>>,
    }

    impl rocksdb::EventListener for FlushTrack {
        fn on_memtable_sealed(&self, _: &MemTableInfo) {
            let _ = self.sealed.lock().unwrap().send(());
        }

        fn on_flush_begin(&self, _: &FlushJobInfo) {
            drop(self.block_flush.lock().unwrap())
        }
    }
}
