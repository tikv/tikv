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

    #[test]
    fn test_persistence_listener() {
        let temp_dir = Builder::new()
            .prefix("test_persistence_listener")
            .tempdir()
            .unwrap();
        let (region_id, tablet_index) = (2, 3);

        let storage = Arc::new(MemStorage::default());
        let state = Arc::new(FlushState::new(0));
        let listener =
            PersistenceListener::new(region_id, tablet_index, state.clone(), storage.clone());
        let mut db_opt = RocksDbOptions::default();
        db_opt.add_event_listener(RocksPersistenceListener::new(listener));
        let (tx, rx) = mpsc::channel();
        let block_flush = Arc::new(Mutex::new(()));
        db_opt.add_event_listener(FlushTrack {
            sealed: Mutex::new(tx),
            block_flush: block_flush.clone(),
        });

        let mut cf_opts: Vec<_> = DATA_CFS
            .iter()
            .map(|cf| (*cf, RocksCfOptions::default()))
            .collect();
        cf_opts[0].1.set_max_write_buffer_number(4);
        cf_opts[0].1.set_min_write_buffer_number_to_merge(2);
        cf_opts[0].1.set_write_buffer_size(1024);
        cf_opts[0].1.set_disable_auto_compactions(true);
        let db = util::new_engine_opt(temp_dir.path().to_str().unwrap(), db_opt, cf_opts).unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        let sst_count = || {
            std::fs::read_dir(temp_dir.path())
                .unwrap()
                .filter(|p| {
                    let p = match p {
                        Ok(p) => p,
                        Err(_) => return false,
                    };
                    p.path().extension().map_or(false, |ext| ext == "sst")
                })
                .count()
        };
        // Although flush is triggered, but there is nothing to flush.
        assert_eq!(sst_count(), 0);
        assert_eq!(storage.records.lock().unwrap().len(), 0);

        // Flush one key should work.
        state.set_applied_index(2);
        db.put_cf(CF_DEFAULT, b"k0", b"v0").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(sst_count(), 1);
        let record = storage.records.lock().unwrap().pop().unwrap();
        assert_eq!(storage.records.lock().unwrap().len(), 0);
        assert_eq!(record.0, region_id);
        assert_eq!(record.1, tablet_index);
        assert_eq!(record.2.applied_index(), 2);

        // When puts and deletes are mixed, the puts may be deleted during flush.
        state.set_applied_index(3);
        db.put_cf(CF_DEFAULT, b"k0", b"v0").unwrap();
        db.delete_cf(CF_DEFAULT, b"k0").unwrap();
        db.delete_cf(CF_DEFAULT, b"k1").unwrap();
        db.put_cf(CF_DEFAULT, b"k1", b"v1").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(sst_count(), 2);
        let record = storage.records.lock().unwrap().pop().unwrap();
        assert_eq!(storage.records.lock().unwrap().len(), 0);
        assert_eq!(record.0, region_id);
        assert_eq!(record.1, tablet_index);
        assert_eq!(record.2.applied_index(), 3);
        // Detail check of `FlushProgress` will be done in raftstore-v2 tests.

        // Drain all the events.
        while rx.try_recv().is_ok() {}
        state.set_applied_index(4);
        let block = block_flush.lock();
        // Seal twice to trigger flush. Seal third to make a seqno conflict, in
        // which case flush largest seqno will be equal to seal earliest seqno.
        let mut key_count = 2;
        for i in 0..3 {
            while rx.try_recv().is_err() {
                db.put(format!("k{key_count}").as_bytes(), &[0; 512])
                    .unwrap();
                key_count += 1;
            }
            state.set_applied_index(5 + i);
        }
        drop(block);
        // Memtable is seal before put, so there must be still one KV in memtable.
        db.flush_cf(CF_DEFAULT, true).unwrap();
        rx.try_recv().unwrap();
        // There is 2 sst before this round, and then 4 are merged into 2, so there
        // should be 4 ssts.
        assert_eq!(sst_count(), 4);
        let records = storage.records.lock().unwrap();
        // Although it seals 4 times, but only create 2 SSTs, so only 2 records.
        assert_eq!(records.len(), 2);
        // The indexes of two merged flush state are 4 and 5, so merged value is 5.
        assert_eq!(records[0].2.applied_index(), 5);
        // The last two flush state is 6 and 7.
        assert_eq!(records[1].2.applied_index(), 7);
    }
}
