// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::rocks_metrics::*;

use file_system::{delete_file_if_exist, get_io_type, set_io_type, IOType};
use lazy_static::lazy_static;
use regex::Regex;
use rocksdb::{
    CompactionJobInfo, DBBackgroundErrorReason, FlushJobInfo, IngestionInfo, SubcompactionJobInfo,
    WriteStallInfo,
};
use tikv_util::{error, set_panic_mark};

pub struct RocksEventListener {
    db_name: String,
}

impl RocksEventListener {
    pub fn new(db_name: &str) -> RocksEventListener {
        RocksEventListener {
            db_name: db_name.to_owned(),
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
    }

    fn on_background_error(&self, reason: DBBackgroundErrorReason, result: Result<(), String>) {
        assert!(result.is_err());
        if let Err(err) = result {
            let r = match reason {
                DBBackgroundErrorReason::Flush => "flush",
                DBBackgroundErrorReason::Compaction => "compaction",
                DBBackgroundErrorReason::WriteCallback => "write_callback",
                DBBackgroundErrorReason::MemTable => "memtable",
            };
            // Avoid tikv from restarting if rocksdb get corruption.
            if err.starts_with("Corruption") {
                set_panic_mark();
            }
            if (reason == DBBackgroundErrorReason::Flush
                || reason == DBBackgroundErrorReason::Compaction)
                && err.starts_with("IO error: No space left on device")
            {
                // recycle incomplete sst file
                lazy_static! {
                    static ref RE: Regex = Regex::new(
                        r"(?:While appending to file: )(?P<path>.+)(?:: No space left on device)"
                    )
                    .unwrap();
                }
                error!("rocksdb background error (disk full), incomplete sst file will be removed.";
                    "db"=>&self.db_name,
                    "reason"=>&r,
                    "error"=>&err
                );
                if let Some(c) = RE.captures(&err) {
                    if let Some(path) = c.name("path") {
                        let path = path.as_str();
                        // CRITICAL: remove incomplete sst file
                        match delete_file_if_exist(path) {
                            Ok(_) => {}
                            Err(err) => {
                                error!("remove incomplete sst file error."; "path"=>path,"err"=>format!("{}", err))
                            }
                        }
                    }
                }
                return;
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

#[cfg(test)]
mod tests {

    use regex::Regex;

    #[test]
    fn test_regex() {
        let s = "IO error: No space left on deviceWhile appending to file: /rocksfull/dbuHoG8a/000017.sst: No space left on device, Accumulated background error counts: 1".to_owned();
        let re =
            Regex::new(r"(?:While appending to file: )(?P<path>.+)(?:: No space left on device)")
                .unwrap();
        let c = re.captures(&s).unwrap();
        assert_eq!(
            c.name("path").unwrap().as_str(),
            "/rocksfull/dbuHoG8a/000017.sst"
        );
    }
}
