// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use file_system::{get_io_type, set_io_type, IOType};
use rocksdb::{
    CompactionJobInfo, DBBackgroundErrorReason, FlushJobInfo, IngestionInfo, MutableStatus,
    SubcompactionJobInfo, WriteStallInfo,
};
use tikv_util::set_panic_mark;

use crate::rocks_metrics::*;

// Message for RocksDB status subcode kNoSpace.
const NO_SPACE_ERROR: &str = "IO error: No space left on device";

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
