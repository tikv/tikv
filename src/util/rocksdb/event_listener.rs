// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use rocksdb::{self, CompactionJobInfo, FlushJobInfo, IngestionInfo};
use util::rocksdb::engine_metrics::*;

pub struct EventListener {
    db_name: String,
}

impl EventListener {
    pub fn new(db_name: &str) -> EventListener {
        EventListener {
            db_name: db_name.to_owned(),
        }
    }
}

impl rocksdb::EventListener for EventListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "flush"])
            .inc();
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
            .inc_by(info.num_corrupt_keys() as f64)
            .unwrap();
    }

    fn on_external_file_ingested(&self, info: &IngestionInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "ingestion"])
            .inc();
    }
}
