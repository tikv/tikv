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

use std::cmp;
use std::sync::mpsc::Sender;
use std::sync::Mutex;

use rocksdb::{self, CompactionJobInfo, FlushJobInfo, IngestionInfo};
use util::rocksdb::engine_metrics::*;

use super::properties::SizeProperties;

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

pub struct CompactedEvent {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
    pub output_level: i32,
}

impl CompactedEvent {
    pub fn new(
        cf: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        total_input_bytes: u64,
        total_output_bytes: u64,
        output_level: i32,
    ) -> CompactedEvent {
        CompactedEvent {
            cf: cf,
            start_key: start_key,
            end_key: end_key,
            total_input_bytes: total_input_bytes,
            total_output_bytes: total_output_bytes,
            output_level: output_level,
        }
    }
}

pub struct CompactionListener {
    notifier: Mutex<Sender<CompactedEvent>>,
}

impl CompactionListener {
    pub fn new(notifier: Sender<CompactedEvent>) -> CompactionListener {
        CompactionListener {
            notifier: Mutex::new(notifier),
        }
    }
}

impl rocksdb::EventListener for CompactionListener {
    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        let cf = info.cf_name().to_owned();
        let output_level = info.output_level();
        let total_input_bytes = info.total_input_bytes();
        let total_output_bytes = info.total_output_bytes();

        let mut smallest_key = None;
        let mut largest_key = None;
        let iter = info.table_properties().into_iter();
        for (_, properties) in iter {
            if let Ok(props) = SizeProperties::decode(properties.user_collected_properties()) {
                if let Some(smallest) = props.smallest_key() {
                    if let Some(s) = smallest_key {
                        smallest_key = Some(cmp::min(s, smallest));
                    } else {
                        smallest_key = Some(smallest);
                    }
                }
                if let Some(largest) = props.largest_key() {
                    if let Some(l) = largest_key {
                        largest_key = Some(cmp::max(l, largest));
                    } else {
                        largest_key = Some(largest);
                    }
                }
            } else {
                return;
            }
        }

        if smallest_key.is_none() || largest_key.is_none() {
            return;
        }
        self.notifier
            .lock()
            .unwrap()
            .send(CompactedEvent::new(
                cf,
                smallest_key.unwrap(),
                largest_key.unwrap(),
                total_input_bytes,
                total_output_bytes,
                output_level,
            ))
            .unwrap();
    }
}
