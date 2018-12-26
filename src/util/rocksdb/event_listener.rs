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

use rocksdb::{
    self, CompactionJobInfo, FlushJobInfo, IngestionInfo, WriteStallCondition, WriteStallInfo,
};

use util::collections::HashSet;
use util::properties::RangeProperties;
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

#[inline]
fn tag_write_stall_condition(e: WriteStallCondition) -> &'static str {
    match e {
        WriteStallCondition::Normal => "normal",
        WriteStallCondition::Delayed => "delayed",
        WriteStallCondition::Stopped => "stopped",
    }
}

impl rocksdb::EventListener for EventListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "flush"])
            .inc();
        STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "triggered_writes_slowdown"])
            .set(info.triggered_writes_slowdown() as i64);
        STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "triggered_writes_stop"])
            .set(info.triggered_writes_stop() as i64);
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
            .inc_by(info.num_corrupt_keys() as i64);
        STORE_ENGINE_COMPACTION_REASON_VEC
            .with_label_values(&[
                &self.db_name,
                info.cf_name(),
                &info.compaction_reason().to_string(),
            ])
            .inc();
    }

    fn on_external_file_ingested(&self, info: &IngestionInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "ingestion"])
            .inc();
    }

    fn on_stall_conditions_changed(&self, info: &WriteStallInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "stall_conditions_changed"])
            .inc();

        STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .with_label_values(&[
                &self.db_name,
                info.cf_name(),
                tag_write_stall_condition(info.cur()),
            ])
            .set(1);
        STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .with_label_values(&[
                &self.db_name,
                info.cf_name(),
                tag_write_stall_condition(info.prev()),
            ])
            .set(0);
    }
}

pub struct CompactedEvent {
    pub cf: String,
    pub output_level: i32,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub input_props: Vec<RangeProperties>,
    pub output_props: Vec<RangeProperties>,
}

impl CompactedEvent {
    pub fn new(
        info: &CompactionJobInfo,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        input_props: Vec<RangeProperties>,
        output_props: Vec<RangeProperties>,
    ) -> CompactedEvent {
        CompactedEvent {
            cf: info.cf_name().to_owned(),
            output_level: info.output_level(),
            total_input_bytes: info.total_input_bytes(),
            total_output_bytes: info.total_output_bytes(),
            start_key,
            end_key,
            input_props,
            output_props,
        }
    }
}

pub type Filter = fn(&CompactionJobInfo) -> bool;

pub struct CompactionListener {
    ch: Box<Fn(CompactedEvent) + Send + Sync>,
    filter: Option<Filter>,
}

impl CompactionListener {
    pub fn new(
        ch: Box<Fn(CompactedEvent) + Send + Sync>,
        filter: Option<Filter>,
    ) -> CompactionListener {
        CompactionListener { ch, filter }
    }
}

impl rocksdb::EventListener for CompactionListener {
    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        if let Some(ref f) = self.filter {
            if !f(info) {
                return;
            }
        }

        let mut input_files = HashSet::with_capacity(info.input_file_count());
        let mut output_files = HashSet::with_capacity(info.output_file_count());
        for i in 0..info.input_file_count() {
            info.input_file_at(i)
                .to_str()
                .map(|x| input_files.insert(x.to_owned()));
        }
        for i in 0..info.output_file_count() {
            info.output_file_at(i)
                .to_str()
                .map(|x| output_files.insert(x.to_owned()));
        }
        let mut input_props = Vec::with_capacity(info.input_file_count());
        let mut output_props = Vec::with_capacity(info.output_file_count());
        let iter = info.table_properties().into_iter();
        for (file, properties) in iter {
            if let Ok(prop) = RangeProperties::decode(properties.user_collected_properties()) {
                if input_files.contains(file) {
                    input_props.push(prop);
                } else if output_files.contains(file) {
                    output_props.push(prop);
                }
            } else {
                warn!("Decode size properties from sst file failed.");
                return;
            }
        }

        if input_props.is_empty() && output_props.is_empty() {
            return;
        }

        let mut smallest_key = None;
        let mut largest_key = None;
        for prop in &input_props {
            if let Some(smallest) = prop.smallest_key() {
                if let Some(s) = smallest_key {
                    smallest_key = Some(cmp::min(s, smallest));
                } else {
                    smallest_key = Some(smallest);
                }
            }
            if let Some(largest) = prop.largest_key() {
                if let Some(l) = largest_key {
                    largest_key = Some(cmp::max(l, largest));
                } else {
                    largest_key = Some(largest);
                }
            }
        }

        if smallest_key.is_none() || largest_key.is_none() {
            return;
        }

        (self.ch)(CompactedEvent::new(
            info,
            smallest_key.unwrap(),
            largest_key.unwrap(),
            input_props,
            output_props,
        ));
    }
}
