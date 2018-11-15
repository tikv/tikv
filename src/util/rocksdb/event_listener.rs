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

use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::AddAssign;
use std::{cmp, u64};
use std::convert::TryFrom;

use rocksdb::{self, CompactionJobInfo, FlushJobInfo, IngestionInfo};
use util::rocksdb::engine_metrics::*;
use util::time::{duration_to_sec, Instant};

use super::properties::RangeProperties;

pub struct MetricsListener {
    db_name: String,
}

impl MetricsListener {
    pub fn new(db_name: &str) -> MetricsListener {
        MetricsListener {
            db_name: db_name.to_owned(),
        }
    }
}

impl rocksdb::EventListener for MetricsListener {
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
            .inc_by(info.num_corrupt_keys() as i64);
    }

    fn on_external_file_ingested(&self, info: &IngestionInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.cf_name(), "ingestion"])
            .inc();
    }
}

#[derive(Default)]
pub struct CompactionStats {
    pub elapsed: u64, // micros
    pub bytes: u64,
    pub keys: u64,
    pub is_level0: bool,
}

impl AddAssign for CompactionStats {
    fn add_assign(&mut self, rhs: Self) {
        self.elapsed += rhs.elapsed;
        self.bytes += rhs.bytes;
        self.keys += rhs.keys;
    }
}

pub struct StorageListener {
    ch: Box<Fn(CompactionStats) + Send + Sync>,
}

impl StorageListener {
    pub fn new(ch: Box<Fn(CompactionStats) + Send + Sync>) -> StorageListener {
        StorageListener { ch: ch }
    }
}

thread_local!(static FLUSH_BEGIN: RefCell<Instant> = RefCell::new(Instant::Monotonic(0)));

impl rocksdb::EventListener for StorageListener {
    fn on_flush_begin(&self, info: &FlushJobInfo) {
        FLUSH_BEGIN.with(|f| *f.borrow_mut() = Instant::now())
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let elapsed = FLUSH_BEGIN.with(|f| {
            let begin = *f.borrow();
            begin.elapsed()
        });

        STORE_ENGINE_FLUSH_DURATIONS_VEC
            .with_label_values(&["kv", info.cf_name()])
            .observe(duration_to_sec(elapsed));

        let prop = info.table_properties();
        let flush_size = prop.data_size() + prop.index_size() + prop.filter_size();

        (self.ch)(CompactionStats {
            elapsed: u64::try_from(elapsed.as_micros()).unwrap_or(0),
            bytes: flush_size,
            keys: prop.num_entries(),
            is_level0: true,
        });
    }

    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        (self.ch)(CompactionStats {
            elapsed: info.elapsed_micros(),
            bytes: info.total_output_bytes(),
            keys: info.output_records(),
            is_level0: false,
        });
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
