// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::path::Path;

use crate::properties::{RangeProperties, UserCollectedPropertiesDecoder};
use engine::rocks::EventListener;
use engine_traits::CompactionJobInfo;
use rocksdb::{
    CompactionJobInfo as RawCompactionJobInfo, CompactionReason, TablePropertiesCollectionView,
};
use tikv_util::collections::hash_set_with_capacity;

pub struct RocksCompactionJobInfo<'a>(&'a RawCompactionJobInfo);

impl<'a> RocksCompactionJobInfo<'a> {
    pub fn from_raw(raw: &'a RawCompactionJobInfo) -> Self {
        RocksCompactionJobInfo(raw)
    }

    pub fn into_raw(self) -> &'a RawCompactionJobInfo {
        self.0
    }
}

impl CompactionJobInfo for RocksCompactionJobInfo<'_> {
    type TablePropertiesCollectionView = TablePropertiesCollectionView;
    type CompactionReason = CompactionReason;

    fn status(&self) -> Result<(), String> {
        self.0.status()
    }

    fn cf_name(&self) -> &str {
        self.0.cf_name()
    }

    fn input_file_count(&self) -> usize {
        self.0.input_file_count()
    }

    fn input_file_at(&self, pos: usize) -> &Path {
        self.0.input_file_at(pos)
    }

    fn output_file_count(&self) -> usize {
        self.0.output_file_count()
    }

    fn output_file_at(&self, pos: usize) -> &Path {
        self.0.output_file_at(pos)
    }

    fn table_properties(&self) -> &Self::TablePropertiesCollectionView {
        self.0.table_properties()
    }

    fn elapsed_micros(&self) -> u64 {
        self.0.elapsed_micros()
    }

    fn num_corrupt_keys(&self) -> u64 {
        self.0.num_corrupt_keys()
    }

    fn output_level(&self) -> i32 {
        self.0.output_level()
    }

    fn input_records(&self) -> u64 {
        self.0.input_records()
    }

    fn output_records(&self) -> u64 {
        self.0.output_records()
    }

    fn total_input_bytes(&self) -> u64 {
        self.0.total_input_bytes()
    }

    fn total_output_bytes(&self) -> u64 {
        self.0.total_output_bytes()
    }

    fn compaction_reason(&self) -> Self::CompactionReason {
        self.0.compaction_reason()
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
        info: &RocksCompactionJobInfo,
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

pub type Filter = fn(&RocksCompactionJobInfo) -> bool;

pub struct CompactionListener {
    ch: Box<dyn Fn(CompactedEvent) + Send + Sync>,
    filter: Option<Filter>,
}

impl CompactionListener {
    pub fn new(
        ch: Box<dyn Fn(CompactedEvent) + Send + Sync>,
        filter: Option<Filter>,
    ) -> CompactionListener {
        CompactionListener { ch, filter }
    }
}

impl EventListener for CompactionListener {
    fn on_compaction_completed(&self, info: &RawCompactionJobInfo) {
        let info = &RocksCompactionJobInfo::from_raw(info);
        if info.status().is_err() {
            return;
        }

        if let Some(ref f) = self.filter {
            if !f(info) {
                return;
            }
        }

        let mut input_files = hash_set_with_capacity(info.input_file_count());
        let mut output_files = hash_set_with_capacity(info.output_file_count());
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
            let ucp = UserCollectedPropertiesDecoder(properties.user_collected_properties());
            if let Ok(prop) = RangeProperties::decode(&ucp) {
                if input_files.contains(file) {
                    input_props.push(prop);
                } else if output_files.contains(file) {
                    output_props.push(prop);
                }
            } else {
                warn!("Decode size properties from sst file failed");
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
