// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::{
        BTreeMap,
        Bound::{Excluded, Included, Unbounded},
    },
    path::Path,
};

use collections::hash_set_with_capacity;
use engine_traits::{CompactedEvent, CompactionJobInfo};
use rocksdb::{
    CompactionJobInfo as RawCompactionJobInfo, CompactionReason, TablePropertiesCollectionView,
};
use tikv_util::warn;

use crate::{
    properties::{RangeProperties, UserCollectedPropertiesDecoder},
    raw::EventListener,
};

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

    fn num_input_files_at_output_level(&self) -> usize {
        self.0.num_input_files_at_output_level()
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

    fn base_input_level(&self) -> i32 {
        self.0.base_input_level()
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

pub struct RocksCompactedEvent {
    pub cf: String,
    pub output_level: i32,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub input_props: Vec<RangeProperties>,
    pub output_props: Vec<RangeProperties>,
}

impl RocksCompactedEvent {
    pub fn new(
        info: &RocksCompactionJobInfo<'_>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        input_props: Vec<RangeProperties>,
        output_props: Vec<RangeProperties>,
    ) -> RocksCompactedEvent {
        RocksCompactedEvent {
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

impl CompactedEvent for RocksCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        if self.total_input_bytes > self.total_output_bytes {
            self.total_input_bytes - self.total_output_bytes
        } else {
            0
        }
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        let total_bytes_declined = self.total_bytes_declined();
        total_bytes_declined < split_check_diff
            || total_bytes_declined * 10 < self.total_input_bytes
    }

    fn output_level_label(&self) -> String {
        self.output_level.to_string()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        // Calculate influenced regions.
        let mut influenced_regions = vec![];
        for (end_key, region_id) in
            ranges.range((Excluded(self.start_key), Included(self.end_key.clone())))
        {
            influenced_regions.push((region_id, end_key.clone()));
        }
        if let Some((end_key, region_id)) = ranges.range((Included(self.end_key), Unbounded)).next()
        {
            influenced_regions.push((region_id, end_key.clone()));
        }

        // Calculate declined bytes for each region.
        // `end_key` in influenced_regions are in incremental order.
        let mut region_declined_bytes = vec![];
        let mut last_end_key: Vec<u8> = vec![];
        for (region_id, end_key) in influenced_regions {
            let mut old_size = 0;
            for prop in &self.input_props {
                old_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
            }
            let mut new_size = 0;
            for prop in &self.output_props {
                new_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
            }
            last_end_key = end_key;

            // Filter some trivial declines for better performance.
            if old_size > new_size && old_size - new_size > bytes_threshold {
                region_declined_bytes.push((*region_id, old_size - new_size));
            }
        }

        region_declined_bytes
    }

    fn cf(&self) -> &str {
        &*self.cf
    }
}

pub type Filter = fn(&RocksCompactionJobInfo<'_>) -> bool;

pub struct CompactionListener {
    ch: Box<dyn Fn(RocksCompactedEvent) + Send + Sync>,
    filter: Option<Filter>,
}

impl CompactionListener {
    pub fn new(
        ch: Box<dyn Fn(RocksCompactedEvent) + Send + Sync>,
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

        (self.ch)(RocksCompactedEvent::new(
            info,
            smallest_key.unwrap(),
            largest_key.unwrap(),
            input_props,
            output_props,
        ));
    }
}
