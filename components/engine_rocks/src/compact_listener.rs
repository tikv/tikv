// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use crate::properties::{RangeProperties, UserCollectedPropertiesDecoder};
use engine::rocks::{CompactionJobInfo, EventListener};
use tikv_util::collections::hash_set_with_capacity;

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
    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
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
