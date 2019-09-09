// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use crate::raftstore::coprocessor::properties::RangeProperties;
use engine::rocks::{CompactionJobInfo, EventListener};
use tikv_util::collections::hash_set_with_capacity;

pub use storage_types::compacted_event::CompactedEvent;

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
            if let Ok(prop) = RangeProperties::decode(properties.user_collected_properties()) {
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
