// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use crate::properties::RangeProperties;
use engine::rocks::CompactionJobInfo;

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
