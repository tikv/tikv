// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{MvccProperties, MvccPropertiesExt, RangeStats, Result, StatsChangeEvent};
use txn_types::TimeStamp;

use crate::engine::PanicEngine;

pub struct PanicStatsChangeEvent;

impl StatsChangeEvent for PanicStatsChangeEvent {
    fn cf(&self) -> &str {
        panic!()
    }

    fn get_input_range_stats(&self) -> Option<&RangeStats> {
        panic!()
    }

    fn get_output_range_stats(&self) -> Option<&RangeStats> {
        panic!()
    }
}

impl MvccPropertiesExt for PanicEngine {
    type StatsChangeEvent = PanicStatsChangeEvent;
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties> {
        panic!()
    }
}
