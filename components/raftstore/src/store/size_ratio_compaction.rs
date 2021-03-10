// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, ffi::CString};

use crate::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use engine_traits::{
    LevelRegionAccessor, LevelRegionAccessorContext, LevelRegionAccessorRequest,
    LevelRegionAccessorResult,
};
use keys::data_end_key;

lazy_static! {
    static ref SIZE_RATIO_COMPACTION: CString = CString::new(b"SizeRatioCompaction".to_vec()).unwrap();
}

pub struct SizeRatioCompaction {
    accessor: RegionInfoAccessor,
}

impl SizeRatioCompaction {
    pub fn new(
        accessor: RegionInfoAccessor,
    ) -> Self {
        SizeRatioCompaction {
            accessor,
        }
    }
}

// Update to implement engine_traits::SstPartitionerFactory instead once we move to use abstracted
// ColumnFamilyOptions in src/config.rs.
impl LevelRegionAccessor for SizeRatioCompaction {

    fn name(&self) -> &CString {
        &COMPACTION_GUARD
    }

    fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResults {
        match self
            .accessor
            .get_regions_in_range(req.smallest_key, req.largest_key)
        {
            Ok(regions) => {

            }
            Err(e) => {
                warn!("failed to create compaction guard generator"; "err" => ?e);
                None
            }
        }
    }
}
