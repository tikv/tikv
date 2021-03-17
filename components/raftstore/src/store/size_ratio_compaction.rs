// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::Cell, ffi::CString};

use crate::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use engine_traits::{
    LevelRegionAccessor, LevelRegionBoundaries,
    LevelRegionAccessorRequest, LevelRegionAccessorResult,
};
use keys::{data_key, data_end_key};

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
        &SIZE_RATIO_COMPACTION
    }

    fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResult {
        match self
            .accessor
            .get_regions_in_range(req.smallest_user_key, req.largest_user_key)
        {
            Ok(regions) => {
                let mut boundaries = regions
                    .iter()
                    .map(|region| LevelRegionBoundaries{start_key: data_key(&region.start_key),
                        end_key: data_end_key(&region.end_key)}).collect();
                LevelRegionAccessorResult{
                    regions: boundaries,
                }
            }
            Err(e) => {
                warn!("failed to get region boundaries"; "err" => ?e);
                LevelRegionAccessorResult{
                    regions: Vec::new(),
                }
            }
        }
    }
}
