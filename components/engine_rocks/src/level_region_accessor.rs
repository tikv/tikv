use std::ffi::CString;
use std::convert::TryInto;

pub struct RocksLevelRegionAccessor<F: engine_traits::LevelRegionAccessor>(pub F);

impl<F: engine_traits::LevelRegionAccessor> rocksdb::LevelRegionAccessor
for RocksLevelRegionAccessor<F>
{
    fn name(&self) -> &CString {
        self.0.name()
    }

    fn level_regions(
        &self,
        request: &rocksdb::LevelRegionAccessorRequest,
    ) -> rocksdb::LevelRegionAccessorResult {
        let req = engine_traits::LevelRegionAccessorRequest {
            smallest_user_key: request.smallest_user_key,
            largest_user_key: request.largest_user_key,
        };
        let regions = self.0.level_regions(&req).regions;
        rocksdb::LevelRegionAccessorResult {
            regions: regions.as_ptr(),
            region_count: regions.len() as i32,
        }
    }
}