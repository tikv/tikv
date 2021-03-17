use std::ffi::CString;

pub struct RocksLevelRegionAccessor<A: engine_traits::LevelRegionAccessor>(pub A);

impl<A: engine_traits::LevelRegionAccessor> rocksdb::LevelRegionAccessor
for RocksLevelRegionAccessor<A>
{
    fn name(&self) -> &CString {
        self.0.name()
    }

    fn level_regions (
        &self,
        request: &rocksdb::LevelRegionAccessorRequest,
    ) -> rocksdb::LevelRegionAccessorResult {
        let req = engine_traits::LevelRegionAccessorRequest {
            smallest_user_key: request.smallest_user_key,
            largest_user_key: request.largest_user_key,
        };
        let result = self.0.level_regions(&req);
        let mut regions = Vec::new();
        for region in result.regions {
            regions.push(rocksdb::LevelRegionBoundaries{
                start_key: region.start_key,
                end_key: region.end_key,
            });
        }
        rocksdb::LevelRegionAccessorResult {
            regions: regions,
        }
    }
}
