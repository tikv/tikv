use std::ffi::CString;

pub struct RocksLevelRegionAccessor<'a, A: engine_traits::LevelRegionAccessor<'a>>(pub &'a A);

impl<'a, A: engine_traits::LevelRegionAccessor<'a>> rocksdb::LevelRegionAccessor
for RocksLevelRegionAccessor<'a, A>
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
        self.0.level_regions(&req)
    }
}
