use std::ffi::CString;

pub struct RocksLevelRegionAccessor<F: engine_traits::LevelRegionAccessor>(pub F);

impl<F: engine_traits::LevelRegionAccessor> rocksdb::LevelRegionAccessor
for RocksLevelRegionAccessor<F>
{
    fn name(&self) -> &CString {
        self.0.name()
    }

    fn level_regions(
        &self,
        req: &rocksdb::LevelRegionAccessorRequest,
    ) -> rocksdb::LevelRegionAccessorResults {
        
    }
}