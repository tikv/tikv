use std::ffi::CString;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorRequest<'a> {
    pub smallest_user_key: &'a [u8],
    pub largest_user_key: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionBoundaries {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorResult {
    pub regions: Vec<LevelRegionBoundaries>,
}

pub trait LevelRegionAccessor<'a> {
    fn name(&self) -> &CString;
    fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResult;
}
