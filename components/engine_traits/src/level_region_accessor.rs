use std::ffi::CString;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorRequest<'a> {
    pub smallest_user_key: &'a [u8],
    pub largest_user_key: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionBoundaries<'a> {
    pub start_key: &'a [u8],
    pub end_key: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorResult<'a> {
    pub regions:  Vec<LevelRegionBoundaries<'a>>,
}

pub trait LevelRegionAccessor {
    fn name(&self) -> &Cstring;
    fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResult;
}
