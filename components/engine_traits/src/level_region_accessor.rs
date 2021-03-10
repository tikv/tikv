use std::ffi::CString;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorRequest<'a> {

}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccesorResult {

}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorContext<'a> {

}

pub trait LevelRegionAccessor {
    fn name(&self) -> &Cstring;
    fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResult;
}
