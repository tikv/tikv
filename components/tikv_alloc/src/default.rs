use crate::{AllocStats, Error};

pub use std::io::Error as AllocatorError;

pub fn dump_stats() -> String {
    String::new()
}
pub fn dump_prof(_path: Option<&str>) {}

pub fn fetch_stats() -> Result<Option<AllocStats>, Error> {
    Ok(None)
}
