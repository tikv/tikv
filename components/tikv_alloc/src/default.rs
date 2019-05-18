// FIXME: https://github.com/tikv/tikv/pull/4524#discussion_r282754034 
#![allow(dead_code)]

use crate::AllocStats;
use std::io;

pub fn dump_stats() -> String {
    String::new()
}
pub fn dump_prof(_path: Option<&str>) {}

pub fn fetch_stats() -> io::Result<Option<AllocStats>> {
    Ok(None)
}
