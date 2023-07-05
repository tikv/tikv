// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

use super::error::{ProfError, ProfResult};
use crate::AllocStats;

pub fn dump_stats() -> String {
    String::new()
}
pub fn dump_prof(_path: &str) -> ProfResult<()> {
    Err(ProfError::MemProfilingNotEnabled)
}

pub fn fetch_stats() -> io::Result<Option<AllocStats>> {
    Ok(None)
}

pub fn activate_prof() -> ProfResult<()> {
    Err(ProfError::MemProfilingNotEnabled)
}

pub fn deactivate_prof() -> ProfResult<()> {
    Err(ProfError::MemProfilingNotEnabled)
}

/// # Safety
///
/// It is safe. The unsafe marker is just for matching the function signature.
/// (So clippy will get happy even jemalloc isn't enabled.)
pub unsafe fn add_thread_memory_accessor() {}

pub fn remove_thread_memory_accessor() {}

pub fn iterate_thread_allocation_stats(_f: impl FnMut(&str, u64, u64)) {}
