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

pub fn set_prof_sample(_rate: u64) -> ProfResult<()> {
    Err(ProfError::MemProfilingNotEnabled)
}

pub fn get_arena_count() -> u32 {
    0
}

pub fn set_thread_exclusive_arena(_enable: bool) {
    // Do nothing
}

pub fn is_profiling_active() -> bool {
    false
}

/// # Safety
///
/// It is safe. The unsafe marker is just for matching the function signature.
/// (So clippy will get happy even jemalloc isn't enabled.)
pub unsafe fn add_thread_memory_accessor() {}

pub fn remove_thread_memory_accessor() {}

pub fn iterate_thread_allocation_stats(_f: impl FnMut(&str, u64, u64)) {}

pub fn iterate_arena_allocation_stats(_f: impl FnMut(&str, u64, u64, u64)) {}

pub fn thread_allocate_exclusive_arena() -> ProfResult<()> {
    Ok(())
}
