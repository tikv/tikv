#[cfg(any(windows, fuzzing))]
use compile_warning::compile_warning;
#[cfg(any(windows, fuzzing))]
compile_warning!("Memory allocation uses System alloc on Windows or Fuzzing");

pub use crate::default::*;

pub type Allocator = std::alloc::System;
pub const fn allocator() -> Allocator {
    std::alloc::System
}
