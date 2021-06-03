// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::default::*;

pub type Allocator = std::alloc::System;
pub const fn allocator() -> Allocator {
    std::alloc::System
}
