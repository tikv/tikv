pub use crate::default::*;

pub type Allocator = std::alloc::System;
pub const fn allocator() -> Allocator {
    std::alloc::System
}
