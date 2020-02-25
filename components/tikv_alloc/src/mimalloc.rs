pub use crate::default::*;

pub type Allocator = mimallocator::Mimalloc;

pub const fn allocator() -> Allocator {
    mimallocator::Mimalloc
}
