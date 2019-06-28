pub use crate::default::*;

pub type Allocator = tcmalloc::TCMalloc;

pub const fn allocator() -> Allocator {
    tcmalloc::TCMalloc
}
