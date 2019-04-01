use tcmalloc::TCMalloc;

pub type Allocator = tcmalloc::TCMalloc;
pub const fn allocator() -> Allocator {
    tcmalloc::TCMalloc
}