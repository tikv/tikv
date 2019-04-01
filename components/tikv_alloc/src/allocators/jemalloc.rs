pub type Allocator = jemallocator::Jemalloc;
pub const fn allocator() -> Allocator {
    jemallocator::Jemalloc
}