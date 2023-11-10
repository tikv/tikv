## Skiplist-rs
```text
    The Skiplist implementation is all in `skiplist/src`. 
```
### 1.Arena Memory Pool
```rs
pub struct Arena {
    len: AtomicUsize,
    cap: Cell<usize>,
    ptr: Cell<*mut u8>,
}
AgatedDb Arena Memeory Pool is above,here:
1.len is used to record how much memory is occupied.
2.cap is the whole capacity of Arena.
3.ptr is the initial address of Arena.

Some details I need to decribe:
1. when new a Arena memory pool, we will alloc a specific memory space, but in rust,
we need to use `memory::forget()', otherwise when the ownership of buf is dropped,
the memory is invalid.
2. when we need to release the memory, we will use `Vec::from_raw_parts`,makeing use of
ownership mechanism. 
3. the first 8-byte of Arena is invalid, we need this to express cornor cases.
4. Arena use 8-byte padding for memory alignment.
5. Arena's modifies is under Ordering::SeqCst, so we can avoid the cpu cache and instruction
reordering problems.
```