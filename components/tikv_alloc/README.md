#Usage

```rust
// Jemallocator
$ cargo build
// Tcmalloc
$ cargo build --no-default-features --features="tcmalloc"
// System Allocator
$ cargo build --no-default-features --features="system-alloc"
```

Or:

```rust
// Jemallocator
$ make build
// Tcmalloc
$ TCMALLOC=1 make build
// System Allocator
$ SYSTEM_ALLOC=1 make build
```

# Note

Memory allocation will use System alloc on Windows