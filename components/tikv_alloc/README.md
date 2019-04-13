#Usage

```rust
// Jemallocator
$ cargo build
// Tcmalloc
$ cargo build --features="tcmalloc"
// System Allocator
$ cargo build --features="system-alloc"
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