// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(thread_id_value)]

/// All mods end up with `_impls` impl structs defined in interface.
/// Other mods which define and impl structs should not end up with name
/// `_impls`.

#[allow(dead_code)]
pub mod interfaces;
// All ffi impls that not related to raft domain.
pub mod basic_ffi_impls;
// All ffi impls that related to raft domain, but not related to proxy helper
// context.
pub mod domain_impls;
// All ffi impls that related to engine store helper context.
pub mod context_impls;
pub mod encryption_impls;
// FFI directly related with EngineStoreServerHelper.
pub mod engine_store_helper_impls;
// FFI directly related with RaftStoreProxyFFIHelper.
pub mod raftstore_proxy;
pub mod raftstore_proxy_helper_impls;
pub mod read_index_helper;
// FFI releated with reading from SST/RocksDB files.
pub mod jemalloc_utils;
pub mod snapshot_reader_impls;
pub mod utils;

pub use self::{
    basic_ffi_impls::*, domain_impls::*, encryption_impls::*, engine_store_helper_impls::*,
    interfaces::root::DB as interfaces_ffi, raftstore_proxy::*, raftstore_proxy_helper_impls::*,
};
