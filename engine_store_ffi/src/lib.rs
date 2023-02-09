// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(drain_filter)]
#![feature(let_chains)]

pub mod core;
pub mod engine;
pub mod ffi;
pub mod observer;

// Be discreet when expose inner mods by pub use.
// engine_store_ffi crate includes too many mods here.
// Thus it is better to directly refer to the mod itself.

// TODO We may integrate engine_tiflash if possible.
pub use engine_tiflash::EngineStoreConfig;
pub type TiFlashEngine = engine_tiflash::RocksEngine;
