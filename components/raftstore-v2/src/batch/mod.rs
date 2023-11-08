// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the specialized implementation of batch systems.
//!
//! StoreSystem is used for polling raft state machines, ApplySystem is used for
//! applying logs.

mod store;

pub use store::{create_store_batch_system, StoreContext, StoreRouter, StoreSystem};
