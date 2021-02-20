// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod store;
mod ttl;

pub use store::RawStore;
#[cfg(test)]
pub use ttl::TEST_CURRENT_TS;
