// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(not(feature = "enable-pagestorage"))]
mod engine;
#[cfg(not(feature = "enable-pagestorage"))]
pub use engine::*;

#[cfg(not(feature = "enable-pagestorage"))]
pub mod write_batch;
#[cfg(not(feature = "enable-pagestorage"))]
pub use write_batch::*;
