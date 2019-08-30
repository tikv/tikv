// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/*!

This module provides an implementation of mpsc channel based on
crossbeam_channel. Comparing to the crossbeam_channel, this implementation
supports closed detection and try operations.

*/
pub mod batch;
pub mod channel;
pub mod queue;

pub use queue::*;
