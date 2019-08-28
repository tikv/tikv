// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(core_intrinsics)]
#![feature(ptr_offset_from)]

#[macro_use]
extern crate static_assertions;
#[macro_use]
extern crate failure;
#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod buffer;
pub mod byte;
mod convert;
mod error;
pub mod number;

pub mod prelude {
    pub use super::buffer::{BufferReader, BufferWriter};
    pub use super::byte::{CompactByteDecoder, CompactByteEncoder};
    pub use super::byte::{MemComparableByteDecoder, MemComparableByteEncoder};
    pub use super::number::{NumberDecoder, NumberEncoder};
}

pub use self::error::{Error, Result};
