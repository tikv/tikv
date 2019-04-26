// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(core_intrinsics)]
#![feature(ptr_offset_from)]

#[macro_use]
extern crate quick_error;
#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod buffer;
mod byte;
mod convert;
mod error;
mod number;

pub mod prelude {
    pub use super::buffer::{BufferReader, BufferWriter};
    pub use super::byte::MemComparableByteCodec;
    pub use super::number::{BufferNumberDecoder, BufferNumberEncoder};
}

pub use self::buffer::{BufferReader, BufferWriter};
pub use self::byte::MemComparableByteCodec;
pub use self::error::{Error, Result};
pub use self::number::NumberCodec;
