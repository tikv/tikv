// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(core_intrinsics)]
#![feature(min_specialization)]

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
    pub use super::{
        buffer::{BufferReader, BufferWriter},
        byte::{
            CompactByteDecoder, CompactByteEncoder, MemComparableByteDecoder,
            MemComparableByteEncoder,
        },
        number::{NumberDecoder, NumberEncoder},
    };
}

pub use self::error::{Error, ErrorInner, Result};
