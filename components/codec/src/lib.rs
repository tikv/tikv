// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(core_intrinsics)]
#![feature(min_specialization)]

#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod buffer;
pub mod byte_v1;
pub mod byte_v2;
mod convert;
pub mod error;
pub mod number_v1;
pub mod number_v2;
pub mod stream_event;

pub mod prelude {
    pub use super::{
        buffer::{BufferReader, BufferWriter},
        byte_v2::{
            CompactByteDecoder, CompactByteEncoder, MemComparableByteDecoder,
            MemComparableByteEncoder,
        },
        number_v2::{NumberDecoder, NumberEncoder},
    };
}

pub use self::error::{Error, ErrorInner, Result};

pub type BytesSlice<'a> = &'a [u8];

#[inline]
pub fn read_slice<'a>(data: &mut BytesSlice<'a>, size: usize) -> Result<BytesSlice<'a>> {
    if data.len() >= size {
        let buf = &data[0..size];
        *data = &data[size..];
        Ok(buf)
    } else {
        Err(ErrorInner::eof().into())
    }
}
