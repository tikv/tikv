// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
    pub use super::byte::{
        CompactByteDecoder, CompactByteEncoder, MemComparableByteCodec, MemComparableByteDecoder,
        MemComparableByteEncoder,
    };
    pub use super::number::{NumberDecoder, NumberEncoder};
}

pub use self::buffer::{BufferReader, BufferWriter};
pub use self::byte::{encoded_bytes_len, encoded_compact_len, MemComparableByteCodec};
pub use self::error::{Error, Result};
pub use self::number::{NumberCodec, F64_SIZE, I64_SIZE, MAX_VARINT64_LENGTH, U64_SIZE};
