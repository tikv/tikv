// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use bitflags::bitflags;
pub const CODEC_VERSION: u8 = 128;

bitflags! {
    struct Flags: u8 {
        const SMALL = 0;
        const BIG = 1;
    }
}

mod row_slice;

#[cfg(test)]
mod encoder;
