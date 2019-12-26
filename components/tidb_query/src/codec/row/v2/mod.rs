// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use bitflags::bitflags;

// Prior to v2, the first byte is not version code, but datum type.
// From v2, it's used for version code, and the value starts from 128, to be compatible.
pub const CODEC_VERSION: u8 = 128;

bitflags! {
    #[derive(Default)]
    struct Flags: u8 {
        const BIG = 1;
    }
}

mod row_slice;

#[cfg(test)]
mod encoder;
