// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub const CODEC_VERSION: u8 = 128;
const LARGE_ID: i64 = 255;

mod decoder;
mod encoder;

pub use decoder::RowDecoder;
pub use encoder::encode;
