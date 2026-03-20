// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod ascii;
mod gb18030;
mod gb18030_data;
mod gbk;
mod unicode_letter;
mod utf8;

use std::str;

pub use ascii::*;
pub use gb18030::*;
pub use gbk::*;
pub use unicode_letter::*;
pub use utf8::*;

use super::Encoding;
use crate::codec::{
    Error, Result,
    data_type::{Bytes, BytesRef},
};

fn format_invalid_char(data: BytesRef<'_>) -> String {
    // Max length of the invalid string is '\x00\x00\x00\x00\x00...'(25) we set 32
    // here.
    let mut buf = String::with_capacity(32);
    const MAX_BYTES_TO_SHOW: usize = 5;
    buf.push('\'');
    for (i, &byte) in data.iter().enumerate() {
        if i > MAX_BYTES_TO_SHOW {
            buf.push_str("...");
            break;
        }
        if byte.is_ascii() {
            buf.push(char::from(byte));
        } else {
            buf.push_str(format!("\\x{:X}", byte).as_str());
        }
    }
    buf.push('\'');
    buf
}
