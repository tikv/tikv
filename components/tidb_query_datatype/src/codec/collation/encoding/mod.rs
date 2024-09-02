// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod ascii;
mod gb18030;
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
    data_type::{Bytes, BytesRef},
    Error, Result,
};

fn format_invalid_char(data: BytesRef<'_>) -> String {
    // Max length of the invalid string is '\x00\x00\x00\x00\x00...'(25) we set 32
    // here.
    let mut buf = String::with_capacity(32);
    const MAX_BYTES_TO_SHOW: usize = 5;
    buf.push('\'');
    for i in 0..data.len() {
        if i > MAX_BYTES_TO_SHOW {
            buf.push_str("...");
            break;
        }
        if data[i].is_ascii() {
            buf.push(char::from(data[i]));
        } else {
            buf.push_str(format!("\\x{:X}", data[i]).as_str());
        }
    }
    buf.push('\'');
    buf
}
