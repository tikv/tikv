// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;

/// `CHARSET_BIN` is used for marking binary charset.
pub const CHARSET_BIN: &str = "binary";
/// `CHARSET_UTF8` is the default charset for string types.
pub const CHARSET_UTF8: &str = "utf8";
/// `CHARSET_UTF8MB4` represents 4 bytes utf8, which works the same way as utf8
/// in Rust.
pub const CHARSET_UTF8MB4: &str = "utf8mb4";
/// `CHARSET_ASCII` is a subset of UTF8.
pub const CHARSET_ASCII: &str = "ascii";
/// `CHARSET_LATIN1` is a single byte charset.
pub const CHARSET_LATIN1: &str = "latin1";
/// `CHARSET_GBK` is Chinese character set.
pub const CHARSET_GBK: &str = "gbk";
/// `CHARSET_GB18030` is another Chinese character set containing GBK.
pub const CHARSET_GB18030: &str = "gb18030";
// For a new implemented multi-byte charset, add it to MULTI_BYTES_CHARSETS

lazy_static! {
    pub static ref MULTI_BYTES_CHARSETS: collections::HashSet<&'static str> =
        [CHARSET_UTF8, CHARSET_UTF8MB4, CHARSET_GBK, CHARSET_GB18030,]
            .iter()
            .cloned()
            .collect();
}
