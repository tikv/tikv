// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

/// `CHARSET_BIN` is used for marking binary charset.
pub const CHARSET_BIN: &str = "binary";
/// `CHARSET_UTF8` is the default charset for string types.
pub const CHARSET_UTF8: &str = "utf8";
/// `CHARSET_UTF8MB4` represents 4 bytes utf8, which works the same way as utf8 in Rust.
pub const CHARSET_UTF8MB4: &str = "utf8mb4";
/// `CHARSET_ASCII` is a subset of UTF8.
pub const CHARSET_ASCII: &str = "ascii";
/// `CHARSET_LATIN1` is a single byte charset.
pub const CHARSET_LATIN1: &str = "latin1";

/// All utf8 charsets.
pub const UTF8_CHARSETS: &[&str] = &[CHARSET_UTF8, CHARSET_UTF8MB4, CHARSET_ASCII];
