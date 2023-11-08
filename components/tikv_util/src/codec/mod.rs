// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

pub mod bytes;
pub mod number;
pub mod stream_event;

use std::io::{self, ErrorKind};

use error_code::{self, ErrorCode, ErrorCodeExt};
use thiserror::Error;

pub type BytesSlice<'a> = &'a [u8];

#[inline]
pub fn read_slice<'a>(data: &mut BytesSlice<'a>, size: usize) -> Result<BytesSlice<'a>> {
    if data.len() >= size {
        let buf = &data[0..size];
        *data = &data[size..];
        Ok(buf)
    } else {
        Err(Error::unexpected_eof())
    }
}

/// return the key that keeps the range [self, self.next_prefix()) contains
/// all keys with the prefix `self`.
///
/// # Examples
/// ```rust
/// # use tikv_util::codec::next_prefix_of;
/// // ["hello", "hellp") contains "hello*"
/// assert_eq!(&next_prefix_of(b"hello".to_vec()), b"hellp");
/// // [[255], []) contains keys matches [255, *]
/// assert_eq!(&next_prefix_of(vec![0xffu8]), &[]);
/// // [[42, 255], [43]) contains keys matches [42, 255, *]
/// assert_eq!(&next_prefix_of(vec![42u8, 0xffu8]), &[43u8]);
/// ```
pub fn next_prefix_of(key: Vec<u8>) -> Vec<u8> {
    let mut next_prefix = key;
    for i in (0..next_prefix.len()).rev() {
        if next_prefix[i] == u8::MAX {
            next_prefix.pop();
        } else {
            next_prefix[i] += 1;
            break;
        }
    }
    // By definition, the empty key means infinity.
    // When we have meet keys like [0xff], return empty slice here is expected.
    next_prefix
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("bad format key(length)")]
    KeyLength,
    #[error("bad format key(padding)")]
    KeyPadding,
    #[error("key not found")]
    KeyNotFound,
    #[error("bad format value(length)")]
    ValueLength,
    #[error("bad format value(meta)")]
    ValueMeta,
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            Error::KeyLength => Some(Error::KeyLength),
            Error::KeyPadding => Some(Error::KeyPadding),
            Error::KeyNotFound => Some(Error::KeyNotFound),
            Error::ValueLength => Some(Error::ValueLength),
            Error::ValueMeta => Some(Error::ValueMeta),
            Error::Io(_) => None,
        }
    }
    pub fn unexpected_eof() -> Error {
        Error::Io(io::Error::new(ErrorKind::UnexpectedEof, "eof"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => error_code::codec::IO,
            Error::KeyLength => error_code::codec::KEY_LENGTH,
            Error::KeyPadding => error_code::codec::BAD_PADDING,
            Error::KeyNotFound => error_code::codec::KEY_NOT_FOUND,
            Error::ValueLength => error_code::codec::VALUE_LENGTH,
            Error::ValueMeta => error_code::codec::VALUE_META,
        }
    }
}
