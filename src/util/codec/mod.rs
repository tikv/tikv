// Copyright 2016 PingCAP, Inc.
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

pub mod bytes;
pub mod number;

use std::io::{self, ErrorKind};

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

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyLength {description("bad format key(length)")}
        KeyPadding {description("bad format key(padding)")}
        KeyNotFound {description("key not found")}
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            Error::KeyLength => Some(Error::KeyLength),
            Error::KeyPadding => Some(Error::KeyPadding),
            Error::KeyNotFound => Some(Error::KeyNotFound),
            Error::Io(_) => None,
        }
    }
    pub fn unexpected_eof() -> Error {
        Error::Io(io::Error::new(ErrorKind::UnexpectedEof, "eof"))
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
