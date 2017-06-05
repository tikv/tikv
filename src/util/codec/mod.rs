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

use std::io;

use protobuf;

const TEN_POW: &'static [u32] =
    &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000];

/// A shortcut to box an error.
macro_rules! invalid_type {
    ($e:expr) => ({
        use util::codec::Error;
        Error::InvalidDataType(($e).into())
    });
    ($f:tt, $($arg:expr),+) => ({
        use util::codec::Error;
        Error::InvalidDataType(format!($f, $($arg),+))
    });
}

pub mod bytes;
pub mod number;
pub mod datum;
pub mod table;
pub mod convert;
pub mod mysql;

pub use self::datum::Datum;

use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::error;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("protobuf error {:?}", err)
        }
        KeyLength {description("bad format key(length)")}
        KeyPadding {description("bad format key(padding)")}
        InvalidDataType(reason: String) {
            description("invalid data type")
            display("{}", reason)
        }
        Encoding(err: Utf8Error) {
            from()
            cause(err)
            description("enconding failed")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        err.utf8_error().into()
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
