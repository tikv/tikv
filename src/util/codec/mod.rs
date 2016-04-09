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

pub mod bytes;
pub mod rpc;
pub mod number;
pub mod datum;
pub mod table;
pub mod convert;

pub use self::datum::Datum;

use std::str::Utf8Error;
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
        OutOfBound(want: usize, actual: usize) {
            description("out of bound.")
            display("want {} actual {}", want, actual)
        }
        InvalidDataType(reason: String) {
            description("invalid data type")
            display("{}", reason)
        }
        Eof {
            description("eof")
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

pub type Result<T> = ::std::result::Result<T, Error>;

pub fn check_bound<T>(buf: &[T], want: usize) -> Result<()> {
    if want > buf.len() {
        return Err(Error::OutOfBound(want, buf.len()));
    }
    Ok(())
}
