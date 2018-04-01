// Copyright 2017 PingCAP, Inc.
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

use std::{error, io, str};
use std::string::FromUtf8Error;
use std::str::Utf8Error;
use tipb::expression::ScalarFuncSig;
use tipb::select;

use coprocessor::codec::mysql::Res;
use util;
use util::codec::Error as CError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        Type { has: &'static str, expected: &'static str } {
            description("type error")
            display("type error: cannot get {:?} result from {:?} expression", expected, has)
        }
        Codec(err: util::codec::Error) {
            from()
            description("codec error")
            display("codec error: {}", err)
            cause(err)
        }
        ColumnOffset(offset: usize) {
            description("column offset not found")
            display("illegal column offset: {}", offset)
        }
        UnknownSignature(sig: ScalarFuncSig) {
            description("Unknown signature")
            display("Unknown signature: {:?}", sig)
        }
        Truncated(s:String) {
            description("Truncated")
            display("{}",s)
        }
        Overflow(msg:String) {
            description("Overflow")
            display("{}",msg)
        }
        TruncatedWrongVal(msg:String) {
            description("TruncatedWrongVal")
            display("{}",msg)
        }
        Eval(s: String) {
            description("evaluation failed")
            display("{}", s)
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}
pub fn gen_overflow_err(data: &str, range: String) -> Error {
    let msg = format!("{} value is out of range in {:?}", data, range);
    Error::Overflow(msg)
}

pub fn gen_truncated_wrong_val(data_type: &str, val: String) -> Error {
    let msg = format!("Truncated incorrect {} value: '{}'", data_type, val);
    Error::TruncatedWrongVal(msg)
}

pub fn gen_cast_neg_int_as_unsigned() -> Error {
    box_err!("Cast to unsigned converted negative integer to it's positive complement")
}

pub fn gen_cast_as_signed_overflow() -> Error {
    box_err!("Cast to signed converted positive out-of-range integer to it's negative complement")
}

impl Into<select::Error> for Error {
    fn into(self) -> select::Error {
        let mut err = select::Error::new();
        err.set_msg(format!("{:?}", self));
        err
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Codec(CError::Encoding(err.utf8_error()))
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Error {
        Error::Codec(CError::Encoding(err))
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl<T> Into<Result<T>> for Res<T> {
    fn into(self) -> Result<T> {
        match self {
            Res::Ok(t) => Ok(t),
            Res::Truncated(_) => Err(Error::Truncated("Data Truncated".into())),
            Res::Overflow(_) => Err(gen_overflow_err("", "".into())),
        }
    }
}
