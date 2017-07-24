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


pub mod evaluator;
mod builtin_cast;
mod builtin_math;

use util::codec;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Codec(e: codec::Error) {
            from()
            description("codec failed")
        }
        Expr(s: String) {
            description("invalid expression")
            display("{}", s)
        }
        Eval(s: String) {
            description("evaluation failed")
            display("{}", s)
        }
    }
}

use std::result;
pub type Result<T> = result::Result<T, Error>;

pub use self::evaluator::{Evaluator, EvalContext};

pub const ERROR_UNIMPLEMENTED: &'static str = "unimplemented";

pub const TYPE_INT: &'static str = "int";

use coprocessor::codec::datum::Datum;

pub fn invalid_type_error(datum: &Datum, expected_type: &str) -> Result<Datum> {
    Err(Error::Eval(format!("invalid expr type: {:?}, expect: {}", datum, expected_type)))
}
