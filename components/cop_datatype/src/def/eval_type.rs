// Copyright 2018 PingCAP, Inc.
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

use std::fmt;

/// Function implementations' parameter data types.
///
/// It is similar to the `EvalType` in TiDB, but doesn't provide type `Timestamp`, which is
/// handled by the same type as `DateTime` here instead of a new type. Also, `String` is
/// called `Bytes` here to be less confusing.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum EvalType {
    Int,
    Real,
    Decimal,
    Bytes,
    DateTime,
    Duration,
    Json,
}

impl fmt::Display for EvalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl ::std::convert::TryFrom<::FieldTypeTp> for EvalType {
    type Error = ::DataTypeError;

    fn try_from(tp: ::FieldTypeTp) -> Result<Self, ::DataTypeError> {
        let eval_type = match tp {
            ::FieldTypeTp::Tiny
            | ::FieldTypeTp::Short
            | ::FieldTypeTp::Int24
            | ::FieldTypeTp::Long
            | ::FieldTypeTp::LongLong
            | ::FieldTypeTp::Year => EvalType::Int,
            ::FieldTypeTp::Float | ::FieldTypeTp::Double => EvalType::Real,
            ::FieldTypeTp::NewDecimal => EvalType::Decimal,
            ::FieldTypeTp::Timestamp | ::FieldTypeTp::Date | ::FieldTypeTp::DateTime => {
                EvalType::DateTime
            }
            ::FieldTypeTp::Duration => EvalType::Duration,
            ::FieldTypeTp::JSON => EvalType::Json,
            ::FieldTypeTp::VarChar
            | ::FieldTypeTp::TinyBlob
            | ::FieldTypeTp::MediumBlob
            | ::FieldTypeTp::LongBlob
            | ::FieldTypeTp::Blob
            | ::FieldTypeTp::VarString
            | ::FieldTypeTp::String => EvalType::Bytes,
            _ => {
                // Note: In TiDB, Bit's eval type is Int, but it is not yet supported in TiKV.
                return Err(::DataTypeError::UnsupportedType {
                    name: tp.to_string(),
                });
            }
        };
        Ok(eval_type)
    }
}
