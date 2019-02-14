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

impl std::convert::TryFrom<crate::FieldTypeTp> for EvalType {
    type Error = crate::DataTypeError;

    fn try_from(tp: crate::FieldTypeTp) -> Result<Self, crate::DataTypeError> {
        let eval_type = match tp {
            crate::FieldTypeTp::Tiny
            | crate::FieldTypeTp::Short
            | crate::FieldTypeTp::Int24
            | crate::FieldTypeTp::Long
            | crate::FieldTypeTp::LongLong
            | crate::FieldTypeTp::Year => EvalType::Int,
            crate::FieldTypeTp::Float | crate::FieldTypeTp::Double => EvalType::Real,
            crate::FieldTypeTp::NewDecimal => EvalType::Decimal,
            crate::FieldTypeTp::Timestamp
            | crate::FieldTypeTp::Date
            | crate::FieldTypeTp::DateTime => EvalType::DateTime,
            crate::FieldTypeTp::Duration => EvalType::Duration,
            crate::FieldTypeTp::JSON => EvalType::Json,
            crate::FieldTypeTp::VarChar
            | crate::FieldTypeTp::TinyBlob
            | crate::FieldTypeTp::MediumBlob
            | crate::FieldTypeTp::LongBlob
            | crate::FieldTypeTp::Blob
            | crate::FieldTypeTp::VarString
            | crate::FieldTypeTp::String => EvalType::Bytes,
            _ => {
                // Note: In TiDB, Bit's eval type is Int, but it is not yet supported in TiKV.
                return Err(crate::DataTypeError::UnsupportedType {
                    name: tp.to_string(),
                });
            }
        };
        Ok(eval_type)
    }
}
