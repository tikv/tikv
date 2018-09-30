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

/// Function implementations' parameter data types.
///
/// It is similar to the `EvalType` in TiDB, but differs in the follows for historical reasons:
/// - Doesn't provide type `Timestamp`. It is handled by the same type as `DateTime`
///   instead of a new type.
pub enum EvalType {
    Int,
    Real,
    Decimal,
    String,
    DateTime,
    Duration,
    Json,
}

pub trait EvalTypeProvider {
    fn get_eval_type(&self) -> EvalType;
}

impl<T: ::FieldTypeProvider> EvalTypeProvider for T {
    #[inline]
    fn get_eval_type(&self) -> EvalType {
        let tp = self.get_tp();
        match tp {
            ::FieldTypeTp::Tiny
            | ::FieldTypeTp::Short
            | ::FieldTypeTp::Int24
            | ::FieldTypeTp::Long
            | ::FieldTypeTp::LongLong
            | ::FieldTypeTp::Bit
            | ::FieldTypeTp::Year => EvalType::Int,
            ::FieldTypeTp::Float | ::FieldTypeTp::Double => EvalType::Real,
            ::FieldTypeTp::NewDecimal => EvalType::Decimal,
            ::FieldTypeTp::Timestamp | ::FieldTypeTp::Date | ::FieldTypeTp::DateTime => {
                EvalType::DateTime
            }
            ::FieldTypeTp::Duration => EvalType::Duration,
            ::FieldTypeTp::JSON => EvalType::Json,
            _ => EvalType::String,
        }
    }
}
