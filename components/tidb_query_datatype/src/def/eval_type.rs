// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

/// Function implementations' parameter data types.
///
/// It is similar to the `EvalType` in TiDB, but doesn't provide type `Timestamp`, which is
/// handled by the same type as `DateTime` here, instead of a new type. Also, `String` is
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

impl EvalType {
    /// Converts `EvalType` into one of the compatible `FieldTypeTp`s.
    ///
    /// This function should be only useful in test scenarios that only cares about `EvalType` but
    /// accepts a `FieldTypeTp`.
    pub fn into_certain_field_type_tp_for_test(self) -> crate::FieldTypeTp {
        match self {
            EvalType::Int => crate::FieldTypeTp::LongLong,
            EvalType::Real => crate::FieldTypeTp::Double,
            EvalType::Decimal => crate::FieldTypeTp::NewDecimal,
            EvalType::Bytes => crate::FieldTypeTp::String,
            EvalType::DateTime => crate::FieldTypeTp::DateTime,
            EvalType::Duration => crate::FieldTypeTp::Duration,
            EvalType::Json => crate::FieldTypeTp::JSON,
        }
    }
}

impl fmt::Display for EvalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::convert::TryFrom<crate::FieldTypeTp> for EvalType {
    type Error = crate::DataTypeError;

    // Succeeds for all field types supported as eval types, fails for unsupported types.
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
                // In TiDB, Bit's eval type is Int, but it is not yet supported in TiKV.
                return Err(crate::DataTypeError::UnsupportedType {
                    name: tp.to_string(),
                });
            }
        };
        Ok(eval_type)
    }
}
