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
    Enum,
    Set,
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
            EvalType::Enum => crate::FieldTypeTp::Enum,
            EvalType::Set => crate::FieldTypeTp::Set,
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
            | crate::FieldTypeTp::String
            | crate::FieldTypeTp::Null => EvalType::Bytes,
            _ => {
                // In TiDB, Bit's eval type is Int, but it is not yet supported in TiKV.
                // TODO: we need to handle FieldTypeTp::{Enum, Set} after we implement encode and decode.
                return Err(crate::DataTypeError::UnsupportedType {
                    name: tp.to_string(),
                });
            }
        };
        Ok(eval_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldTypeAccessor;
    use crate::FieldTypeTp::*;
    use std::convert::TryFrom;

    #[test]
    fn test_fieldtype_to_evaltype() {
        let cases = vec![
            (Unspecified, None),
            (Tiny, Some(EvalType::Int)),
            (Short, Some(EvalType::Int)),
            (Long, Some(EvalType::Int)),
            (Float, Some(EvalType::Real)),
            (Double, Some(EvalType::Real)),
            (Null, Some(EvalType::Bytes)),
            (Timestamp, Some(EvalType::DateTime)),
            (LongLong, Some(EvalType::Int)),
            (Int24, Some(EvalType::Int)),
            (Date, Some(EvalType::DateTime)),
            (Duration, Some(EvalType::Duration)),
            (DateTime, Some(EvalType::DateTime)),
            (Year, Some(EvalType::Int)),
            (NewDate, None),
            (VarChar, Some(EvalType::Bytes)),
            (Bit, None),
            (JSON, Some(EvalType::Json)),
            (NewDecimal, Some(EvalType::Decimal)),
            (Enum, None),
            (Set, None),
            (TinyBlob, Some(EvalType::Bytes)),
            (MediumBlob, Some(EvalType::Bytes)),
            (LongBlob, Some(EvalType::Bytes)),
            (Blob, Some(EvalType::Bytes)),
            (VarString, Some(EvalType::Bytes)),
            (String, Some(EvalType::Bytes)),
            (Geometry, None),
        ];

        for (tp, etype) in cases {
            let mut ft = tipb::FieldType::default();
            ft.set_tp(tp as i32);

            let ftt = EvalType::try_from(ft.as_accessor().tp());

            if let Some(etype) = etype {
                assert_eq!(ftt.unwrap(), etype);
            } else {
                assert!(ftt.is_err());
            }
        }
    }
}
