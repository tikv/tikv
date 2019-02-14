// Copyright 2019 PingCAP, Inc.
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

use cop_datatype::EvalType;

use super::*;

/// A scalar value container, a.k.a. datum, for all concrete eval types.
///
/// In many cases, for example, at the framework level, the concrete eval type is unknown at compile
/// time. So we use this enum container to represent types dynamically. It is similar to trait
/// object `Box<T>` where `T` is a concrete eval type but faster.
///
/// Like `VectorValue`, the inner concrete value is immutable.
///
/// Compared to `VectorValue`, it only contains a single concrete value.
/// Compared to `Datum`, it is a newer encapsulation that naturally wraps `Option<..>`.
///
/// TODO: Once we removed the `Option<..>` wrapper, it will be much like `Datum`. At that time,
/// we only need to preserve one of them.
#[derive(Debug, PartialEq)]
pub enum ScalarValue {
    Int(Option<super::Int>),
    Real(Option<super::Real>),
    Decimal(Option<super::Decimal>),
    Bytes(Option<super::Bytes>),
    DateTime(Option<super::DateTime>),
    Duration(Option<super::Duration>),
    Json(Option<super::Json>),
}

impl ScalarValue {
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match self {
            ScalarValue::Int(_) => EvalType::Int,
            ScalarValue::Real(_) => EvalType::Real,
            ScalarValue::Decimal(_) => EvalType::Decimal,
            ScalarValue::Bytes(_) => EvalType::Bytes,
            ScalarValue::DateTime(_) => EvalType::DateTime,
            ScalarValue::Duration(_) => EvalType::Duration,
            ScalarValue::Json(_) => EvalType::Json,
        }
    }
}

impl AsMySQLBool for ScalarValue {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        match self {
            ScalarValue::Int(ref v) => v.as_mysql_bool(),
            ScalarValue::Real(ref v) => v.as_mysql_bool(),
            ScalarValue::Decimal(ref v) => v.as_mysql_bool(),
            ScalarValue::Bytes(ref v) => v.as_mysql_bool(),
            ScalarValue::DateTime(ref v) => v.as_mysql_bool(),
            ScalarValue::Duration(ref v) => v.as_mysql_bool(),
            ScalarValue::Json(ref v) => v.as_mysql_bool(),
        }
    }
}

macro_rules! impl_as_ref {
    ($ty:tt, $name:ident) => {
        impl ScalarValue {
            #[inline]
            pub fn $name(&self) -> &Option<$ty> {
                match self {
                    ScalarValue::$ty(ref v) => v,
                    other => panic!(
                        "Cannot cast {} scalar value into {}",
                        other.eval_type(),
                        stringify!($tt),
                    ),
                }
            }
        }

        impl AsRef<Option<$ty>> for ScalarValue {
            #[inline]
            fn as_ref(&self) -> &Option<$ty> {
                self.$name()
            }
        }

        // `AsMut` is not implemented intentionally.
    };
}

impl_as_ref! { Int, as_int }
impl_as_ref! { Real, as_real }
impl_as_ref! { Decimal, as_decimal }
impl_as_ref! { Bytes, as_bytes }
impl_as_ref! { DateTime, as_date_time }
impl_as_ref! { Duration, as_duration }
impl_as_ref! { Json, as_json }
