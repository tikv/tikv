// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use crate::codec::collation::{match_template_collator, Collator};
use match_template::match_template;
use tidb_query_datatype::{Collation, EvalType, FieldTypeAccessor};
use tipb::FieldType;

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
#[derive(Clone, Debug, PartialEq)]
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
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(_) => EvalType::TT,
            }
        }
    }

    #[inline]
    pub fn as_scalar_value_ref(&self) -> ScalarValueRef<'_> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => ScalarValueRef::TT(v),
            }
        }
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.is_none(),
            }
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.is_some(),
            }
        }
    }
}

impl AsMySQLBool for ScalarValue {
    #[inline]
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.as_mysql_bool(context),
            }
        }
    }
}

macro_rules! impl_from {
    ($ty:tt) => {
        impl From<Option<$ty>> for ScalarValue {
            #[inline]
            fn from(s: Option<$ty>) -> ScalarValue {
                ScalarValue::$ty(s)
            }
        }

        impl From<$ty> for ScalarValue {
            #[inline]
            fn from(s: $ty) -> ScalarValue {
                ScalarValue::$ty(Some(s))
            }
        }

        impl From<ScalarValue> for Option<$ty> {
            #[inline]
            fn from(s: ScalarValue) -> Option<$ty> {
                match s {
                    ScalarValue::$ty(v) => v,
                    _ => panic!(
                        "Cannot cast {} scalar value into {}",
                        s.eval_type(),
                        stringify!($ty),
                    ),
                }
            }
        }
    };
}

impl_from! { Int }
impl_from! { Real }
impl_from! { Decimal }
impl_from! { Bytes }
impl_from! { DateTime }
impl_from! { Duration }
impl_from! { Json }

impl From<Option<f64>> for ScalarValue {
    #[inline]
    fn from(s: Option<f64>) -> ScalarValue {
        ScalarValue::Real(s.and_then(|f| Real::new(f).ok()))
    }
}

impl From<f64> for ScalarValue {
    #[inline]
    fn from(s: f64) -> ScalarValue {
        ScalarValue::Real(Real::new(s).ok())
    }
}

impl From<ScalarValue> for Option<f64> {
    #[inline]
    fn from(s: ScalarValue) -> Option<f64> {
        match s {
            ScalarValue::Real(v) => v.map(|v| v.into_inner()),
            _ => panic!("Cannot cast {} scalar value into f64", s.eval_type()),
        }
    }
}

/// A scalar value reference container. Can be created from `ScalarValue` or `VectorValue`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarValueRef<'a> {
    Int(&'a Option<super::Int>),
    Real(&'a Option<super::Real>),
    Decimal(&'a Option<super::Decimal>),
    Bytes(&'a Option<super::Bytes>),
    DateTime(&'a Option<super::DateTime>),
    Duration(&'a Option<super::Duration>),
    Json(&'a Option<super::Json>),
}

impl<'a> ScalarValueRef<'a> {
    #[inline]
    #[allow(clippy::clone_on_copy)]
    pub fn to_owned(self) -> ScalarValue {
        match_template_evaluable! {
            TT, match self {
                ScalarValueRef::TT(v) => ScalarValue::TT(v.clone()),
            }
        }
    }

    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                ScalarValueRef::TT(_) => EvalType::TT,
            }
        }
    }

    #[inline]
    pub fn cmp_sort_key(
        &self,
        other: &ScalarValueRef,
        field_type: &FieldType,
    ) -> crate::codec::Result<Ordering> {
        Ok(match_template! {
            TT = [Real, Decimal, DateTime, Duration, Json],
            match (self, other) {
                (ScalarValueRef::TT(v1), ScalarValueRef::TT(v2)) => v1.cmp(v2),
                (ScalarValueRef::Int(v1), ScalarValueRef::Int(v2)) => compare_int(v1, v2, &field_type),
                (ScalarValueRef::Bytes(None), ScalarValueRef::Bytes(None)) => Ordering::Equal,
                (ScalarValueRef::Bytes(Some(_)), ScalarValueRef::Bytes(None)) => Ordering::Greater,
                (ScalarValueRef::Bytes(None), ScalarValueRef::Bytes(Some(_))) => Ordering::Less,
                (ScalarValueRef::Bytes(Some(v1)), ScalarValueRef::Bytes(Some(v2))) => {
                    match_template_collator! {
                        TT, match field_type.collation()? {
                            Collation::TT => TT::sort_compare(v1, v2)?
                        }
                    }
                }
                _ => panic!("Cannot compare two ScalarValueRef in different type"),
            }
        })
    }
}

#[inline]
fn compare_int(
    lhs: &Option<super::Int>,
    rhs: &Option<super::Int>,
    field_type: &FieldType,
) -> Ordering {
    if field_type.is_unsigned() {
        lhs.map(|i| i as u64).cmp(&rhs.map(|i| i as u64))
    } else {
        lhs.cmp(rhs)
    }
}

macro_rules! impl_as_ref {
    ($ty:tt, $name:ident) => {
        impl ScalarValue {
            #[inline]
            pub fn $name(&self) -> &Option<$ty> {
                match self {
                    ScalarValue::$ty(v) => v,
                    other => panic!(
                        "Cannot cast {} scalar value into {}",
                        other.eval_type(),
                        stringify!($ty),
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

        impl<'a> ScalarValueRef<'a> {
            #[inline]
            pub fn $name(&'a self) -> &'a Option<$ty> {
                match self {
                    ScalarValueRef::$ty(v) => v,
                    other => panic!(
                        "Cannot cast {} scalar value into {}",
                        other.eval_type(),
                        stringify!($ty),
                    ),
                }
            }
        }

        impl AsRef<Option<$ty>> for ScalarValueRef<'_> {
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

impl<'a> Ord for ScalarValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect("Cannot compare two ScalarValueRef in different type")
    }
}

impl<'a> PartialOrd for ScalarValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match_template_evaluable! {
            TT, match (self, other) {
                // v1 and v2 are `Option<T>`. However, in MySQL NULL values are considered lower
                // than any non-NULL value, so using `Option::PartialOrd` directly is fine.
                (ScalarValueRef::TT(v1), ScalarValueRef::TT(v2)) => Some(v1.cmp(v2)),
                _ => None,
            }
        }
    }
}

impl<'a> PartialEq<ScalarValue> for ScalarValueRef<'a> {
    fn eq(&self, other: &ScalarValue) -> bool {
        match_template_evaluable! {
            TT, match (self, other) {
                (ScalarValueRef::TT(v1), ScalarValue::TT(v2)) => v1 == &v2,
                _ => false
            }
        }
    }
}

impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValue {
    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
        other == self
    }
}
