// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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

pub enum ScalarValueRef<'a> {
    Int(&'a Option<super::Int>),
    Real(&'a Option<super::Real>),
    Decimal(&'a Option<super::Decimal>),
    Bytes(&'a Option<super::Bytes>),
    DateTime(&'a Option<super::DateTime>),
    Duration(&'a Option<super::Duration>),
    Json(&'a Option<super::Json>),
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
    pub fn as_vector_like(&self) -> VectorLikeValueRef<'_> {
        VectorLikeValueRef::Scalar(self)
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

impl<'a> From<ScalarValueRef<'a>> for ScalarValue {
    #[inline]
    #[allow(clippy::clone_on_copy)]
    fn from(s: ScalarValueRef<'a>) -> ScalarValue {
        match_template_evaluable! {
            TT, match s {
                ScalarValueRef::TT(v) => ScalarValue::TT(v.clone()),
            }
        }
    }
}
