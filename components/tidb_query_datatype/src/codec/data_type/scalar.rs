// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use match_template::match_template;
use tipb::FieldType;

use super::*;
use crate::{
    codec::collation::Collator, match_template_collator, match_template_evaltype, Collation,
    EvalType, FieldTypeAccessor,
};

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
    Enum(Option<super::Enum>),
    Set(Option<super::Set>),
}

impl ScalarValue {
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaltype! {
            TT, match self {
                ScalarValue::TT(_) => EvalType::TT,
            }
        }
    }

    #[inline]
    pub fn as_scalar_value_ref(&self) -> ScalarValueRef<'_> {
        match self {
            ScalarValue::Int(x) => ScalarValueRef::Int(x.as_ref()),
            ScalarValue::Duration(x) => ScalarValueRef::Duration(x.as_ref()),
            ScalarValue::DateTime(x) => ScalarValueRef::DateTime(x.as_ref()),
            ScalarValue::Real(x) => ScalarValueRef::Real(x.as_ref()),
            ScalarValue::Decimal(x) => ScalarValueRef::Decimal(x.as_ref()),
            ScalarValue::Bytes(x) => ScalarValueRef::Bytes(x.as_ref().map(|x| x.as_slice())),
            ScalarValue::Json(x) => ScalarValueRef::Json(x.as_ref().map(|x| x.as_ref())),
            ScalarValue::Enum(x) => ScalarValueRef::Enum(x.as_ref().map(|x| x.as_ref())),
            ScalarValue::Set(x) => ScalarValueRef::Set(x.as_ref().map(|x| x.as_ref())),
        }
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        match_template_evaltype! {
            TT, match self {
                ScalarValue::TT(v) => v.is_none(),
            }
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        match_template_evaltype! {
            TT, match self {
                ScalarValue::TT(v) => v.is_some(),
            }
        }
    }
}

impl AsMySQLBool for ScalarValue {
    #[inline]
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match_template_evaltype! {
            TT, match self {
                ScalarValue::TT(v) => v.as_ref().as_mysql_bool(context),
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

impl<'a> From<Option<JsonRef<'a>>> for ScalarValue {
    #[inline]
    fn from(s: Option<JsonRef<'a>>) -> ScalarValue {
        ScalarValue::Json(s.map(|x| x.to_owned()))
    }
}

impl<'a> From<Option<BytesRef<'a>>> for ScalarValue {
    #[inline]
    fn from(s: Option<BytesRef<'a>>) -> ScalarValue {
        ScalarValue::Bytes(s.map(|x| x.to_vec()))
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
    Int(Option<&'a super::Int>),
    Real(Option<&'a super::Real>),
    Decimal(Option<&'a super::Decimal>),
    Bytes(Option<BytesRef<'a>>),
    DateTime(Option<&'a super::DateTime>),
    Duration(Option<&'a super::Duration>),
    Json(Option<JsonRef<'a>>),
    Enum(Option<EnumRef<'a>>),
    Set(Option<SetRef<'a>>),
}

impl<'a> ScalarValueRef<'a> {
    #[inline]
    #[allow(clippy::clone_on_copy)]
    pub fn to_owned(self) -> ScalarValue {
        match self {
            ScalarValueRef::Int(x) => ScalarValue::Int(x.cloned()),
            ScalarValueRef::Duration(x) => ScalarValue::Duration(x.cloned()),
            ScalarValueRef::DateTime(x) => ScalarValue::DateTime(x.cloned()),
            ScalarValueRef::Real(x) => ScalarValue::Real(x.cloned()),
            ScalarValueRef::Decimal(x) => ScalarValue::Decimal(x.cloned()),
            ScalarValueRef::Bytes(x) => ScalarValue::Bytes(x.map(|x| x.to_vec())),
            ScalarValueRef::Json(x) => ScalarValue::Json(x.map(|x| x.to_owned())),
            ScalarValueRef::Enum(x) => ScalarValue::Enum(x.map(|x| x.to_owned())),
            ScalarValueRef::Set(x) => ScalarValue::Set(x.map(|x| x.to_owned())),
        }
    }

    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaltype! {
            TT, match self {
                ScalarValueRef::TT(_) => EvalType::TT,
            }
        }
    }

    /// Encodes into binary format.
    pub fn encode(
        &self,
        field_type: &FieldType,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::codec::datum_codec::EvaluableDatumEncoder;

        match self {
            ScalarValueRef::Int(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        // Always encode to INT / UINT instead of VAR INT to be efficient.
                        let is_unsigned = field_type.is_unsigned();
                        output.write_evaluable_datum_int(**val, is_unsigned)?;
                    }
                }
                Ok(())
            }
            ScalarValueRef::Real(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_real(val.into_inner())?;
                    }
                }
                Ok(())
            }
            ScalarValueRef::Decimal(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_decimal(val)?;
                    }
                }
                Ok(())
            }
            ScalarValueRef::Bytes(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_bytes(val)?;
                    }
                }
                Ok(())
            }
            ScalarValueRef::DateTime(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_date_time(**val, ctx)?;
                    }
                }
                Ok(())
            }
            ScalarValueRef::Duration(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_duration(**val)?;
                    }
                }
                Ok(())
            }
            ScalarValueRef::Json(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_json(*val)?;
                    }
                }
                Ok(())
            }
            // TODO: we should implement enum/set encode
            ScalarValueRef::Enum(_) => unimplemented!(),
            ScalarValueRef::Set(_) => unimplemented!(),
        }
    }

    pub fn encode_sort_key(
        &self,
        field_type: &FieldType,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::codec::datum_codec::EvaluableDatumEncoder;

        match self {
            ScalarValueRef::Bytes(val) => {
                match val {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        let sort_key = match_template_collator! {
                            TT, match field_type.collation().map_err(crate::codec::Error::from)? {
                                Collation::TT => TT::sort_key(val)?
                            }
                        };
                        output.write_evaluable_datum_bytes(&sort_key)?;
                    }
                }
                Ok(())
            }
            _ => self.encode(field_type, ctx, output),
        }
    }

    #[inline]
    pub fn cmp_sort_key(
        &self,
        other: &ScalarValueRef<'_>,
        field_type: &FieldType,
    ) -> crate::codec::Result<Ordering> {
        Ok(match_template! {
            TT = [Real, Decimal, DateTime, Duration, Json, Enum],
            match (self, other) {
                (ScalarValueRef::TT(v1), ScalarValueRef::TT(v2)) => v1.cmp(v2),
                (ScalarValueRef::Int(v1), ScalarValueRef::Int(v2)) => compare_int(&v1.cloned(), &v2.cloned(), field_type),
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
            pub fn $name(&self) -> Option<&$ty> {
                Evaluable::borrow_scalar_value(self)
            }
        }

        impl<'a> ScalarValueRef<'a> {
            #[inline]
            pub fn $name(&'a self) -> Option<&'a $ty> {
                Evaluable::borrow_scalar_value_ref(*self)
            }
        }
    };
}

impl_as_ref! { Int, as_int }
impl_as_ref! { Real, as_real }
impl_as_ref! { Decimal, as_decimal }
impl_as_ref! { DateTime, as_date_time }
impl_as_ref! { Duration, as_duration }

impl ScalarValue {
    #[inline]
    pub fn as_json(&self) -> Option<JsonRef<'_>> {
        EvaluableRef::borrow_scalar_value(self)
    }
}

impl<'a> ScalarValueRef<'a> {
    #[inline]
    pub fn as_json(&'a self) -> Option<JsonRef<'a>> {
        EvaluableRef::borrow_scalar_value_ref(*self)
    }
}

impl ScalarValue {
    #[inline]
    pub fn as_bytes(&self) -> Option<BytesRef<'_>> {
        EvaluableRef::borrow_scalar_value(self)
    }
}

impl<'a> ScalarValueRef<'a> {
    #[inline]
    pub fn as_bytes(&'a self) -> Option<BytesRef<'a>> {
        EvaluableRef::borrow_scalar_value_ref(*self)
    }
}

impl<'a> Ord for ScalarValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect("Cannot compare two ScalarValueRef in different type")
    }
}

impl<'a> PartialOrd for ScalarValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match_template_evaltype! {
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
        self == &other.as_scalar_value_ref()
    }
}

impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValue {
    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
        other == self
    }
}
