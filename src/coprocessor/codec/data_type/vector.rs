// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};

use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
use tipb::expression::FieldType;

use super::*;
use crate::coprocessor::codec::data_type::scalar::ScalarValueRef;
use crate::coprocessor::codec::datum;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::codec::{Error, Result};
use tikv_util::codec::{bytes, number};

/// A vector value container, a.k.a. column, for all concrete eval types.
///
/// The inner concrete value is immutable. However it is allowed to push and remove values from
/// this vector container.
#[derive(Debug, PartialEq)]
pub enum VectorValue {
    Int(Vec<Option<Int>>),
    Real(Vec<Option<Real>>),
    Decimal(Vec<Option<Decimal>>),
    // TODO: We need to improve its performance, i.e. store strings in adjacent memory places
    Bytes(Vec<Option<Bytes>>),
    DateTime(Vec<Option<DateTime>>),
    Duration(Vec<Option<Duration>>),
    Json(Vec<Option<Json>>),
}

impl Clone for VectorValue {
    #[inline]
    fn clone(&self) -> Self {
        // Implement `Clone` manually so that capacity can be preserved after clone.
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(vec) => VectorValue::TT(tikv_util::vec_clone_with_capacity(vec)),
            }
        }
    }
}

impl VectorValue {
    /// Creates an empty `VectorValue` according to `eval_tp` and reserves capacity according
    /// to `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize, eval_tp: EvalType) -> Self {
        match_template_evaluable! {
            TT, match eval_tp {
                EvalType::TT => VectorValue::TT(Vec::with_capacity(capacity)),
            }
        }
    }

    /// Returns the `EvalType` used to construct current column.
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(_) => EvalType::TT,
            }
        }
    }

    /// Returns the number of datums contained in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.len(),
            }
        }
    }

    /// Returns whether this column is empty.
    ///
    /// Equals to `len() == 0`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Shortens the column, keeping the first `len` datums and dropping the rest.
    ///
    /// If `len` is greater than the column's current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.truncate(len),
            }
        }
    }

    /// Clears the column, removing all datums.
    #[inline]
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Returns the number of elements this column can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.capacity(),
            }
        }
    }

    /// Retains only the elements specified by the predicate, which accepts index only.
    ///
    /// In other words, remove all rows such that `f(element_index)` returns `false`.
    #[inline]
    pub fn retain_by_index<F>(&mut self, mut f: F)
    where
        F: FnMut(usize) -> bool,
    {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => {
                    let mut idx = 0;
                    v.retain(|_| {
                        let r = f(idx);
                        idx += 1;
                        r
                    });
                },
            }
        }
    }

    /// Moves all the elements of `other` into `Self`, leaving `other` empty.
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have the same `EvalType` as `Self`.
    #[inline]
    pub fn append(&mut self, other: &mut VectorValue) {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(self_vec) => match other {
                    VectorValue::TT(other_vec) => {
                        self_vec.append(other_vec);
                    }
                    other => panic!("Cannot append {} to {} vector", other.eval_type(), self.eval_type())
                },
            }
        }
    }

    #[inline]
    pub fn as_vector_like(&self) -> VectorLikeValueRef<'_> {
        VectorLikeValueRef::Vector(self)
    }

    /// Evaluates values into MySQL logic values.
    ///
    /// The caller must provide an output buffer which is large enough for holding values.
    pub fn eval_as_mysql_bools(
        &self,
        context: &mut EvalContext,
        outputs: &mut [bool],
    ) -> crate::coprocessor::Result<()> {
        assert!(outputs.len() >= self.len());
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => {
                    let l = self.len();
                    for i in 0..l {
                        outputs[i] = v[i].as_mysql_bool(context)?;
                    }
                },
            }
        }
        Ok(())
    }

    /// Returns a `ScalarValueRef` to the element at the index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of range.
    #[inline]
    pub fn get_scalar_ref(&self, index: usize) -> ScalarValueRef<'_> {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => ScalarValueRef::TT(&v[index]),
            }
        }
    }

    /// Pushes a value into the column by decoding the datum and converting to current
    /// column's type.
    ///
    /// For values that needs a time zone, `time_zone` will be used.
    ///
    /// For values that current type's type is not sufficient, `field_type` will be used.
    ///
    /// # Panics
    ///
    /// Panics if `field_type` doesn't match current column's type.
    #[inline]
    #[allow(clippy::cast_lossless)]
    pub fn push_datum(
        &mut self,
        mut raw_datum: &[u8],
        time_zone: &Tz,
        field_type: &FieldType,
    ) -> Result<()> {
        #[inline]
        fn decode_int(v: &mut &[u8]) -> Result<i64> {
            number::decode_i64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as i64".to_owned()))
        }

        #[inline]
        fn decode_uint(v: &mut &[u8]) -> Result<u64> {
            number::decode_u64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as u64".to_owned()))
        }

        #[inline]
        fn decode_var_int(v: &mut &[u8]) -> Result<i64> {
            number::decode_var_i64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as var_i64".to_owned()))
        }

        #[inline]
        fn decode_var_uint(v: &mut &[u8]) -> Result<u64> {
            number::decode_var_u64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as var_u64".to_owned()))
        }

        #[inline]
        fn decode_float(v: &mut &[u8]) -> Result<f64> {
            number::decode_f64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as f64".to_owned()))
        }

        #[inline]
        fn decode_decimal(v: &mut &[u8]) -> Result<Decimal> {
            Decimal::decode(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as decimal".to_owned()))
        }

        #[inline]
        fn decode_bytes(v: &mut &[u8]) -> Result<Vec<u8>> {
            bytes::decode_bytes(v, false)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as bytes".to_owned()))
        }

        #[inline]
        fn decode_compact_bytes(v: &mut &[u8]) -> Result<Vec<u8>> {
            bytes::decode_compact_bytes(v).map_err(|_| {
                Error::InvalidDataType("Failed to decode data as compact bytes".to_owned())
            })
        }

        #[inline]
        fn decode_json(v: &mut &[u8]) -> Result<Json> {
            Json::decode(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as json".to_owned()))
        }

        #[inline]
        fn decode_duration_from_i64(v: i64) -> Result<Duration> {
            Duration::from_nanos(v, 0)
                .map_err(|_| Error::InvalidDataType("Failed to decode i64 as duration".to_owned()))
        }

        #[inline]
        fn decode_date_time_from_uint(
            v: u64,
            time_zone: &Tz,
            field_type: &FieldType,
        ) -> Result<DateTime> {
            let fsp = field_type.decimal() as i8;
            let time_type = field_type.tp().try_into()?;
            DateTime::from_packed_u64(v, time_type, fsp, time_zone)
        }

        // The inner implementation is much like `table::decode_col_value`, however it constructs
        // value directly without constructing a `Datum` to improve performance.

        // TODO: Use BufferReader.
        // TODO: Confirm correctness with TiDB team.

        // Make sure that the `field_type` given matches current column's type.
        let tp = field_type.tp();
        assert_eq!(EvalType::try_from(tp).unwrap(), self.eval_type());

        if raw_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }

        let flag = raw_datum[0];
        raw_datum = &raw_datum[1..];

        match self {
            VectorValue::Int(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::INT_FLAG => vec.push(Some(decode_int(&mut raw_datum)?)),
                datum::UINT_FLAG => vec.push(Some(decode_uint(&mut raw_datum)? as i64)),
                datum::VAR_INT_FLAG => vec.push(Some(decode_var_int(&mut raw_datum)?)),
                datum::VAR_UINT_FLAG => vec.push(Some(decode_var_uint(&mut raw_datum)? as i64)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Int vector",
                        flag
                    )));
                }
            },
            VectorValue::Real(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In both index and record, it's flag is `FLOAT`. See TiDB's `encode()`.
                datum::FLOAT_FLAG => {
                    let mut v = decode_float(&mut raw_datum)?;
                    if tp == FieldTypeTp::Float {
                        v = (v as f32) as f64;
                    }
                    vec.push(Real::new(v).ok()); // NaN to None
                }
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Real vector",
                        flag
                    )));
                }
            },
            VectorValue::Decimal(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In both index and record, it's flag is `DECIMAL`. See TiDB's `encode()`.
                datum::DECIMAL_FLAG => vec.push(Some(decode_decimal(&mut raw_datum)?)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Decimal vector",
                        flag
                    )));
                }
            },
            VectorValue::Bytes(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In index, it's flag is `BYTES`. See TiDB's `encode()`.
                datum::BYTES_FLAG => vec.push(Some(decode_bytes(&mut raw_datum)?)),
                // In record, it's flag is `COMPACT_BYTES`. See TiDB's `encode()`.
                datum::COMPACT_BYTES_FLAG => vec.push(Some(decode_compact_bytes(&mut raw_datum)?)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Bytes vector",
                        flag
                    )));
                }
            },
            VectorValue::DateTime(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In index, it's flag is `UINT`. See TiDB's `encode()`.
                datum::UINT_FLAG => {
                    let v = decode_uint(&mut raw_datum)?;
                    let v = decode_date_time_from_uint(v, time_zone, field_type)?;
                    vec.push(Some(v));
                }
                // In record, it's flag is `VAR_UINT`. See TiDB's `flatten()` and `encode()`.
                datum::VAR_UINT_FLAG => {
                    let v = decode_var_uint(&mut raw_datum)?;
                    let v = decode_date_time_from_uint(v, time_zone, field_type)?;
                    vec.push(Some(v));
                }
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for DateTime vector",
                        flag
                    )));
                }
            },
            VectorValue::Duration(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In index, it's flag is `DURATION`. See TiDB's `encode()`.
                datum::DURATION_FLAG => {
                    let v = decode_int(&mut raw_datum)?;
                    let v = decode_duration_from_i64(v)?;
                    vec.push(Some(v));
                }
                // In record, it's flag is `VAR_INT`. See TiDB's `flatten()` and `encode()`.
                datum::VAR_INT_FLAG => {
                    let v = decode_var_int(&mut raw_datum)?;
                    let v = decode_duration_from_i64(v)?;
                    vec.push(Some(v));
                }
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Duration vector",
                        flag
                    )));
                }
            },
            VectorValue::Json(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In both index and record, it's flag is `JSON`. See TiDB's `encode()`.
                datum::JSON_FLAG => vec.push(Some(decode_json(&mut raw_datum)?)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Json vector",
                        flag
                    )));
                }
            },
        }

        Ok(())
    }

    /// Returns maximum encoded size in binary format.
    pub fn maximum_encoded_size(&self) -> Result<usize> {
        match self {
            VectorValue::Int(ref vec) => Ok(vec.len() * 9),

            // Some elements might be NULLs which encoded size is 1 byte. However it's fine because
            // this function only calculates a maximum encoded size (for constructing buffers), not
            // actual encoded size.
            VectorValue::Real(ref vec) => Ok(vec.len() * 9),
            VectorValue::Decimal(ref vec) => {
                let mut size = 0;
                for el in vec {
                    match el {
                        Some(v) => {
                            // FIXME: We don't need approximate size. Maximum size is enough (so
                            // that we don't need to iterate each value).
                            size += 1 /* FLAG */ + v.approximate_encoded_size();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                Ok(size)
            }
            VectorValue::Bytes(ref vec) => {
                let mut size = 0;
                for el in vec {
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + 10 /* MAX VARINT LEN */ + v.len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                Ok(size)
            }
            VectorValue::DateTime(ref vec) => Ok(vec.len() * 9),
            VectorValue::Duration(ref vec) => Ok(vec.len() * 9),
            VectorValue::Json(ref vec) => {
                let mut size = 0;
                for el in vec {
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + v.binary_len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                Ok(size)
            }
        }
    }

    /// Encodes a single element into binary format.
    // FIXME: Use BufferWriter.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::coprocessor::codec::mysql::DecimalEncoder;
        use crate::coprocessor::codec::mysql::JsonEncoder;
        use tikv_util::codec::bytes::BytesEncoder;
        use tikv_util::codec::number::NumberEncoder;

        match self {
            VectorValue::Int(ref vec) => {
                match vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(val) => {
                        // Always encode to INT / UINT instead of VAR INT to be efficient.
                        if field_type.flag().contains(FieldTypeFlag::UNSIGNED) {
                            output.push(datum::UINT_FLAG);
                            output.encode_u64(val as u64)?;
                        } else {
                            output.push(datum::INT_FLAG);
                            output.encode_i64(val)?;
                        }
                    }
                }
                Ok(())
            }
            VectorValue::Real(ref vec) => {
                match vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(val) => {
                        output.push(datum::FLOAT_FLAG);
                        output.encode_f64(val.into_inner())?;
                    }
                }
                Ok(())
            }
            VectorValue::Decimal(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(val) => {
                        output.push(datum::DECIMAL_FLAG);
                        let (prec, frac) = val.prec_and_frac();
                        output.encode_decimal(val, prec, frac)?;
                    }
                }
                Ok(())
            }
            VectorValue::Bytes(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(ref val) => {
                        output.push(datum::COMPACT_BYTES_FLAG);
                        output.encode_compact_bytes(val)?;
                    }
                }
                Ok(())
            }
            VectorValue::DateTime(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(ref val) => {
                        output.push(datum::UINT_FLAG);
                        output.encode_u64(val.to_packed_u64())?;
                    }
                }
                Ok(())
            }
            VectorValue::Duration(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(ref val) => {
                        output.push(datum::DURATION_FLAG);
                        output.encode_i64(val.to_nanos())?;
                    }
                }
                Ok(())
            }
            VectorValue::Json(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.push(datum::NIL_FLAG);
                    }
                    Some(ref val) => {
                        output.push(datum::JSON_FLAG);
                        output.encode_json(val)?;
                    }
                }
                Ok(())
            }
        }
    }
}

macro_rules! impl_as_slice {
    ($ty:tt, $name:ident) => {
        impl VectorValue {
            /// Extracts a slice of values in specified concrete type from current column.
            ///
            /// # Panics
            ///
            /// Panics if the current column does not match the type.
            #[inline]
            pub fn $name(&self) -> &[Option<$ty>] {
                match self {
                    VectorValue::$ty(vec) => vec.as_slice(),
                    other => panic!(
                        "Cannot call `{}` over a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                }
            }
        }

        impl AsRef<[Option<$ty>]> for VectorValue {
            #[inline]
            fn as_ref(&self) -> &[Option<$ty>] {
                self.$name()
            }
        }

        // TODO: We should only expose interface for push value, not the entire Vec.
        impl AsMut<Vec<Option<$ty>>> for VectorValue {
            #[inline]
            fn as_mut(&mut self) -> &mut Vec<Option<$ty>> {
                match self {
                    VectorValue::$ty(ref mut vec) => vec,
                    other => panic!(
                        "Cannot retrieve a mutable `{}` vector over a {} column",
                        stringify!($ty),
                        other.eval_type()
                    ),
                }
            }
        }
    };
}

impl_as_slice! { Int, as_int_slice }
impl_as_slice! { Real, as_real_slice }
impl_as_slice! { Decimal, as_decimal_slice }
impl_as_slice! { Bytes, as_bytes_slice }
impl_as_slice! { DateTime, as_date_time_slice }
impl_as_slice! { Duration, as_duration_slice }
impl_as_slice! { Json, as_json_slice }

/// Additional `VectorValue` methods available via generics. These methods support different
/// concrete types but have same names and should be specified via the generic parameter type.
pub trait VectorValueExt<T: Evaluable> {
    /// The generic version for `VectorValue::push_xxx()`.
    fn push(&mut self, v: Option<T>);
}

macro_rules! impl_ext {
    ($ty:tt, $push_name:ident) => {
        // Explicit version

        impl VectorValue {
            /// Pushes a value in specified concrete type into current column.
            ///
            /// # Panics
            ///
            /// Panics if the current column does not match the type.
            #[inline]
            pub fn $push_name(&mut self, v: Option<$ty>) {
                match self {
                    VectorValue::$ty(ref mut vec) => vec.push(v),
                    other => panic!(
                        "Cannot call `{}` over to a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                };
            }
        }

        // Implicit version

        impl VectorValueExt<$ty> for VectorValue {
            #[inline]
            fn push(&mut self, v: Option<$ty>) {
                self.$push_name(v);
            }
        }
    };
}

impl_ext! { Int, push_int }
impl_ext! { Real, push_real }
impl_ext! { Decimal, push_decimal }
impl_ext! { Bytes, push_bytes }
impl_ext! { DateTime, push_date_time }
impl_ext! { Duration, push_duration }
impl_ext! { Json, push_json }

macro_rules! impl_from {
    ($ty:tt) => {
        impl From<Vec<Option<$ty>>> for VectorValue {
            #[inline]
            fn from(s: Vec<Option<$ty>>) -> VectorValue {
                VectorValue::$ty(s)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut column = VectorValue::with_capacity(0, EvalType::Bytes);
        assert_eq!(column.eval_type(), EvalType::Bytes);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 0);
        assert!(column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[]);

        column.push_bytes(None);
        assert_eq!(column.len(), 1);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[None]);

        column.push_bytes(Some(vec![1, 2, 3]));
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[None, Some(vec![1, 2, 3])]);

        let mut column = VectorValue::with_capacity(3, EvalType::Real);
        assert_eq!(column.eval_type(), EvalType::Real);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        assert!(column.is_empty());
        assert_eq!(column.as_real_slice(), &[]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(Real::new(1.0).ok());
        assert_eq!(column.len(), 1);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Real::new(1.0).ok()]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(None);
        assert_eq!(column.len(), 2);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Real::new(1.0).ok(), None]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(Real::new(4.5).ok());
        assert_eq!(column.len(), 3);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(
            column.as_real_slice(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok()]
        );
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(None);
        assert_eq!(column.len(), 4);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(
            column.as_real_slice(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok(), None]
        );
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.truncate(2);
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Real::new(1.0).ok(), None]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        let column = VectorValue::with_capacity(10, EvalType::DateTime);
        assert_eq!(column.eval_type(), EvalType::DateTime);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 10);
        assert!(column.is_empty());
        assert_eq!(column.as_date_time_slice(), &[]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(
            column.clone().as_date_time_slice(),
            column.as_date_time_slice()
        );
    }

    #[test]
    fn test_retain_by_index() {
        let mut column = VectorValue::with_capacity(3, EvalType::Real);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        column.retain_by_index(|_| true);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        column.retain_by_index(|_| false);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);

        column.push_real(None);
        column.push_real(Real::new(2.0).ok());
        column.push_real(Real::new(1.0).ok());
        column.push_real(None);
        column.push_real(Real::new(5.0).ok());
        column.push_real(None);

        let retain_map = &[true, true, false, false, true, false];
        column.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(column.len(), 3);
        assert!(column.capacity() > 3);
        assert_eq!(
            column.as_real_slice(),
            &[None, Real::new(2.0).ok(), Real::new(5.0).ok()]
        );

        column.push_real(None);
        column.push_real(Real::new(1.5).ok());
        column.push_real(None);
        column.push_real(Real::new(4.0).ok());

        assert_eq!(column.len(), 7);
        assert_eq!(
            column.as_real_slice(),
            &[
                None,
                Real::new(2.0).ok(),
                Real::new(5.0).ok(),
                None,
                Real::new(1.5).ok(),
                None,
                Real::new(4.0).ok()
            ]
        );

        let retain_map = &[true, false, true, false, false, true, true];
        column.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(column.len(), 4);
        assert_eq!(
            column.as_real_slice(),
            &[None, Real::new(5.0).ok(), None, Real::new(4.0).ok()]
        );

        column.retain_by_index(|_| true);
        assert_eq!(column.len(), 4);
        assert_eq!(
            column.as_real_slice(),
            &[None, Real::new(5.0).ok(), None, Real::new(4.0).ok()]
        );

        column.retain_by_index(|_| false);
        assert_eq!(column.len(), 0);
        assert_eq!(column.as_real_slice(), &[]);

        column.push_real(None);
        column.push_real(Real::new(1.5).ok());
        assert_eq!(column.as_real_slice(), &[None, Real::new(1.5).ok()]);
    }

    #[test]
    fn test_append() {
        let mut column1 = VectorValue::with_capacity(0, EvalType::Real);
        let mut column2 = VectorValue::with_capacity(3, EvalType::Real);

        column1.append(&mut column2);
        assert_eq!(column1.len(), 0);
        assert_eq!(column1.capacity(), 0);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);

        column2.push_real(Real::new(1.0).ok());
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert_eq!(column1.capacity(), 0);
        assert_eq!(column1.as_real_slice(), &[]);
        assert_eq!(column2.len(), 1);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.as_real_slice(), &[Real::new(1.0).ok()]);

        column1.push_real(None);
        column1.push_real(None);
        column1.append(&mut column2);
        assert_eq!(column1.len(), 3);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.as_real_slice(), &[None, None, Real::new(1.0).ok()]);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.as_real_slice(), &[]);

        column1.push_real(Real::new(1.1).ok());
        column2.push_real(Real::new(3.5).ok());
        column2.push_real(Real::new(4.1).ok());
        column2.truncate(1);
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.as_real_slice(), &[]);
        assert_eq!(column2.len(), 5);
        assert!(column2.capacity() > 3);
        assert_eq!(
            column2.as_real_slice(),
            &[
                Real::new(3.5).ok(),
                None,
                None,
                Real::new(1.0).ok(),
                Real::new(1.1).ok()
            ]
        );
    }

    #[test]
    fn test_from() {
        let slice: &[_] = &[None, Real::new(1.0).ok()];
        let vec = slice.to_vec();
        let column = VectorValue::from(vec);
        assert_eq!(column.len(), 2);
        assert_eq!(column.as_real_slice(), slice);
    }
}

#[cfg(test)]
mod benches {
    use crate::test;

    use super::*;

    #[bench]
    fn bench_push_datum_int(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = VectorValue::with_capacity(1000, EvalType::Int);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        let field_type = FieldTypeTp::LongLong.into();
        let tz = Tz::utc();

        b.iter(move || {
            for _ in 0..1000 {
                column
                    .push_datum(
                        test::black_box(&datum_raw),
                        test::black_box(&tz),
                        test::black_box(&field_type),
                    )
                    .unwrap();
            }
            test::black_box(&column);
            column.clear();
        });
    }

    /// Bench performance of naively decoding multiple datums (without pushing into a vector).
    #[bench]
    fn bench_batch_decode(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use crate::coprocessor::codec::table;
        use crate::coprocessor::dag::expr::EvalContext;
        use cop_datatype::FieldTypeTp;

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        let col_info = {
            let mut col_info = tipb::schema::ColumnInfo::new();
            col_info.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            col_info
        };
        let eval_ctx = EvalContext::default();

        b.iter(|| {
            for _ in 0..1000 {
                let mut raw = test::black_box(&datum_raw).as_slice();
                let datum = table::decode_col_value(
                    &mut raw,
                    test::black_box(&eval_ctx),
                    test::black_box(&col_info),
                )
                .unwrap();
                match datum {
                    Datum::I64(v) => {
                        test::black_box(v);
                    }
                    Datum::U64(v) => {
                        test::black_box(v);
                    }
                    _ => {
                        panic!();
                    }
                }
            }
        });
    }

    /// Bench performance of retain by array. It is used in Selection executor.
    #[bench]
    fn bench_retain(b: &mut test::Bencher) {
        use rand;

        let mut column = VectorValue::with_capacity(1000, EvalType::Int);
        for _ in 0..1000 {
            column.push_int(Some(rand::random()));
        }

        // Filter out 20% elements
        let mut should_retain = vec![true; 1000];
        for retain in &mut should_retain {
            if rand::random::<f64>() >= 0.8 {
                *retain = false;
            }
        }

        b.iter(|| {
            let should_retain = test::black_box(&should_retain);
            let mut c = test::black_box(&column).clone();
            c.retain_by_index(|idx| should_retain[idx]);
            test::black_box(c);
        });
    }
}
