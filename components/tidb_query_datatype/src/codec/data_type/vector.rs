// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{scalar::ScalarValueRef, *};
use crate::{
    codec::{mysql::decimal::DECIMAL_STRUCT_SIZE, Result},
    match_template_collator, match_template_evaltype, EvalType, FieldTypeAccessor,
};

/// A vector value container, a.k.a. column, for all concrete eval types.
///
/// The inner concrete value is immutable. However it is allowed to push and remove values from
/// this vector container.
#[derive(Debug, PartialEq, Clone)]
pub enum VectorValue {
    Int(ChunkedVecSized<Int>),
    Real(ChunkedVecSized<Real>),
    Decimal(ChunkedVecSized<Decimal>),
    // TODO: We need to improve its performance, i.e. store strings in adjacent memory places
    Bytes(ChunkedVecBytes),
    DateTime(ChunkedVecSized<DateTime>),
    Duration(ChunkedVecSized<Duration>),
    Json(ChunkedVecJson),
    Enum(ChunkedVecEnum),
    Set(ChunkedVecSet),
}

impl VectorValue {
    /// Creates an empty `VectorValue` according to `eval_tp` and reserves capacity according
    /// to `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize, eval_tp: EvalType) -> Self {
        match_template_evaltype! {
            TT, match eval_tp {
                EvalType::TT => VectorValue::TT(ChunkedVec::with_capacity(capacity)),
            }
        }
    }

    /// Creates a `VectorValue` of length `len` with the given value `scalar`.
    #[inline]
    pub fn from_scalar(scalar: &ScalarValue, len: usize) -> Self {
        macro_rules! expand_convertion {
            ($val:tt, $( $tp:tt : $chktp:ty ),* ) => {
                match &$val {
                    $(
                        &ScalarValue::$tp(val) => {
                            let mut v: $chktp = ChunkedVec::with_capacity(len);
                            match val {
                                None => {
                                    for _ in 0..len {
                                        v.push_null();
                                    }
                                },
                                Some(val) => {
                                    for _ in 0..len {
                                        v.push_data(val.clone());
                                    }
                                }
                            }
                            VectorValue::$tp(v)
                        }
                    )*
                }
            }
        }
        expand_convertion!(
            scalar,
            Int: ChunkedVecSized<Int>,
            Real: ChunkedVecSized<Real>,
            Decimal: ChunkedVecSized<Decimal>,
            DateTime: ChunkedVecSized<DateTime>,
            Duration: ChunkedVecSized<Duration>,
            Set: ChunkedVecSet,
            Json: ChunkedVecJson,
            Enum: ChunkedVecEnum,
            Bytes: ChunkedVecBytes
        )
    }

    /// Creates a new empty `VectorValue` with the same eval type.
    #[inline]
    #[must_use]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        match_template_evaltype! {
            TT, match self {
                VectorValue::TT(_) => VectorValue::TT(ChunkedVec::with_capacity(capacity)),
            }
        }
    }

    /// Returns the `EvalType` used to construct current column.
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaltype! {
            TT, match self {
                VectorValue::TT(_) => EvalType::TT,
            }
        }
    }

    /// Returns the number of datums contained in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match_template_evaltype! {
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
        match_template_evaltype! {
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
        match_template_evaltype! {
            TT, match self {
                VectorValue::TT(v) => v.capacity(),
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
        match_template_evaltype! {
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

    /// Evaluates values into MySQL logic values.
    ///
    /// The caller must provide an output buffer which is large enough for holding values.
    pub fn eval_as_mysql_bools(
        &self,
        ctx: &mut EvalContext,
        outputs: &mut [bool],
    ) -> tidb_query_common::error::Result<()> {
        assert!(outputs.len() >= self.len());
        match_template_evaltype! {
            TT, match self {
                VectorValue::TT(v) => {
                    let l = self.len();
                    for i in 0..l {
                        outputs[i] = v.get_option_ref(i).as_mysql_bool(ctx)?;
                    }
                },
            }
        }
        Ok(())
    }

    /// Gets a reference of the element in corresponding index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of range.
    #[inline]
    pub fn get_scalar_ref(&self, index: usize) -> ScalarValueRef<'_> {
        match_template_evaltype! {
            TT, match self {
                VectorValue::TT(v) => ScalarValueRef::TT(v.get_option_ref(index)),
            }
        }
    }

    /// Returns maximum encoded size in binary format.
    pub fn maximum_encoded_size(&self, logical_rows: &[usize]) -> usize {
        match self {
            VectorValue::Int(_) => logical_rows.len() * 9,

            // Some elements might be NULLs which encoded size is 1 byte. However it's fine because
            // this function only calculates a maximum encoded size (for constructing buffers), not
            // actual encoded size.
            VectorValue::Real(_) => logical_rows.len() * 9,
            VectorValue::Decimal(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
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
                size
            }
            VectorValue::Bytes(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + 10 /* MAX VARINT LEN */ + v.len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                size
            }
            VectorValue::DateTime(_) => logical_rows.len() * 9,
            VectorValue::Duration(_) => logical_rows.len() * 9,
            VectorValue::Json(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + v.binary_len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                size
            }
            VectorValue::Enum(_) => logical_rows.len() * 9,
            // TODO: implement here after we implement set encoding
            VectorValue::Set(_) => unimplemented!(),
        }
    }

    /// Returns maximum encoded size in chunk format.
    pub fn maximum_encoded_size_chunk(&self, logical_rows: &[usize]) -> usize {
        match self {
            VectorValue::Int(_) => logical_rows.len() * 9 + 10,
            VectorValue::Real(_) => logical_rows.len() * 9 + 10,
            VectorValue::Decimal(_) => logical_rows.len() * (DECIMAL_STRUCT_SIZE + 1) + 10,
            VectorValue::DateTime(_) => logical_rows.len() * 21 + 10,
            VectorValue::Duration(_) => logical_rows.len() * 9 + 10,
            VectorValue::Bytes(vec) => {
                let mut size = logical_rows.len() + 10;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.len();
                        }
                        None => {
                            size +=  8 /* Offset */;
                        }
                    }
                }
                size
            }
            VectorValue::Json(vec) => {
                let mut size = logical_rows.len() + 10;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.binary_len();
                        }
                        None => {
                            size += 8 /* Offset */;
                        }
                    }
                }
                size
            }
            VectorValue::Enum(vec) => {
                let mut size = logical_rows.len() * 9 + 10;
                for idx in logical_rows {
                    let el = vec.get_option_ref(*idx);
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.len();
                        }
                        None => {
                            size += 8;
                        }
                    }
                }
                size
            }
            // TODO: implement here after we implement set encoding
            VectorValue::Set(_) => unimplemented!(),
        }
    }

    /// Encodes a single element into binary format.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &impl FieldTypeAccessor,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::codec::datum_codec::EvaluableDatumEncoder;

        match self {
            VectorValue::Int(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        // Always encode to INT / UINT instead of VAR INT to be efficient.
                        let is_unsigned = field_type.is_unsigned();
                        output.write_evaluable_datum_int(*val, is_unsigned)?;
                    }
                }
                Ok(())
            }
            VectorValue::Real(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_real(val.into_inner())?;
                    }
                }
                Ok(())
            }
            VectorValue::Decimal(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_decimal(*val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Bytes(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_bytes(val)?;
                    }
                }
                Ok(())
            }
            VectorValue::DateTime(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_date_time(*val, ctx)?;
                    }
                }
                Ok(())
            }
            VectorValue::Duration(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_duration(*val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Json(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_json(*val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Enum(ref vec) => {
                match &vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_enum_uint(*val)?;
                    }
                }
                Ok(())
            }
            // TODO: implement set encoding
            VectorValue::Set(_) => unimplemented!(),
        }
    }

    pub fn encode_sort_key(
        &self,
        row_index: usize,
        field_type: &impl FieldTypeAccessor,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::{
            codec::{collation::Collator, datum_codec::EvaluableDatumEncoder},
            Collation,
        };

        match self {
            VectorValue::Bytes(ref vec) => {
                match vec.get_option_ref(row_index) {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        let sort_key = match_template_collator! {
                            TT, match field_type.collation()? {
                                Collation::TT => TT::sort_key(val)?
                            }
                        };
                        output.write_evaluable_datum_bytes(&sort_key)?;
                    }
                }
                Ok(())
            }
            _ => self.encode(row_index, field_type, ctx, output),
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
            pub fn $name(&self) -> Vec<Option<$ty>> {
                match self {
                    VectorValue::$ty(vec) => vec.to_vec(),
                    other => panic!(
                        "Cannot call `{}` over a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                }
            }
        }
    };
}

impl_as_slice! { Int, to_int_vec }
impl_as_slice! { Real, to_real_vec }
impl_as_slice! { Decimal, to_decimal_vec }
impl_as_slice! { Bytes, to_bytes_vec }
impl_as_slice! { DateTime, to_date_time_vec }
impl_as_slice! { Duration, to_duration_vec }
impl_as_slice! { Json, to_json_vec }
impl_as_slice! { Enum, to_enum_vec }
impl_as_slice! { Set, to_set_vec }

/// Additional `VectorValue` methods available via generics. These methods support different
/// concrete types but have same names and should be specified via the generic parameter type.
pub trait VectorValueExt<T: EvaluableRet> {
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
                        "Cannot call `{}` over a {} column",
                        stringify!($push_name),
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
impl_ext! { Enum, push_enum }
impl_ext! { Set, push_set }

macro_rules! impl_from {
    ($ty:tt, $chunk:ty) => {
        impl From<$chunk> for VectorValue {
            #[inline]
            fn from(s: $chunk) -> VectorValue {
                VectorValue::$ty(s)
            }
        }
    };
}

impl_from! { Int, ChunkedVecSized<Int> }
impl_from! { Real, ChunkedVecSized<Real> }
impl_from! { Decimal, ChunkedVecSized<Decimal> }
impl_from! { Bytes, ChunkedVecBytes }
impl_from! { DateTime, ChunkedVecSized<DateTime> }
impl_from! { Duration, ChunkedVecSized<Duration> }
impl_from! { Json, ChunkedVecJson }
impl_from! { Enum, ChunkedVecEnum }
impl_from! { Set, ChunkedVecSet }

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
        assert_eq!(column.to_bytes_vec(), &[]);

        column.push_bytes(None);
        assert_eq!(column.len(), 1);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.to_bytes_vec(), &[None]);

        column.push_bytes(Some(vec![1, 2, 3]));
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.to_bytes_vec(), &[None, Some(vec![1, 2, 3])]);

        let mut column = VectorValue::with_capacity(3, EvalType::Real);
        assert_eq!(column.eval_type(), EvalType::Real);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        assert!(column.is_empty());
        assert_eq!(column.to_real_vec(), &[]);
        let column_cloned = column.clone();
        assert_eq!(column_cloned.capacity(), 0);
        assert_eq!(column_cloned.to_real_vec(), column.to_real_vec());

        column.push_real(Real::new(1.0).ok());
        assert_eq!(column.len(), 1);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.to_real_vec(), &[Real::new(1.0).ok()]);
        let column_cloned = column.clone();
        assert_eq!(column_cloned.capacity(), 1);
        assert_eq!(column_cloned.to_real_vec(), column.to_real_vec());

        column.push_real(None);
        assert_eq!(column.len(), 2);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.to_real_vec(), &[Real::new(1.0).ok(), None]);
        let column_cloned = column.clone();
        assert_eq!(column_cloned.capacity(), 2);
        assert_eq!(column_cloned.to_real_vec(), column.to_real_vec());

        column.push_real(Real::new(4.5).ok());
        assert_eq!(column.len(), 3);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(
            column.to_real_vec(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok()]
        );
        let column_cloned = column.clone();
        assert_eq!(column_cloned.capacity(), 3);
        assert_eq!(column_cloned.to_real_vec(), column.to_real_vec());

        column.push_real(None);
        assert_eq!(column.len(), 4);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(
            column.to_real_vec(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok(), None]
        );
        assert_eq!(column.to_real_vec(), column.to_real_vec());

        column.truncate(2);
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.to_real_vec(), &[Real::new(1.0).ok(), None]);
        assert_eq!(column.to_real_vec(), column.to_real_vec());

        let column = VectorValue::with_capacity(10, EvalType::DateTime);
        assert_eq!(column.eval_type(), EvalType::DateTime);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 10);
        assert!(column.is_empty());
        assert_eq!(column.to_date_time_vec(), &[]);
        assert_eq!(column.to_date_time_vec(), column.to_date_time_vec());

        let column = VectorValue::with_capacity(10, EvalType::Enum);
        assert_eq!(column.eval_type(), EvalType::Enum);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 10);
        assert!(column.is_empty());
        assert_eq!(column.to_enum_vec(), &[]);
        assert_eq!(column.to_enum_vec(), column.to_enum_vec());

        let column = VectorValue::with_capacity(10, EvalType::Set);
        assert_eq!(column.eval_type(), EvalType::Set);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 10);
        assert!(column.is_empty());
        assert_eq!(column.to_set_vec(), &[]);
        assert_eq!(column.to_set_vec(), column.to_set_vec());
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
        assert_eq!(column1.to_real_vec(), &[]);
        assert_eq!(column2.len(), 1);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.to_real_vec(), &[Real::new(1.0).ok()]);

        column1.push_real(None);
        column1.push_real(None);
        column1.append(&mut column2);
        assert_eq!(column1.len(), 3);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.to_real_vec(), &[None, None, Real::new(1.0).ok()]);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.to_real_vec(), &[]);

        column1.push_real(Real::new(1.1).ok());
        column2.push_real(Real::new(3.5).ok());
        column2.push_real(Real::new(4.1).ok());
        column2.truncate(1);
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.to_real_vec(), &[]);
        assert_eq!(column2.len(), 5);
        assert!(column2.capacity() > 3);
        assert_eq!(
            column2.to_real_vec(),
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
        let chunked_vec = ChunkedVecSized::from_slice(slice);
        let column = VectorValue::from(chunked_vec);
        assert_eq!(column.len(), 2);
        assert_eq!(column.to_real_vec(), slice);
    }
}
