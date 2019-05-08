// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use smallvec::SmallVec;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::FieldType;

use crate::coprocessor::codec::data_type::VectorValue;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::codec::Result;

/// A container stores an array of datums, which can be either raw (not decoded), or decoded into
/// the `VectorValue` type.
///
/// TODO:
/// Since currently the data format in response can be the same as in storage, we use this structure
/// to avoid unnecessary repeated serialization / deserialization. In future, Coprocessor will
/// respond all data in Arrow format which is different to the format in storage. At that time,
/// this structure is no longer useful and should be removed.
pub enum LazyBatchColumn {
    /// Ensure that small datum values (i.e. Int, Real, Time) are stored compactly.
    /// When using VarInt encoding there are 9 bytes. Plus 1 extra byte to store the datum flag.
    /// Thus totally there are 10 bytes.
    Raw(Vec<SmallVec<[u8; 10]>>),
    Decoded(VectorValue),
}

impl std::fmt::Debug for LazyBatchColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LazyBatchColumn::Raw(ref v) => {
                let vec_display: Vec<_> = v
                    .iter()
                    .map(|item| tikv_util::escape(item.as_slice()))
                    .collect();
                f.debug_tuple("Raw").field(&vec_display).finish()
            }
            LazyBatchColumn::Decoded(ref v) => f.debug_tuple("Decoded").field(v).finish(),
        }
    }
}

impl From<VectorValue> for LazyBatchColumn {
    #[inline]
    fn from(vec: VectorValue) -> Self {
        LazyBatchColumn::Decoded(vec)
    }
}

impl Clone for LazyBatchColumn {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            LazyBatchColumn::Raw(v) => {
                // This is much more efficient than `SmallVec::clone`.
                let mut raw_vec = Vec::with_capacity(v.capacity());
                for d in v {
                    raw_vec.push(SmallVec::from_slice(d.as_slice()));
                }
                LazyBatchColumn::Raw(raw_vec)
            }
            LazyBatchColumn::Decoded(v) => LazyBatchColumn::Decoded(v.clone()),
        }
    }
}

impl LazyBatchColumn {
    /// Creates a new `LazyBatchColumn::Raw` with specified capacity.
    #[inline]
    pub fn raw_with_capacity(capacity: usize) -> Self {
        LazyBatchColumn::Raw(Vec::with_capacity(capacity))
    }

    /// Creates a new `LazyBatchColumnb::Decoded` with specified capacity and eval type.
    #[inline]
    pub fn decoded_with_capacity_and_tp(capacity: usize, eval_tp: EvalType) -> Self {
        LazyBatchColumn::Decoded(VectorValue::with_capacity(capacity, eval_tp))
    }

    #[inline]
    pub fn is_raw(&self) -> bool {
        match self {
            LazyBatchColumn::Raw(_) => true,
            LazyBatchColumn::Decoded(_) => false,
        }
    }

    #[inline]
    pub fn is_decoded(&self) -> bool {
        match self {
            LazyBatchColumn::Raw(_) => false,
            LazyBatchColumn::Decoded(_) => true,
        }
    }

    #[inline]
    pub fn decoded(&self) -> &VectorValue {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(ref v) => v,
        }
    }

    #[inline]
    pub fn mut_decoded(&mut self) -> &mut VectorValue {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(ref mut v) => v,
        }
    }

    #[inline]
    pub fn raw(&self) -> &Vec<SmallVec<[u8; 10]>> {
        match self {
            LazyBatchColumn::Raw(ref v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn mut_raw(&mut self) -> &mut Vec<SmallVec<[u8; 10]>> {
        match self {
            LazyBatchColumn::Raw(ref mut v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(ref v) => v.len(),
            LazyBatchColumn::Decoded(ref v) => v.len(),
        }
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match self {
            LazyBatchColumn::Raw(ref mut v) => v.truncate(len),
            LazyBatchColumn::Decoded(ref mut v) => v.truncate(len),
        };
    }

    #[inline]
    pub fn clear(&mut self) {
        self.truncate(0)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(ref v) => v.capacity(),
            LazyBatchColumn::Decoded(ref v) => v.capacity(),
        }
    }

    #[inline]
    pub fn retain_by_index<F>(&mut self, mut f: F)
    where
        F: FnMut(usize) -> bool,
    {
        match self {
            LazyBatchColumn::Raw(ref mut v) => {
                let mut idx = 0;
                v.retain(|_| {
                    let r = f(idx);
                    idx += 1;
                    r
                });
            }
            LazyBatchColumn::Decoded(ref mut v) => {
                v.retain_by_index(f);
            }
        }
    }

    /// Decodes this column in place if the column is not decoded.
    ///
    /// The field type is needed because we use the same `DateTime` structure when handling
    /// Date, Time or Timestamp.
    // TODO: Maybe it's a better idea to assign different eval types for different date types.
    pub fn decode(&mut self, time_zone: &Tz, field_type: &FieldType) -> Result<()> {
        if self.is_decoded() {
            return Ok(());
        }

        let eval_type = box_try!(EvalType::try_from(field_type.tp()));

        let mut decoded_column = VectorValue::with_capacity(self.capacity(), eval_type);
        {
            let raw_values = self.raw();
            for raw_value in raw_values {
                let raw_datum = raw_value.as_slice();
                decoded_column.push_datum(raw_datum, time_zone, field_type)?;
            }
        }
        *self = LazyBatchColumn::Decoded(decoded_column);

        Ok(())
    }

    /// Push a raw datum which is not yet decoded.
    ///
    /// `raw_datum.len()` can be 0, indicating a missing value for corresponding cell.
    ///
    /// # Panics
    ///
    /// Panics when current column is already decoded.
    // TODO: Deprecate this function. mut_raw should be preferred so that there won't be repeated
    // match for each iteration.
    #[inline]
    pub fn push_raw(&mut self, raw_datum: impl AsRef<[u8]>) {
        match self {
            LazyBatchColumn::Raw(ref mut v) => {
                v.push(SmallVec::from_slice(raw_datum.as_ref()));
            }
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    /// Moves all elements of `other` into `Self`, leaving `other` empty.
    ///
    /// # Panics
    ///
    /// Panics when `other` and `Self` does not have identical decoded status or identical
    /// `EvalType`, i.e. one is `decoded` but another is `raw`, or one is `decoded(Int)` but
    /// another is `decoded(Real)`.
    pub fn append(&mut self, other: &mut Self) {
        match self {
            LazyBatchColumn::Raw(ref mut dest) => match other {
                LazyBatchColumn::Raw(ref mut src) => dest.append(src),
                _ => panic!("Cannot append decoded LazyBatchColumn into raw LazyBatchColumn"),
            },
            LazyBatchColumn::Decoded(ref mut dest) => match other {
                LazyBatchColumn::Decoded(ref mut src) => dest.append(src),
                _ => panic!("Cannot append raw LazyBatchColumn into decoded LazyBatchColumn"),
            },
        }
    }

    /// Returns maximum encoded size.
    pub fn maximum_encoded_size(&self) -> Result<usize> {
        match self {
            LazyBatchColumn::Raw(ref v) => {
                let mut size = 0;
                for s in v {
                    size += s.len();
                }
                Ok(size)
            }
            LazyBatchColumn::Decoded(ref v) => v.maximum_encoded_size(),
        }
    }

    /// Encodes into binary format.
    // FIXME: Use BufferWriter.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            LazyBatchColumn::Raw(ref v) => {
                output.extend_from_slice(v[row_index].as_slice());
                Ok(())
            }
            LazyBatchColumn::Decoded(ref v) => v.encode(row_index, field_type, output),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::coprocessor::codec::datum::{Datum, DatumEncoder};

    #[test]
    fn test_lazy_batch_column_clone() {
        use cop_datatype::FieldTypeTp;

        let mut col = LazyBatchColumn::raw_with_capacity(5);
        assert!(col.is_raw());
        assert_eq!(col.len(), 0);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.raw().len(), 0);
        {
            // Clone empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.raw().len(), 0);
        }
        {
            // Empty raw to empty decoded.
            let mut col = col.clone();
            col.decode(&Tz::utc(), &FieldTypeTp::Long.into()).unwrap();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.decoded().as_int_slice(), &[]);
            {
                // Clone empty decoded LazyBatchColumn.
                let col = col.clone();
                assert!(col.is_decoded());
                assert_eq!(col.len(), 0);
                assert_eq!(col.capacity(), 5);
                assert_eq!(col.decoded().as_int_slice(), &[]);
            }
        }

        let mut datum_raw_1 = Vec::new();
        DatumEncoder::encode(&mut datum_raw_1, &[Datum::U64(32)], false).unwrap();
        col.push_raw(&datum_raw_1);

        let mut datum_raw_2 = Vec::new();
        DatumEncoder::encode(&mut datum_raw_2, &[Datum::U64(7)], true).unwrap();
        col.push_raw(&datum_raw_2);

        assert!(col.is_raw());
        assert_eq!(col.len(), 2);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.raw().len(), 2);
        assert_eq!(col.raw()[0].as_slice(), datum_raw_1.as_slice());
        assert_eq!(col.raw()[1].as_slice(), datum_raw_2.as_slice());
        {
            // Clone non-empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 2);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.raw().len(), 2);
            assert_eq!(col.raw()[0].as_slice(), datum_raw_1.as_slice());
            assert_eq!(col.raw()[1].as_slice(), datum_raw_2.as_slice());
        }
        // Non-empty raw to non-empty decoded.
        col.decode(&Tz::utc(), &FieldTypeTp::Long.into()).unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 2);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.decoded().as_int_slice(), &[Some(32), Some(7)]);
        {
            // Clone non-empty decoded LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 2);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.decoded().as_int_slice(), &[Some(32), Some(7)]);
        }
    }
}

#[cfg(test)]
mod benches {
    use super::*;

    #[bench]
    fn bench_lazy_batch_column_push_raw_4bytes(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 4];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.push_raw(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    #[bench]
    fn bench_lazy_batch_column_push_raw_9bytes(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 9];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.push_raw(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// 10 bytes > inline size for LazyBatchColumn, which will be slower.
    /// This benchmark shows how slow it will be.
    #[bench]
    fn bench_lazy_batch_column_push_raw_10bytes(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.push_raw(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a raw column which size <= inline size.
    #[bench]
    fn bench_lazy_batch_column_clone(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 9];
        for _ in 0..1000 {
            column.push_raw(&val);
        }
        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    /// Bench performance of cloning a raw column which size > inline size.
    #[bench]
    fn bench_lazy_batch_column_clone_10bytes(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        for _ in 0..1000 {
            column.push_raw(&val);
        }
        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    /// Bench performance of naively cloning a raw column
    /// (which uses `SmallVec::clone()` instead of our own)
    #[bench]
    fn bench_lazy_batch_column_clone_naive(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        for _ in 0..1000 {
            column.push_raw(&val);
        }
        b.iter(|| match test::black_box(&column) {
            LazyBatchColumn::Raw(raw_vec) => {
                test::black_box(raw_vec.clone());
            }
            _ => panic!(),
        })
    }

    /// Bench performance of cloning a decoded column.
    #[bench]
    fn bench_lazy_batch_column_clone_decoded(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use cop_datatype::FieldTypeTp;

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }

        column
            .decode(&Tz::utc(), &FieldTypeTp::LongLong.into())
            .unwrap();

        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    /// Bench performance of decoding a raw batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use cop_datatype::FieldTypeTp;

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }

        let ft = FieldTypeTp::LongLong.into();
        let tz = Tz::utc();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.decode(test::black_box(&tz), test::black_box(&ft))
                .unwrap();
            test::black_box(&col);
        });
    }

    /// Bench performance of decoding a decoded lazy batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode_decoded(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use cop_datatype::FieldTypeTp;

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }

        let ft = FieldTypeTp::LongLong.into();
        let tz = Tz::utc();

        column.decode(&tz, &ft).unwrap();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.decode(test::black_box(&tz), test::black_box(&ft))
                .unwrap();
            test::black_box(&col);
        });
    }

    /// A vector based LazyBatchColumn
    #[derive(Clone)]
    struct VectorLazyBatchColumn(Vec<Vec<u8>>);

    impl VectorLazyBatchColumn {
        #[inline]
        pub fn raw_with_capacity(capacity: usize) -> Self {
            VectorLazyBatchColumn(Vec::with_capacity(capacity))
        }

        #[inline]
        pub fn clear(&mut self) {
            self.0.clear();
        }

        #[inline]
        pub fn push_raw(&mut self, raw_datum: &[u8]) {
            self.0.push(raw_datum.to_vec());
        }
    }

    /// Bench performance of pushing 10 bytes to a vector based LazyBatchColumn.
    #[bench]
    fn bench_lazy_batch_column_by_vec_push_raw_10bytes(b: &mut test::Bencher) {
        let mut column = VectorLazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.push_raw(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a raw vector based LazyBatchColumn.
    #[bench]
    fn bench_lazy_batch_column_by_vec_clone(b: &mut test::Bencher) {
        let mut column = VectorLazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        for _ in 0..1000 {
            column.push_raw(&val);
        }
        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }
}
