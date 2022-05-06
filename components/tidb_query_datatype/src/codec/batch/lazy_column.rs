// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use tikv_util::buffer_vec::BufferVec;
use tipb::FieldType;

use crate::{
    codec::{
        chunk::{ChunkColumnEncoder, Column},
        data_type::{ChunkedVec, LogicalRows, VectorValue},
        datum_codec::RawDatumDecoder,
        Result,
    },
    expr::EvalContext,
    match_template_evaltype, EvalType, FieldTypeAccessor,
};

/// A container stores an array of datums, which can be either raw (not decoded), or decoded into
/// the `VectorValue` type.
///
/// TODO:
/// Since currently the data format in response can be the same as in storage, we use this structure
/// to avoid unnecessary repeated serialization / deserialization. In future, Coprocessor will
/// respond all data in Chunk format which is different to the format in storage. At that time,
/// this structure is no longer useful and should be removed.
#[derive(Clone, Debug)]
pub enum LazyBatchColumn {
    Raw(BufferVec),
    Decoded(VectorValue),
}

impl From<VectorValue> for LazyBatchColumn {
    #[inline]
    fn from(vec: VectorValue) -> Self {
        LazyBatchColumn::Decoded(vec)
    }
}

impl LazyBatchColumn {
    /// Creates a new `LazyBatchColumn::Raw` with specified capacity.
    #[inline]
    pub fn raw_with_capacity(capacity: usize) -> Self {
        use codec::number::MAX_VARINT64_LENGTH;
        // We assume that each element *may* has a size of MAX_VAR_INT_LEN + Datum Flag (1 byte).
        LazyBatchColumn::Raw(BufferVec::with_capacity(
            capacity,
            capacity * (MAX_VARINT64_LENGTH + 1),
        ))
    }

    /// Creates a new `LazyBatchColumn::Decoded` with specified capacity and eval type.
    #[inline]
    pub fn decoded_with_capacity_and_tp(capacity: usize, eval_tp: EvalType) -> Self {
        LazyBatchColumn::Decoded(VectorValue::with_capacity(capacity, eval_tp))
    }

    /// Creates a new empty `LazyBatchColumn` with the same schema.
    #[inline]
    #[must_use]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        match self {
            LazyBatchColumn::Raw(_) => Self::raw_with_capacity(capacity),
            LazyBatchColumn::Decoded(v) => LazyBatchColumn::Decoded(v.clone_empty(capacity)),
        }
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
            LazyBatchColumn::Decoded(v) => v,
        }
    }

    #[inline]
    pub fn mut_decoded(&mut self) -> &mut VectorValue {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(v) => v,
        }
    }

    #[inline]
    pub fn raw(&self) -> &BufferVec {
        match self {
            LazyBatchColumn::Raw(v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn mut_raw(&mut self) -> &mut BufferVec {
        match self {
            LazyBatchColumn::Raw(v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(v) => v.len(),
            LazyBatchColumn::Decoded(v) => v.len(),
        }
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match self {
            LazyBatchColumn::Raw(v) => v.truncate(len),
            LazyBatchColumn::Decoded(v) => v.truncate(len),
        };
    }

    #[inline]
    pub fn clear(&mut self) {
        match self {
            LazyBatchColumn::Raw(v) => v.clear(),
            LazyBatchColumn::Decoded(v) => v.clear(),
        };
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(v) => v.capacity(),
            LazyBatchColumn::Decoded(v) => v.capacity(),
        }
    }

    /// Decodes this column if the column is not decoded, according to the given logical rows map.
    /// After decoding, the decoded column will have the same physical layout as the encoded one
    /// (i.e. the same logical rows), but elements in unnecessary positions will not be decoded
    /// and will be `None`.
    ///
    /// The field type is needed because we use the same `DateTime` structure when handling
    /// Date, Time or Timestamp.
    // TODO: Maybe it's a better idea to assign different eval types for different date types.
    pub fn ensure_decoded(
        &mut self,
        ctx: &mut EvalContext,
        field_type: &FieldType,
        logical_rows: LogicalRows<'_>,
    ) -> Result<()> {
        if self.is_decoded() {
            return Ok(());
        }
        let eval_type = box_try!(EvalType::try_from(field_type.as_accessor().tp()));
        let raw_vec = self.raw();
        let raw_vec_len = raw_vec.len();

        let mut decoded_column = VectorValue::with_capacity(raw_vec_len, eval_type);

        match_template_evaltype! {
            TT, match &mut decoded_column {
                VectorValue::TT(vec) => {
                    match logical_rows {
                        LogicalRows::Identical { size } => {
                            for i in 0..size {
                                vec.push(raw_vec[i].decode(field_type, ctx)?);
                            }
                            for _ in size..raw_vec_len {
                                vec.push(None);
                            }
                        }
                        LogicalRows::Ref { logical_rows } => {
                            let mut decode_bitmap = vec![false; raw_vec_len];
                            for row_index in logical_rows {
                                decode_bitmap[*row_index] = true;
                            }
                            for i in 0..raw_vec_len {
                                if decode_bitmap[i] {
                                    vec.push(raw_vec[i].decode(field_type, ctx)?);
                                } else {
                                    vec.push(None);
                                }
                            }
                        }
                    }
                }
            }
        }

        *self = LazyBatchColumn::Decoded(decoded_column);

        Ok(())
    }

    pub fn ensure_all_decoded_for_test(
        &mut self,
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        let logical_rows = LogicalRows::Identical { size: self.len() };
        self.ensure_decoded(ctx, field_type, logical_rows)
    }

    /// Returns maximum encoded size.
    pub fn maximum_encoded_size(&self, logical_rows: &[usize]) -> usize {
        match self {
            LazyBatchColumn::Raw(v) => v.total_len(),
            LazyBatchColumn::Decoded(v) => v.maximum_encoded_size(logical_rows),
        }
    }

    /// Returns maximum encoded size in chunk format.
    pub fn maximum_encoded_size_chunk(&self, logical_rows: &[usize]) -> usize {
        match self {
            LazyBatchColumn::Raw(v) => v.total_len() * 2,
            LazyBatchColumn::Decoded(v) => v.maximum_encoded_size_chunk(logical_rows),
        }
    }

    /// Encodes into binary format.
    // FIXME: Use BufferWriter.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &impl FieldTypeAccessor,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            LazyBatchColumn::Raw(v) => {
                output.extend_from_slice(&v[row_index]);
                Ok(())
            }
            LazyBatchColumn::Decoded(ref v) => v.encode(row_index, field_type, ctx, output),
        }
    }

    /// Encodes into Chunk format.
    // FIXME: Use BufferWriter.
    pub fn encode_chunk(
        &self,
        ctx: &mut EvalContext,
        logical_rows: &[usize],
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let column = match self {
            LazyBatchColumn::Raw(v) => Column::from_raw_datums(field_type, v, logical_rows, ctx)?,
            LazyBatchColumn::Decoded(ref v) => {
                Column::from_vector_value(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::datum::{Datum, DatumEncoder};

    #[test]
    fn test_basic() {
        use crate::FieldTypeTp;

        let mut col = LazyBatchColumn::raw_with_capacity(5);
        let mut ctx = EvalContext::default();
        assert!(col.is_raw());
        assert_eq!(col.len(), 0);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.raw().len(), 0);
        {
            // Clone empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 0);
            assert_eq!(col.raw().len(), 0);
        }
        {
            // Empty raw to empty decoded.
            let mut col = col.clone();
            col.ensure_all_decoded_for_test(&mut ctx, &FieldTypeTp::Long.into())
                .unwrap();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 0);
            assert_eq!(col.decoded().to_int_vec(), &[]);
            {
                assert!(col.is_decoded());
                assert_eq!(col.len(), 0);
                assert_eq!(col.capacity(), 0);
                assert_eq!(col.decoded().to_int_vec(), &[]);
            }
        }

        let mut ctx = EvalContext::default();
        let mut datum_raw_1 = Vec::new();
        datum_raw_1
            .write_datum(&mut ctx, &[Datum::U64(32)], false)
            .unwrap();
        col.mut_raw().push(&datum_raw_1);

        let mut datum_raw_2 = Vec::new();
        datum_raw_2
            .write_datum(&mut ctx, &[Datum::U64(7)], true)
            .unwrap();
        col.mut_raw().push(&datum_raw_2);

        let mut datum_raw_3 = Vec::new();
        datum_raw_3
            .write_datum(&mut ctx, &[Datum::U64(10)], true)
            .unwrap();
        col.mut_raw().push(&datum_raw_3);

        assert!(col.is_raw());
        assert_eq!(col.len(), 3);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.raw().len(), 3);
        assert_eq!(&col.raw()[0], datum_raw_1.as_slice());
        assert_eq!(&col.raw()[1], datum_raw_2.as_slice());
        assert_eq!(&col.raw()[2], datum_raw_3.as_slice());
        {
            // Clone non-empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 3);
            assert_eq!(col.capacity(), 3);
            assert_eq!(col.raw().len(), 3);
            assert_eq!(&col.raw()[0], datum_raw_1.as_slice());
            assert_eq!(&col.raw()[1], datum_raw_2.as_slice());
            assert_eq!(&col.raw()[2], datum_raw_3.as_slice());
        }

        // Non-empty raw to non-empty decoded.
        col.ensure_decoded(
            &mut ctx,
            &FieldTypeTp::Long.into(),
            LogicalRows::from_slice(&[2, 0]),
        )
        .unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 3);
        assert_eq!(col.capacity(), 3);
        // Element 1 is None because it is not referred in `logical_rows` and we don't decode it.
        assert_eq!(col.decoded().to_int_vec(), &[Some(32), None, Some(10)]);

        {
            // Clone non-empty decoded LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 3);
            assert_eq!(col.capacity(), 3);
            assert_eq!(col.decoded().to_int_vec(), &[Some(32), None, Some(10)]);
        }

        // Decode a decoded column, even using a different logical rows, does not have effect.
        col.ensure_decoded(
            &mut ctx,
            &FieldTypeTp::Long.into(),
            LogicalRows::from_slice(&[0, 1]),
        )
        .unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 3);
        assert_eq!(col.capacity(), 3);
        assert_eq!(col.decoded().to_int_vec(), &[Some(32), None, Some(10)]);
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
                column.mut_raw().push(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a decoded column.
    #[bench]
    fn bench_lazy_batch_column_clone_decoded(b: &mut test::Bencher) {
        use crate::{
            codec::datum::{Datum, DatumEncoder},
            FieldTypeTp,
        };

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut ctx = EvalContext::default();
        let mut datum_raw: Vec<u8> = Vec::new();
        datum_raw
            .write_datum(&mut ctx, &[Datum::U64(0xDEADBEEF)], true)
            .unwrap();

        for _ in 0..1000 {
            column.mut_raw().push(datum_raw.as_slice());
        }
        let logical_rows = LogicalRows::Identical { size: 1000 };

        column
            .ensure_decoded(&mut ctx, &FieldTypeTp::LongLong.into(), logical_rows)
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
        use crate::{
            codec::datum::{Datum, DatumEncoder},
            FieldTypeTp,
        };

        let mut ctx = EvalContext::default();
        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        datum_raw
            .write_datum(&mut ctx, &[Datum::U64(0xDEADBEEF)], true)
            .unwrap();

        for _ in 0..1000 {
            column.mut_raw().push(datum_raw.as_slice());
        }
        let logical_rows = LogicalRows::Identical { size: 1000 };

        let ft = FieldTypeTp::LongLong.into();
        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.ensure_decoded(
                test::black_box(&mut ctx),
                test::black_box(&ft),
                logical_rows,
            )
            .unwrap();
            test::black_box(&col);
        });
    }

    /// Bench performance of decoding a decoded lazy batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode_decoded(b: &mut test::Bencher) {
        use crate::{
            codec::datum::{Datum, DatumEncoder},
            FieldTypeTp,
        };

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut ctx = EvalContext::default();
        let mut datum_raw: Vec<u8> = Vec::new();
        datum_raw
            .write_datum(&mut ctx, &[Datum::U64(0xDEADBEEF)], true)
            .unwrap();

        for _ in 0..1000 {
            column.mut_raw().push(datum_raw.as_slice());
        }
        let logical_rows = LogicalRows::Identical { size: 1000 };

        let ft = FieldTypeTp::LongLong.into();

        column.ensure_decoded(&mut ctx, &ft, logical_rows).unwrap();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.ensure_decoded(
                test::black_box(&mut ctx),
                test::black_box(&ft),
                logical_rows,
            )
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
