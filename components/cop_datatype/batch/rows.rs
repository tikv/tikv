// Copyright 2018 PingCAP, Inc.
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

use smallvec::SmallVec;

use tipb::schema::ColumnInfo;

pub struct BatchRows<E> {
    /// Multiple interested columns. Each column is either decoded, or not decoded.
    ///
    /// For decoded columns, they may be in different types. If the column is in
    /// type `LazyBatchColumn::Encoded`, it means that it is not decoded.
    columns: Vec<LazyBatchColumn>,

    /// The number of rows (either encoded or decoded) stored.
    number_of_rows: usize,

    /// The optional error occurred during batch execution. Once there is an error, this field
    /// is set and no more rows should be fetched or executed.
    some_error: Option<E>,
}

impl<E: Clone> Clone for BatchRows<E> {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            number_of_rows: self.number_of_rows,
            some_error: self.some_error.clone(),
        }
    }
}

impl<E> BatchRows<E> {
    /// Creates a new `BatchRows`, which contains `columns_count` number of raw columns
    /// and each of them reserves capacity according to `rows_capacity`.
    pub fn raw(columns_count: usize, rows_capacity: usize) -> Self {
        let mut columns = Vec::with_capacity(columns_count);
        for _ in 0..columns_count {
            let column = LazyBatchColumn::raw_with_capacity(rows_capacity);
            columns.push(column);
        }
        Self {
            columns,
            number_of_rows: 0,
            some_error: None,
        }
    }

    /// Pushes each raw datum into columns at corresponding index and increase the row
    /// count.
    ///
    /// # Panics
    ///
    /// It is caller's duty to ensure that these columns are not decoded and the number of datums
    /// matches the number of columns. Otherwise there will be panics.
    pub fn push_raw_row(&mut self, raw_row: &[&[u8]]) {
        assert_eq!(self.cols_len(), raw_row.len());
        for (col_index, raw_datum) in raw_row.iter().enumerate() {
            let lazy_col = &mut self.columns[col_index];
            assert!(lazy_col.is_raw());
            lazy_col.push_raw(raw_datum);
        }
    }

    /// Ensures that a column at specified `column_index` is decoded and returns a reference
    /// to the decoded column.
    ///
    /// If the column is already decoded, this function does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidData` if raw data cannot be decoded by given column type.
    pub fn ensure_column_decoded(
        &mut self,
        column_index: usize,
        column_info: &ColumnInfo,
    ) -> ::Result<&::BatchColumn> {
        let col = &mut self.columns[column_index];
        assert_eq!(col.len(), self.number_of_rows);
        col.decode(column_info)?;
        Ok(col.get_decoded())
    }

    /// Returns number of rows.
    #[inline]
    pub fn rows_len(&self) -> usize {
        self.number_of_rows
    }

    /// Returns number of columns. It might be possible that there is no row but multiple columns.
    #[inline]
    pub fn cols_len(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Clone)]
enum LazyBatchColumn {
    /// Ensure that small datum values (i.e. Int, Real, Time) are stored compactly.
    /// Notice that there is an extra 1 byte for datum to store the flag, so there are 9 bytes.
    Raw(Vec<SmallVec<[u8; 9]>>),
    Decoded(::BatchColumn),
}

impl LazyBatchColumn {
    /// Creates a new `LazyBatchColumn::Raw` with specified capacity.
    #[inline]
    pub fn raw_with_capacity(capacity: usize) -> Self {
        LazyBatchColumn::Raw(Vec::with_capacity(capacity))
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
    pub fn get_decoded(&self) -> &::BatchColumn {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(ref v) => v,
        }
    }

    #[inline]
    pub fn get_raw(&self) -> &Vec<SmallVec<[u8; 9]>> {
        match self {
            LazyBatchColumn::Raw(ref v) => v,
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

    /// Decodes this column according to column info if the column is not decoded.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidData` if raw data cannot be decoded by given column type.
    pub fn decode(&mut self, column_info: &ColumnInfo) -> ::Result<()> {
        if self.is_decoded() {
            return Ok(());
        }

        // TODO: Can we eliminate an copy here?

        let mut decoded_column = ::BatchColumn::with_capacity(self.len(), column_info);
        {
            let raw_values = self.get_raw();
            for raw_value in raw_values {
                if raw_value.is_empty() {
                    decoded_column.push_datum(column_info.get_default_val())?;
                } else {
                    decoded_column.push_datum(raw_value.as_slice())?;
                }
            }
        }
        *self = LazyBatchColumn::Decoded(decoded_column);

        Ok(())
    }

    /// Push a raw datum (not yet decoded).
    ///
    /// `datum.len()` can be 0, indicating a missing value for corresponding cell.
    ///
    /// # Panics
    ///
    /// Panics when current column is already decoded.
    #[inline]
    pub fn push_raw(&mut self, raw_datum: &[u8]) {
        match self {
            LazyBatchColumn::Raw(ref mut v) => {
                v.push(SmallVec::from_slice(raw_datum));
            }
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }
}

#[cfg(test)]
mod benches {
    use test;

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

    /// 10 bytes > inline size for LazyBatchColumn, which will be slower. This benchmark
    /// shows how slow it is.
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

    #[bench]
    fn bench_lazy_batch_column_clone(b: &mut test::Bencher) {
        use tikv::coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();
        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }
        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    #[bench]
    fn bench_lazy_batch_column_decode(b: &mut test::Bencher) {
        use num_traits::ToPrimitive;
        use tikv::coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();
        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }
        let mut col_info = ::tipb::schema::ColumnInfo::new();
        col_info.set_tp(::FieldTypeTp::LongLong.to_i32().unwrap());

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.decode(&col_info).unwrap();
            test::black_box(&col);
        });
    }

    /// The performance of naively decoding multiple datums.
    #[bench]
    fn bench_batch_decode(b: &mut test::Bencher) {
        use tikv::coprocessor::codec::datum::{self, Datum, DatumEncoder};

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        b.iter(|| {
            for _ in 0..1000 {
                let mut raw = test::black_box(&datum_raw).as_slice();
                let datum = datum::decode_datum(&mut raw).unwrap();
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
}
