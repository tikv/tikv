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

// TODO: Remove this.
#![allow(dead_code)]

use std::convert::TryFrom;

use smallvec::SmallVec;

use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeFlag};
use tipb::schema::ColumnInfo;

use super::BatchColumn;
use coprocessor::codec::mysql::Tz;
use coprocessor::codec::{datum, Error, Result};

pub struct BatchRows<E> {
    /// Multiple interested columns. Each column is either decoded, or not decoded.
    ///
    /// For decoded columns, they may be in different types. If the column is in
    /// type `LazyBatchColumn::Encoded`, it means that it is not decoded.
    columns: Vec<LazyBatchColumn>,

    /// The optional error occurred during batch execution. Once there is an error, this field
    /// is set and no more rows should be fetched or executed.
    some_error: Option<E>,
}

impl<E: Clone> Clone for BatchRows<E> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
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
            some_error: None,
        }
    }

    /// Pushes each raw datum into columns at corresponding index.
    ///
    /// If datum is missing, the corresponding element should be an empty slice.
    ///
    /// # Panics
    ///
    /// It is caller's duty to ensure that these columns are not decoded and the number of datums
    /// matches the number of columns. Otherwise there will be panics.
    #[inline]
    pub fn push_raw_row<D, V>(&mut self, raw_row: V)
    where
        D: AsRef<[u8]>,
        V: AsRef<[D]>,
    {
        let raw_row_slice = raw_row.as_ref();
        assert_eq!(self.columns_len(), raw_row_slice.len());
        for (col_index, raw_datum) in raw_row_slice.iter().enumerate() {
            let lazy_col = &mut self.columns[col_index];
            assert!(lazy_col.is_raw());
            lazy_col.push_raw(raw_datum);
        }
    }

    /// Ensures that a column at specified `column_index` is decoded and returns a reference
    /// to the decoded column.
    ///
    /// If the column is already decoded, this function does nothing.
    #[inline]
    pub fn ensure_column_decoded(
        &mut self,
        column_index: usize,
        time_zone: Tz,
        column_info: &ColumnInfo,
    ) -> Result<&BatchColumn> {
        let number_of_rows = self.rows_len();
        let col = &mut self.columns[column_index];
        assert_eq!(col.len(), number_of_rows);
        col.decode(time_zone, column_info)?;
        Ok(col.get_decoded())
    }

    /// Returns whether column at specified `column_index` is decoded.
    #[inline]
    pub fn is_column_decoded(&self, column_index: usize) -> bool {
        self.columns[column_index].is_decoded()
    }

    /// Returns the number of columns.
    ///
    /// It might be possible that there is no row but multiple columns.
    #[inline]
    pub fn columns_len(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of rows.
    #[inline]
    pub fn rows_len(&self) -> usize {
        if self.columns.is_empty() {
            return 0;
        }
        self.columns[0].len()
    }

    /// Returns the number of rows this container can hold without reallocating.
    #[inline]
    pub fn rows_capacity(&self) -> usize {
        if self.columns.is_empty() {
            return 0;
        }
        self.columns[0].capacity()
    }

    /// Retains only the rows specified by the predicate, which accepts index only.
    ///
    /// In other words, remove all rows such that `f(row_index)` returns `false`.
    #[inline]
    pub fn retain_by_index<F>(&mut self, mut f: F)
    where
        F: FnMut(usize) -> bool,
    {
        if self.rows_len() == 0 {
            return;
        }

        let current_rows_len = self.rows_len();
        let mut new_rows = None;

        // We retain column by column to be efficient.
        for col in &mut self.columns {
            assert_eq!(col.len(), current_rows_len);
            col.retain_by_index(&mut f);

            match new_rows {
                None => {
                    new_rows = Some(col.len());
                }
                Some(rows) => {
                    assert_eq!(col.len(), rows);
                }
            }
        }
    }
}

enum LazyBatchColumn {
    /// Ensure that small datum values (i.e. Int, Real, Time) are stored compactly.
    /// Notice that there is an extra 1 byte for datum to store the flag, so there are 9 bytes.
    Raw(Vec<SmallVec<[u8; 9]>>),
    Decoded(BatchColumn),
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
    pub fn get_decoded(&self) -> &BatchColumn {
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

    /// Decodes this column according to column info if the column is not decoded.
    pub fn decode(&mut self, time_zone: Tz, column_info: &ColumnInfo) -> Result<()> {
        if self.is_decoded() {
            return Ok(());
        }

        let eval_type =
            EvalType::try_from(column_info.tp()).map_err(|e| Error::Other(box_err!(e)))?;

        let mut decoded_column = BatchColumn::with_capacity(self.capacity(), eval_type);
        {
            let raw_values = self.get_raw();
            for raw_value in raw_values {
                let raw_datum = if raw_value.is_empty() {
                    if column_info.has_default_val() {
                        column_info.get_default_val()
                    } else if !column_info.flag().contains(FieldTypeFlag::NOT_NULL) {
                        datum::DATUM_DATA_NULL
                    } else {
                        return Err(box_err!(
                            "Column (id = {}) has flag NOT NULL, but no value is given",
                            column_info.get_column_id()
                        ));
                    }
                } else {
                    raw_value.as_slice()
                };
                decoded_column.push_datum(raw_datum, time_zone, column_info)?;
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
    #[inline]
    pub fn push_raw(&mut self, raw_datum: impl AsRef<[u8]>) {
        match self {
            LazyBatchColumn::Raw(ref mut v) => {
                v.push(SmallVec::from_slice(raw_datum.as_ref()));
            }
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use coprocessor::codec::datum::{Datum, DatumEncoder};

    /// Helper method to generate raw row ([u8] vector) from datum vector.
    fn raw_row_from_datums(datums: impl AsRef<[Option<Datum>]>, comparable: bool) -> Vec<Vec<u8>> {
        datums
            .as_ref()
            .into_iter()
            .map(|some_datum| {
                let mut ret = Vec::new();
                if some_datum.is_some() {
                    DatumEncoder::encode(
                        &mut ret,
                        &[some_datum.clone().take().unwrap()],
                        comparable,
                    ).unwrap();
                }
                ret
            })
            .collect()
    }

    #[test]
    fn test_lazy_batch_column_clone() {
        use cop_datatype::FieldTypeTp;

        let mut col_info = ColumnInfo::new();
        col_info.as_mut_accessor().set_tp(FieldTypeTp::Long);

        let mut col = LazyBatchColumn::raw_with_capacity(5);
        assert!(col.is_raw());
        assert_eq!(col.len(), 0);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.get_raw().len(), 0);
        {
            // Clone empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.get_raw().len(), 0);
        }
        {
            // Empty raw to empty decoded.
            let mut col = col.clone();
            col.decode(Tz::utc(), &col_info).unwrap();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.get_decoded().as_int_slice(), &[]);
            {
                // Clone empty decoded LazyBatchColumn.
                let col = col.clone();
                assert!(col.is_decoded());
                assert_eq!(col.len(), 0);
                assert_eq!(col.capacity(), 5);
                assert_eq!(col.get_decoded().as_int_slice(), &[]);
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
        assert_eq!(col.get_raw().len(), 2);
        assert_eq!(col.get_raw()[0].as_slice(), datum_raw_1.as_slice());
        assert_eq!(col.get_raw()[1].as_slice(), datum_raw_2.as_slice());
        {
            // Clone non-empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 2);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.get_raw().len(), 2);
            assert_eq!(col.get_raw()[0].as_slice(), datum_raw_1.as_slice());
            assert_eq!(col.get_raw()[1].as_slice(), datum_raw_2.as_slice());
        }
        // Non-empty raw to non-empty decoded.
        col.decode(Tz::utc(), &col_info).unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 2);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.get_decoded().as_int_slice(), &[Some(32), Some(7)]);
        {
            // Clone non-empty decoded LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 2);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.get_decoded().as_int_slice(), &[Some(32), Some(7)]);
        }
    }

    #[test]
    fn test_ensure_column_decoded() {
        use cop_datatype::FieldTypeTp;

        for comparable in &[true, false] {
            let schema = [
                {
                    // Column 1: Long, Default value 5
                    let mut default_value = Vec::new();
                    DatumEncoder::encode(&mut default_value, &[Datum::U64(5)], *comparable)
                        .unwrap();

                    let mut col_info = ColumnInfo::new();
                    col_info.as_mut_accessor().set_tp(FieldTypeTp::Long);
                    col_info.set_default_val(default_value);
                    col_info
                },
                {
                    // Column 2: Double, Default value NULL
                    let mut col_info = ColumnInfo::new();
                    col_info.as_mut_accessor().set_tp(FieldTypeTp::Double);
                    col_info
                },
                {
                    // Column 3: VarChar, Default value NULL
                    let mut col_info = ColumnInfo::new();
                    col_info.as_mut_accessor().set_tp(FieldTypeTp::VarChar);
                    col_info
                },
            ];

            let values = vec![
                vec![Some(Datum::U64(1)), Some(Datum::F64(1.0)), None],
                // None in Column 0 should be decoded to its default value 5.
                vec![None, Some(Datum::Null), Some(Datum::Bytes(vec![0u8, 2u8]))],
                // Null in Column 0 should be decoded to NULL.
                vec![Some(Datum::Null), Some(Datum::F64(3.0)), None],
                // None in Column 1 should be decoded to NULL because of no default value.
                vec![Some(Datum::U64(4)), None, Some(Datum::Null)],
            ];

            // Empty BatchRows
            let mut rows = BatchRows::<()>::raw(3, 1);
            assert_eq!(rows.rows_len(), 0);
            assert_eq!(rows.columns_len(), 3);

            for raw_row in values.iter().map(|r| raw_row_from_datums(r, *comparable)) {
                rows.push_raw_row(raw_row.iter());
            }
            assert_eq!(rows.rows_len(), values.len());

            // Decode Column Index 2
            assert!(!rows.is_column_decoded(2));
            {
                let col = rows
                    .ensure_column_decoded(2, Tz::utc(), &schema[2])
                    .unwrap();
                assert_eq!(col.len(), 4);
                assert_eq!(col.eval_type(), EvalType::Bytes);
                assert_eq!(
                    col.as_bytes_slice(),
                    &[None, Some(vec![0u8, 2u8]), None, None]
                );
            }
            // Decode a decoded column
            assert!(rows.is_column_decoded(2));
            {
                let col = rows
                    .ensure_column_decoded(2, Tz::utc(), &schema[2])
                    .unwrap();
                assert_eq!(col.len(), 4);
                assert_eq!(col.eval_type(), EvalType::Bytes);
                assert_eq!(
                    col.as_bytes_slice(),
                    &[None, Some(vec![0u8, 2u8]), None, None]
                );
            }
            assert!(rows.is_column_decoded(2));

            // Decode Column Index 0
            assert!(!rows.is_column_decoded(0));
            {
                let col = rows
                    .ensure_column_decoded(0, Tz::utc(), &schema[0])
                    .unwrap();
                assert_eq!(col.len(), 4);
                assert_eq!(col.eval_type(), EvalType::Int);
                assert_eq!(col.as_int_slice(), &[Some(1), Some(5), None, Some(4)]);
            }
            assert!(rows.is_column_decoded(0));

            // Decode Column Index 1
            assert!(!rows.is_column_decoded(1));
            {
                let col = rows
                    .ensure_column_decoded(1, Tz::utc(), &schema[1])
                    .unwrap();
                assert_eq!(col.len(), 4);
                assert_eq!(col.eval_type(), EvalType::Real);
                assert_eq!(col.as_real_slice(), &[Some(1.0), None, Some(3.0), None]);
            }
            assert!(rows.is_column_decoded(1));
        }
    }

    #[test]
    fn test_retain_by_index() {
        use cop_datatype::FieldTypeTp;

        let schema = [
            {
                // Column 1: Long, Default value 5
                let mut default_value = Vec::new();
                DatumEncoder::encode(&mut default_value, &[Datum::U64(5)], true).unwrap();

                let mut col_info = ColumnInfo::new();
                col_info.as_mut_accessor().set_tp(FieldTypeTp::Long);
                col_info.set_default_val(default_value);
                col_info
            },
            {
                // Column 2: Double
                let mut col_info = ColumnInfo::new();
                col_info.as_mut_accessor().set_tp(FieldTypeTp::Double);
                col_info
            },
        ];

        let mut rows = BatchRows::<()>::raw(2, 3);
        assert_eq!(rows.rows_len(), 0);
        assert_eq!(rows.columns_len(), 2);
        assert_eq!(rows.rows_capacity(), 3);
        rows.retain_by_index(|_| true);
        assert_eq!(rows.rows_len(), 0);
        assert_eq!(rows.columns_len(), 2);
        assert_eq!(rows.rows_capacity(), 3);
        rows.retain_by_index(|_| false);
        assert_eq!(rows.rows_len(), 0);
        assert_eq!(rows.columns_len(), 2);
        assert_eq!(rows.rows_capacity(), 3);

        rows.push_raw_row(raw_row_from_datums(&[None, Some(Datum::F64(1.3))], false));
        rows.push_raw_row(raw_row_from_datums(&[None, None], false));
        rows.push_raw_row(raw_row_from_datums(&[Some(Datum::U64(3)), None], true));
        rows.push_raw_row(raw_row_from_datums(
            &[Some(Datum::U64(3)), Some(Datum::F64(5.0))],
            false,
        ));
        rows.push_raw_row(raw_row_from_datums(
            &[Some(Datum::U64(11)), Some(Datum::F64(7.5))],
            true,
        ));
        rows.push_raw_row(raw_row_from_datums(&[None, Some(Datum::F64(13.1))], true));

        let retain_map = &[true, true, false, false, true, false];
        rows.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(rows.rows_len(), 3);
        assert_eq!(rows.columns_len(), 2);
        assert!(rows.rows_capacity() > 3);
        {
            // Clone `rows` because `ensure_column_decoded` will mutate lazy column from raw bytes
            // to decoded permanently.
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 3);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(column0.as_int_slice(), &[Some(5), Some(5), Some(11)]);
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 3);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(column1.as_real_slice(), &[Some(1.3), None, Some(7.5)]);
        }

        rows.push_raw_row(raw_row_from_datums(
            &[None, Some(Datum::F64(101.51))],
            false,
        ));
        rows.push_raw_row(raw_row_from_datums(&[Some(Datum::U64(1)), None], false));
        rows.push_raw_row(raw_row_from_datums(
            &[Some(Datum::U64(5)), Some(Datum::F64(1.9))],
            true,
        ));
        rows.push_raw_row(raw_row_from_datums(
            &[None, Some(Datum::F64(101.51))],
            false,
        ));

        assert_eq!(rows.rows_len(), 7);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 7);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(
                column0.as_int_slice(),
                &[
                    Some(5),
                    Some(5),
                    Some(11),
                    Some(5),
                    Some(1),
                    Some(5),
                    Some(5)
                ]
            );
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 7);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(
                column1.as_real_slice(),
                &[
                    Some(1.3),
                    None,
                    Some(7.5),
                    Some(101.51),
                    None,
                    Some(1.9),
                    Some(101.51)
                ]
            );
        }

        let retain_map = &[true, false, true, false, false, true, true];
        rows.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(rows.rows_len(), 4);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 4);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(
                column0.as_int_slice(),
                &[Some(5), Some(11), Some(5), Some(5)]
            );
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 4);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(
                column1.as_real_slice(),
                &[Some(1.3), Some(7.5), Some(1.9), Some(101.51)]
            );
        }

        rows.retain_by_index(|_| true);

        assert_eq!(rows.rows_len(), 4);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 4);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(
                column0.as_int_slice(),
                &[Some(5), Some(11), Some(5), Some(5)]
            );
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 4);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(
                column1.as_real_slice(),
                &[Some(1.3), Some(7.5), Some(1.9), Some(101.51)]
            );
        }

        rows.retain_by_index(|_| false);

        assert_eq!(rows.rows_len(), 0);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 0);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(column0.as_int_slice(), &[]);
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 0);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(column1.as_real_slice(), &[]);
        }

        rows.push_raw_row(raw_row_from_datums(&[None, Some(Datum::F64(7.77))], true));
        rows.push_raw_row(raw_row_from_datums(&[Some(Datum::U64(5)), None], false));
        rows.push_raw_row(raw_row_from_datums(
            &[Some(Datum::U64(1)), Some(Datum::F64(7.17))],
            false,
        ));

        assert_eq!(rows.rows_len(), 3);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 3);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(column0.as_int_slice(), &[Some(5), Some(5), Some(1)]);
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 3);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(column1.as_real_slice(), &[Some(7.77), None, Some(7.17)]);
        }

        // Let's change a column from lazy to decoded and test whether retain works
        rows.ensure_column_decoded(0, Tz::utc(), &schema[0])
            .unwrap();

        rows.retain_by_index(|_| true);

        assert_eq!(rows.rows_len(), 3);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 3);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(column0.as_int_slice(), &[Some(5), Some(5), Some(1)]);
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 3);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(column1.as_real_slice(), &[Some(7.77), None, Some(7.17)]);
        }

        let retain_map = &[true, false, true];
        rows.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(rows.rows_len(), 2);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 2);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(column0.as_int_slice(), &[Some(5), Some(1)]);
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 2);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(column1.as_real_slice(), &[Some(7.77), Some(7.17)]);
        }

        rows.retain_by_index(|_| false);

        assert_eq!(rows.rows_len(), 0);
        assert_eq!(rows.columns_len(), 2);
        {
            let mut rows = rows.clone();
            let column0 = rows
                .ensure_column_decoded(0, Tz::utc(), &schema[0])
                .unwrap();
            assert_eq!(column0.len(), 0);
            assert_eq!(column0.eval_type(), EvalType::Int);
            assert_eq!(column0.as_int_slice(), &[]);
        }
        {
            let mut rows = rows.clone();
            let column1 = rows
                .ensure_column_decoded(1, Tz::utc(), &schema[1])
                .unwrap();
            assert_eq!(column1.len(), 0);
            assert_eq!(column1.eval_type(), EvalType::Real);
            assert_eq!(column1.as_real_slice(), &[]);
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
        use cop_datatype::FieldTypeTp;
        use coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }

        let col_info = {
            let mut col_info = ::tipb::schema::ColumnInfo::new();
            col_info.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            col_info
        };
        let tz = Tz::utc();

        column.decode(tz, &col_info).unwrap();

        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    /// Bench performance of decoding a raw batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode(b: &mut test::Bencher) {
        use cop_datatype::FieldTypeTp;
        use coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }

        let col_info = {
            let mut col_info = ::tipb::schema::ColumnInfo::new();
            col_info.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            col_info
        };
        let tz = Tz::utc();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.decode(test::black_box(tz), test::black_box(&col_info))
                .unwrap();
            test::black_box(&col);
        });
    }

    /// Bench performance of decoding a decoded lazy batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode_decoded(b: &mut test::Bencher) {
        use cop_datatype::FieldTypeTp;
        use coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.push_raw(datum_raw.as_slice());
        }

        let col_info = {
            let mut col_info = ::tipb::schema::ColumnInfo::new();
            col_info.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            col_info
        };
        let tz = Tz::utc();

        column.decode(tz, &col_info).unwrap();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.decode(test::black_box(tz), test::black_box(&col_info))
                .unwrap();
            test::black_box(&col);
        });
    }

    /// A vector based LazyBatchColumn
    #[derive(Clone)]
    enum VectorLazyBatchColumn {
        Raw(Vec<Vec<u8>>),
        Decoded(BatchColumn),
    }

    impl VectorLazyBatchColumn {
        #[inline]
        pub fn raw_with_capacity(capacity: usize) -> Self {
            VectorLazyBatchColumn::Raw(Vec::with_capacity(capacity))
        }

        #[inline]
        pub fn clear(&mut self) {
            match self {
                VectorLazyBatchColumn::Raw(ref mut v) => v.clear(),
                VectorLazyBatchColumn::Decoded(ref mut v) => v.clear(),
            }
        }

        #[inline]
        pub fn push_raw(&mut self, raw_datum: &[u8]) {
            match self {
                VectorLazyBatchColumn::Raw(ref mut v) => v.push(raw_datum.to_vec()),
                VectorLazyBatchColumn::Decoded(_) => panic!(),
            }
        }
    }

    /// Bench performance of push 10 bytes to a vector based LazyBatchColumn.
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
