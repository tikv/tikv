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

impl<E> BatchRows<E> {
    /// Creates a new `BatchRows`, which contains `columns_count` number of raw columns
    /// and each of them reserves capacity according to `rows_capacity`.
    pub fn raw(columns_count: usize, rows_capacity: usize) -> Self {
        let mut columns = Vec::with_capacity(columns_count);
        for _ in 0..columns_count {
            let column = LazyBatchColumn::Raw(Vec::with_capacity(rows_capacity));
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
    /// Returns `Error::UnsupportedType` if given column type is not supported.
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

enum LazyBatchColumn {
    /// Ensure that for small datum values (i.e. Int, Real, Time) they are stored compactly.
    Raw(Vec<SmallVec<[u8; 10]>>),
    Decoded(::BatchColumn),
}

impl LazyBatchColumn {
    pub fn is_raw(&self) -> bool {
        match self {
            LazyBatchColumn::Raw(_) => true,
            LazyBatchColumn::Decoded(_) => false,
        }
    }

    pub fn is_decoded(&self) -> bool {
        match self {
            LazyBatchColumn::Raw(_) => false,
            LazyBatchColumn::Decoded(_) => true,
        }
    }

    pub fn get_decoded(&self) -> &::BatchColumn {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(ref v) => v,
        }
    }

    pub fn get_raw(&self) -> &Vec<SmallVec<[u8; 10]>> {
        match self {
            LazyBatchColumn::Raw(ref v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(ref v) => v.len(),
            LazyBatchColumn::Decoded(ref v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decodes this column according to column info if the column is not decoded.
    ///
    /// # Errors
    ///
    /// Returns `Error::UnsupportedType` if column type is not supported.
    ///
    /// Returns `Error::InvalidData` if raw data cannot be decoded by given column type.
    pub fn decode(&mut self, column_info: &ColumnInfo) -> ::Result<()> {
        if self.is_decoded() {
            return Ok(());
        }

        // TODO: Can we eliminate an copy here?

        let mut decoded_column = ::BatchColumn::with_capacity(self.len(), column_info)?;
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
    pub fn push_raw(&mut self, raw_datum: &[u8]) {
        match self {
            LazyBatchColumn::Raw(ref mut v) => {
                v.push(SmallVec::from_slice(raw_datum));
            }
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }
}
