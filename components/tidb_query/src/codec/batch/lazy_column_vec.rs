// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::FieldType;

use super::LazyBatchColumn;
use crate::codec::chunk::{Chunk, ChunkEncoder};
use crate::codec::data_type::VectorValue;
use crate::codec::Result;
use crate::expr::EvalContext;

/// Stores multiple `LazyBatchColumn`s. Each column has an equal length.
#[derive(Clone, Debug)]
pub struct LazyBatchColumnVec {
    /// Multiple lazy batch columns. Each column is either decoded, or not decoded.
    ///
    /// For decoded columns, they may be in different types. If the column is in
    /// type `LazyBatchColumn::Raw`, it means that it is not decoded.
    columns: Vec<LazyBatchColumn>,
}

impl From<Vec<LazyBatchColumn>> for LazyBatchColumnVec {
    #[inline]
    fn from(columns: Vec<LazyBatchColumn>) -> Self {
        LazyBatchColumnVec { columns }
    }
}

impl From<Vec<VectorValue>> for LazyBatchColumnVec {
    #[inline]
    fn from(columns: Vec<VectorValue>) -> Self {
        LazyBatchColumnVec {
            columns: columns
                .into_iter()
                .map(|v| LazyBatchColumn::from(v))
                .collect(),
        }
    }
}

impl LazyBatchColumnVec {
    /// Creates a new empty `LazyBatchColumnVec`, which does not have columns and rows.
    ///
    /// Because column numbers won't change, it means constructed instance will be always empty.
    #[inline]
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Creates a new empty `LazyBatchColumnVec` with the same number of columns and schema.
    #[inline]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .map(|c| c.clone_empty(capacity))
                .collect(),
        }
    }

    /// Creates a new `LazyBatchColumnVec`, which contains `columns_count` number of raw columns.
    #[cfg(test)]
    pub fn with_raw_columns(columns_count: usize) -> Self {
        let mut columns = Vec::with_capacity(columns_count);
        for _ in 0..columns_count {
            let column = LazyBatchColumn::raw_with_capacity(0);
            columns.push(column);
        }
        Self { columns }
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

    /// Asserts that all columns have equal length.
    #[inline]
    pub fn assert_columns_equal_length(&self) {
        let len = self.rows_len();
        for column in &self.columns {
            assert_eq!(len, column.len());
        }
    }

    /// Returns maximum encoded size.
    // TODO: Move to other place.
    pub fn maximum_encoded_size(
        &self,
        logical_rows: impl AsRef<[usize]>,
        output_offsets: impl AsRef<[u32]>,
    ) -> Result<usize> {
        let logical_rows = logical_rows.as_ref();
        let mut size = 0;
        for offset in output_offsets.as_ref() {
            size += self.columns[(*offset) as usize].maximum_encoded_size(logical_rows)?;
        }
        Ok(size)
    }

    /// Returns maximum encoded size in chunk format.
    // TODO: Move to other place.
    pub fn maximum_encoded_size_chunk(
        &self,
        logical_rows: impl AsRef<[usize]>,
        output_offsets: impl AsRef<[u32]>,
    ) -> Result<usize> {
        let logical_rows = logical_rows.as_ref();
        let mut size = 0;
        for offset in output_offsets.as_ref() {
            size += self.columns[(*offset) as usize].maximum_encoded_size_chunk(logical_rows)?;
        }
        Ok(size)
    }

    /// Encodes into binary format.
    // TODO: Move to other place.
    pub fn encode(
        &self,
        logical_rows: impl AsRef<[usize]>,
        output_offsets: impl AsRef<[u32]>,
        schema: impl AsRef<[FieldType]>,
        output: &mut Vec<u8>,
        ctx: &mut EvalContext,
    ) -> Result<()> {
        let schema = schema.as_ref();
        let output_offsets = output_offsets.as_ref();
        for idx in logical_rows.as_ref() {
            for offset in output_offsets {
                let offset = *offset as usize;
                let col = &self.columns[offset];
                col.encode(*idx, &schema[offset], ctx, output)?;
            }
        }
        Ok(())
    }

    /// Encode into chunk format.
    pub fn encode_chunk(
        &mut self,
        logical_rows: impl AsRef<[usize]>,
        output_offsets: impl AsRef<[u32]>,
        schema: impl AsRef<[FieldType]>,
        output: &mut Vec<u8>,
        ctx: &mut EvalContext,
    ) -> Result<()> {
        // Step 1 : Decode all data.
        let schema = schema.as_ref();
        let output_offsets = output_offsets.as_ref();
        for offset in output_offsets {
            let offset = *offset as usize;
            let col = &mut self.columns[offset];
            col.ensure_decoded(ctx, &schema[offset], logical_rows.as_ref())?;
        }

        // Step 2 : Make the chunk and append data.
        let mut fields: Vec<FieldType> = Vec::new();
        for offset in output_offsets {
            let offset = *offset as usize;
            let field_type = &schema[offset];
            fields.push(field_type.clone());
        }
        let mut chunk = Chunk::new(&fields, logical_rows.as_ref().len());

        for column_idx in 0..output_offsets.len() {
            let offset = output_offsets[column_idx] as usize;
            let col = &self.columns[offset];
            chunk.append_vec(
                logical_rows.as_ref(),
                &schema[offset],
                col.decoded(),
                column_idx,
            )?;
        }

        // Step 3 : Encode chunk to output.
        output.encode_chunk(&chunk).unwrap();
        Ok(())
    }

    /// Truncates columns into equal length. The new length of all columns would be the length of
    /// the shortest column before calling this function.
    pub fn truncate_into_equal_length(&mut self) {
        let mut min_len = self.rows_len();
        for col in &self.columns {
            min_len = min_len.min(col.len());
        }
        for col in &mut self.columns {
            col.truncate(min_len);
        }
        self.assert_columns_equal_length();
    }

    /// Returns the inner columns as a slice.
    pub fn as_slice(&self) -> &[LazyBatchColumn] {
        self.columns.as_slice()
    }

    /// Returns the inner columns as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [LazyBatchColumn] {
        self.columns.as_mut_slice()
    }
}

// Do not implement Deref, since we want to forbid some misleading function calls like
// `LazyBatchColumnVec.len()`.

impl std::ops::Index<usize> for LazyBatchColumnVec {
    type Output = LazyBatchColumn;

    fn index(&self, index: usize) -> &LazyBatchColumn {
        &self.columns[index]
    }
}

impl std::ops::IndexMut<usize> for LazyBatchColumnVec {
    fn index_mut(&mut self, index: usize) -> &mut LazyBatchColumn {
        &mut self.columns[index]
    }
}
