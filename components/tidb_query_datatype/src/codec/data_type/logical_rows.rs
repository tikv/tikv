// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: This value is chosen based on MonetDB/X100's research without our own
// benchmarks.
pub const BATCH_MAX_SIZE: usize = 1024;

/// Identical logical row is a special case in expression evaluation that
/// the rows in physical_value are continuous and in order.
pub static IDENTICAL_LOGICAL_ROWS: [usize; BATCH_MAX_SIZE] = {
    let mut logical_rows = [0; BATCH_MAX_SIZE];
    let mut row = 0;
    while row < logical_rows.len() {
        logical_rows[row] = row;
        row += 1;
    }
    logical_rows
};

/// LogicalRows is a replacement for `logical_rows` parameter
/// in many of the copr functions. By distinguishing identical
/// and non-identical mapping with a enum, we can directly
/// tell if a `logical_rows` contains all items in a vector,
/// and we may optimiaze many cases by using direct copy and
/// construction.
///
/// Note that `Identical` supports no more than `BATCH_MAX_SIZE`
/// rows. In this way, it is always recommended to use `get_idx`
/// instead of `as_slice` to avoid runtime error.
#[derive(Clone, Copy, Debug)]
pub enum LogicalRows<'a> {
    Identical { size: usize },
    Ref { logical_rows: &'a [usize] },
}

impl<'a> LogicalRows<'a> {
    pub fn new_ident(size: usize) -> Self {
        Self::Identical { size }
    }

    pub fn from_slice(logical_rows: &'a [usize]) -> Self {
        Self::Ref { logical_rows }
    }

    /// Convert `LogicalRows` into legacy `&[usize]`.
    /// This function should only be called if you are sure
    /// that identical `logical_rows` doesn't exceed `BATCH_MAX_SIZE`.
    /// This function will be phased out after all `logical_rows`
    /// are refactored to use the new form.
    pub fn as_slice(self) -> &'a [usize] {
        match self {
            LogicalRows::Identical { size } => {
                if size >= BATCH_MAX_SIZE {
                    panic!("construct identical logical_rows larger than batch size")
                }
                &IDENTICAL_LOGICAL_ROWS[0..size]
            }
            LogicalRows::Ref { logical_rows } => logical_rows,
        }
    }

    #[inline]
    pub fn get_idx(&self, idx: usize) -> usize {
        match self {
            LogicalRows::Identical { size: _ } => idx,
            LogicalRows::Ref { logical_rows } => logical_rows[idx],
        }
    }

    pub fn is_ident(&self) -> bool {
        match self {
            LogicalRows::Identical { size: _ } => true,
            LogicalRows::Ref { logical_rows: _ } => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            LogicalRows::Identical { size } => *size,
            LogicalRows::Ref { logical_rows } => logical_rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct LogicalRowsIterator<'a> {
    logical_rows: LogicalRows<'a>,
    idx: usize,
}

impl<'a> Iterator for LogicalRowsIterator<'a> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.idx < self.logical_rows.len() {
            Some(self.logical_rows.get_idx(self.idx))
        } else {
            None
        };

        self.idx += 1;

        result
    }
}

impl<'a> IntoIterator for LogicalRows<'a> {
    type Item = usize;
    type IntoIter = LogicalRowsIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LogicalRowsIterator {
            logical_rows: self,
            idx: 0,
        }
    }
}
