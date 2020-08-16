// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: This value is chosen based on MonetDB/X100's research without our own benchmarks.
pub const BATCH_MAX_SIZE: usize = 1024;

/// Identical logical row is a special case in expression evaluation that
/// the rows in physical_value are continuous and in order.
static IDENTICAL_LOGICAL_ROWS: [usize; BATCH_MAX_SIZE] = {
    let mut logical_rows = [0; BATCH_MAX_SIZE];
    let mut row = 0;
    while row < logical_rows.len() {
        logical_rows[row] = row;
        row += 1;
    }
    logical_rows
};

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

    pub fn as_slice(self) -> &'a [usize] {
        match self {
            LogicalRows::Identical { size } => &IDENTICAL_LOGICAL_ROWS[0..size],
            LogicalRows::Ref { logical_rows } => logical_rows,
        }
    }

    #[inline]
    pub fn get_idx(self, idx: usize) -> usize {
        match self {
            LogicalRows::Identical { size: _ } => idx,
            LogicalRows::Ref { logical_rows } => logical_rows[idx],
        }
    }

    pub fn is_ident(self) -> bool {
        match self {
            LogicalRows::Identical { size: _ } => true,
            LogicalRows::Ref { logical_rows: _ } => false,
        }
    }
}
