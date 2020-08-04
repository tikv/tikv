// TODO: This value is chosen based on MonetDB/X100's research without our own benchmarks.
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
