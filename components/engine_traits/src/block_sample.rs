// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

/// Options of [`crate::Snapshot::sample_mvcc_rows`].
#[derive(Clone, Debug)]
pub struct MvccBlockSampleOptions {
    /// One random data block in each run of `spacing` consecutive blocks of
    /// each SST file is read, so each block with probability `1 / spacing`.
    pub spacing: u64,
    pub seed: u64,
}

/// What [`crate::Snapshot::sample_mvcc_rows`] read and returned.
#[derive(Clone, Debug, Default)]
pub struct MvccBlockSampleStats {
    pub blocks_in_range: u64,
    pub blocks_selected: u64,
    pub memtable_entries: u64,
    /// Versions that could be the visible version of their row.
    pub candidates: u64,
    /// Visible rows returned.
    pub rows: u64,
    /// Visible deletes skipped.
    pub deleted: u64,
    /// Candidates shadowed by a newer version.
    pub shadowed: u64,
    /// Blocks read from files (block cache misses) while sampling.
    pub block_reads: u64,
}
