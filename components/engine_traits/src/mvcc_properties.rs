// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use txn_types::TimeStamp;

use crate::TtlProperties;

#[derive(Clone, Debug)]
pub struct MvccProperties {
<<<<<<< HEAD
    pub min_ts: TimeStamp,     // The minimal timestamp.
    pub max_ts: TimeStamp,     // The maximal timestamp.
    pub num_rows: u64,         // The number of rows.
    pub num_puts: u64,         // The number of MVCC puts of all rows.
    pub num_deletes: u64,      // The number of MVCC deletes of all rows.
    pub num_versions: u64,     // The number of MVCC versions of all rows.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single row.
    pub ttl: TtlProperties,    // The ttl properties of all rows, for RawKV only.
=======
    pub min_ts: TimeStamp,           // The minimal timestamp.
    pub max_ts: TimeStamp,           // The maximal timestamp.
    pub num_rows: u64,               // The number of rows.
    pub num_puts: u64,               // The number of MVCC puts of all rows.
    pub num_deletes: u64,            // The number of MVCC deletes of all rows.
    pub num_versions: u64,           // The number of MVCC versions of all rows.
    pub num_stale_deletes: u64,      // Delete versions after the latest row version.
    pub num_default_puts: u64,       // Puts whose value is stored in default CF.
    pub num_stale_default_puts: u64, // Stale puts whose value is stored in default CF.
    pub max_row_versions: u64,       // The maximal number of MVCC versions of a single row.
    pub ttl: TtlProperties,          // The ttl properties of all rows, for RawKV only.
    // Statistics for estimating number of discardable MVCC stale versions.
    // Stale versions are redundant versions of rows that were updated (not deleted).
    // These can be physically removed once past GC safe point.
    pub oldest_stale_version_ts: TimeStamp,
    pub newest_stale_version_ts: TimeStamp,

    // Statistics for estimating number of discardable TiKV MVCC deletes.
    // Delete versions represent rows that were deleted and can be physically
    // removed once past GC safe point.
    pub oldest_delete_ts: TimeStamp,
    pub newest_delete_ts: TimeStamp,
>>>>>>> 51b411a728 (gc_worker, raftstore: prioritize large unsplittable Regions for auto-compaction (#20051))
}

impl MvccProperties {
    pub fn new() -> MvccProperties {
        MvccProperties {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            num_rows: 0,
            num_puts: 0,
            num_deletes: 0,
            num_versions: 0,
            num_stale_deletes: 0,
            num_default_puts: 0,
            num_stale_default_puts: 0,
            max_row_versions: 0,
            ttl: TtlProperties::default(),
        }
    }

    pub fn add(&mut self, other: &MvccProperties) {
        self.min_ts = cmp::min(self.min_ts, other.min_ts);
        self.max_ts = cmp::max(self.max_ts, other.max_ts);
        self.num_rows += other.num_rows;
        self.num_puts += other.num_puts;
        self.num_deletes += other.num_deletes;
        self.num_versions += other.num_versions;
        self.num_stale_deletes += other.num_stale_deletes;
        self.num_default_puts += other.num_default_puts;
        self.num_stale_default_puts += other.num_stale_default_puts;
        self.max_row_versions = cmp::max(self.max_row_versions, other.max_row_versions);
        self.ttl.merge(&other.ttl);
    }
}

impl Default for MvccProperties {
    fn default() -> Self {
        Self::new()
    }
}

pub trait MvccPropertiesExt {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties>;
}
