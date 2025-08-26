// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use txn_types::TimeStamp;

use crate::TtlProperties;

/// MVCC properties for estimating discardable versions based on GC safe point.
///
/// These statistics are collected to estimate the number of discardable MVCC
/// versions using uniform distribution assumption. There are two types of
///  discardable versions:
/// 1. Stale versions: Redundant versions from row updates that can be
///    physically removed
/// 2. Delete versions: Deleted rows that can be physically removed
///
/// Example scenario:
/// - Each row gets inserted once, updated once, and deleted once
/// - Each operation is 10 seconds apart
///
/// Timeline:
/// [{r1_t01, v1}, {r1_t11, v2}, {r1_t21, del},
///  {r2_t02, v1}, {r2_t12, v2}, {r2_t22, del},
///  {r3_t03, v1}, {r3_t13, v2}, {r3_t23, del}]
///
/// Statistics collected:
/// - oldest_stale_version_ts: t01, newest_stale_version_ts: t13
/// - oldest_delete_ts: t21, newest_delete_ts: t23
///
/// If GC safe point is t22:
/// - Discardable stale versions: 2 out of 6 total (versions t01-t13 span, t22
///   cuts at ~2/3)
/// - Discardable deletes: 1 out of 3 total (deletes t21-t23 span, t22 cuts at
///   ~1/3)
#[derive(Clone, Debug)]
pub struct MvccProperties {
    pub min_ts: TimeStamp,     // The minimal timestamp.
    pub max_ts: TimeStamp,     // The maximal timestamp.
    pub num_rows: u64,         // The number of rows.
    pub num_puts: u64,         // The number of MVCC puts of all rows.
    pub num_deletes: u64,      // The number of MVCC deletes of all rows.
    pub num_versions: u64,     // The number of MVCC versions of all rows.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single row.
    pub ttl: TtlProperties,    // The ttl properties of all rows, for RawKV only.
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
            max_row_versions: 0,
            ttl: TtlProperties::default(),
            oldest_stale_version_ts: TimeStamp::max(),
            newest_stale_version_ts: TimeStamp::zero(),
            oldest_delete_ts: TimeStamp::max(),
            newest_delete_ts: TimeStamp::zero(),
        }
    }

    pub fn add(&mut self, other: &MvccProperties) {
        self.min_ts = cmp::min(self.min_ts, other.min_ts);
        self.max_ts = cmp::max(self.max_ts, other.max_ts);
        self.num_rows += other.num_rows;
        self.num_puts += other.num_puts;
        self.num_deletes += other.num_deletes;
        self.num_versions += other.num_versions;
        self.max_row_versions = cmp::max(self.max_row_versions, other.max_row_versions);
        self.ttl.merge(&other.ttl);
        self.oldest_stale_version_ts =
            cmp::min(self.oldest_stale_version_ts, other.oldest_stale_version_ts);
        self.newest_stale_version_ts =
            cmp::max(self.newest_stale_version_ts, other.newest_stale_version_ts);
        self.oldest_delete_ts = cmp::min(self.oldest_delete_ts, other.oldest_delete_ts);
        self.newest_delete_ts = cmp::max(self.newest_delete_ts, other.newest_delete_ts);
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
