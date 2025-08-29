// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use kll_rs::KllDoubleSketch;
use txn_types::TimeStamp;

use crate::TtlProperties;

/// MVCC properties for estimating discardable versions based on GC safe point.
///
/// These statistics are collected to estimate the number of discardable MVCC
/// versions using KLL sketches for more accurate timestamp distribution
/// analysis. There are two types of discardable versions:
/// 1. Stale versions: Redundant versions from row updates that can be
///    physically removed
/// 2. Delete versions: Deleted rows that can be physically removed
/// Ignore the the versions that should be kept before GC safe point during
/// estimate.
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
/// KLL sketches separately track timestamp distributions for:
/// - Stale versions (t01, t02, t03, t11, t12, t13)
/// - Delete versions (t21, t22, t23)
///
/// This allows for more accurate estimation of discardable entries based on
/// the actual timestamp distributions rather than uniform distribution
/// assumptions.
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
    // KLL sketches for timestamp distribution analysis
    // Separate sketches for stale versions and delete versions to improve
    // estimation accuracy since they may have different distributions
    pub stale_version_kll_sketch: KllDoubleSketch, // For stale MVCC versions
    pub delete_kll_sketch: KllDoubleSketch,        // For delete MVCC versions
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
            stale_version_kll_sketch: KllDoubleSketch::new().unwrap_or_default(),
            delete_kll_sketch: KllDoubleSketch::new().unwrap_or_default(),
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
        self.stale_version_kll_sketch
            .merge(&other.stale_version_kll_sketch)
            .unwrap();
        self.delete_kll_sketch
            .merge(&other.delete_kll_sketch)
            .unwrap();
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
