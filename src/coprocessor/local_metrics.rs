// Copyright 2017 PingCAP, Inc.
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

use coprocessor::metrics::*;

/// `ScanCounter` is for recording range query and point query.
#[derive(Default, Debug)]
pub struct ScanCounter {
    range: usize,
    point: usize,
}

impl ScanCounter {
    #[inline]
    pub fn inc_range(&mut self) {
        self.range += 1;
    }

    #[inline]
    pub fn inc_point(&mut self) {
        self.point += 1;
    }

    /// Merge records from `other` into `self`, and clear `other`.
    #[inline]
    pub fn merge(&mut self, other: &mut ScanCounter) {
        self.range += other.range;
        self.point += other.point;
        other.range = 0;
        other.point = 0;
    }

    #[allow(dead_code)]
    #[inline]
    pub fn flush(&mut self) {
        if self.range > 0 {
            let range_counter = COPR_GET_OR_SCAN_COUNT.with_label_values(&["range"]);
            range_counter.inc_by(self.range as f64).unwrap();
            self.range = 0;
        }
        if self.point > 0 {
            let point_counter = COPR_GET_OR_SCAN_COUNT.with_label_values(&["point"]);
            point_counter.inc_by(self.point as f64).unwrap();
            self.point = 0;
        }
    }
}
