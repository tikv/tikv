// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;

pub struct HistogramReader {
    histogram: Histogram,
    // histogram value at last read.
    sum: f64,
    count: u64,
}

impl HistogramReader {
    pub fn new(histogram: Histogram) -> Self {
        let (sum, count) = (histogram.get_sample_sum(), histogram.get_sample_count());
        HistogramReader {
            histogram,
            sum,
            count,
        }
    }

    // Returns histogram average value since last read.
    pub fn read_latest_avg(&mut self) -> f64 {
        let (sum, count) = (
            self.histogram.get_sample_sum(),
            self.histogram.get_sample_count(),
        );
        if count == self.count {
            return 0.0;
        }
        let val = (sum - self.sum) / (count - self.count) as f64;
        self.sum = sum;
        self.count = count;
        val
    }
}
