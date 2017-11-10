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

use tipb::analyze;
use byteorder::{ByteOrder, LittleEndian};
use protobuf::RepeatedField;
use murmur3::murmur3_x64_128;

/// CMSketch is used to estimate point queries.
/// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
#[derive(Clone)]
pub struct CMSketch {
    depth: usize,
    width: usize,
    count: u32,
    table: Vec<Vec<u32>>,
}

impl CMSketch {
    pub fn new(d: usize, w: usize) -> CMSketch {
        CMSketch {
            depth: d,
            width: w,
            count: 0,
            table: vec![vec![0; w]; d],
        }
    }

    fn hash(mut bytes: &[u8]) -> (u64, u64) {
        let mut out: [u8; 16] = [0; 16];
        murmur3_x64_128(&mut bytes, 0, &mut out);
        (
            LittleEndian::read_u64(&out[0..8]),
            LittleEndian::read_u64(&out[8..16]),
        )
    }

    pub fn insert(&mut self, bytes: &[u8]) {
        self.count = self.count.wrapping_add(1);
        let (h1, h2) = CMSketch::hash(bytes);
        for i in 0..self.depth {
            let j = (h1.wrapping_add(h2.wrapping_mul(i as u64)) % self.width as u64) as usize;
            self.table[i][j] = self.table[i][j].saturating_add(1);
        }
    }

    pub fn into_proto(&self) -> analyze::CMSketch {
        let mut proto = analyze::CMSketch::new();
        let mut rows = vec![analyze::CMSketchRow::default(); self.depth];
        for i in 0..self.depth {
            rows[i].set_counters(self.table[i].to_owned());
        }
        proto.set_rows(RepeatedField::from_vec(rows));
        proto
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::datum;
    use coprocessor::codec::datum::Datum;
    use util::as_slice;
    use std::collections::HashMap;
    use std::cmp::min;
    use rand::{thread_rng, Rng};
    use zipf::ZipfDistribution;
    use super::*;

    impl CMSketch {
        fn query(&self, bytes: &[u8]) -> u32 {
            let (h1, h2) = CMSketch::hash(bytes);
            let mut vals = vec![0u32; self.depth];
            let mut min_counter = u32::max_value();
            for i in 0..self.depth {
                let j = (h1.wrapping_add(h2.wrapping_mul(i as u64)) % self.width as u64) as usize;
                let noise = (self.count - self.table[i][j]) / (self.width as u32 - 1);
                vals[i] = self.table[i][j].saturating_sub(noise);
                min_counter = min(min_counter, self.table[i][j])
            }
            vals.sort();
            min(
                min_counter,
                vals[(self.depth - 1) / 2] +
                    (vals[self.depth / 2] - vals[(self.depth - 1) / 2]) / 2,
            )
        }

        pub fn count(&self) -> u32 {
            self.count
        }
    }

    fn average_error(depth: usize, width: usize, total: u32, max_value: usize, s: f64) -> u64 {
        let mut c = CMSketch::new(depth, width);
        let mut map: HashMap<u64, u32> = HashMap::new();
        let mut gen = ZipfDistribution::new(thread_rng(), max_value, s).unwrap();
        for _ in 0..total {
            let val = gen.next_u64();
            let bytes = datum::encode_value(as_slice(&Datum::U64(val))).unwrap();
            c.insert(&bytes);
            let counter = map.entry(val).or_insert(0);
            *counter += 1;
        }
        let mut total = 0u64;
        for (val, num) in &map {
            let bytes = datum::encode_value(as_slice(&Datum::U64(*val))).unwrap();
            let estimate = c.query(&bytes);
            let err = if *num > estimate {
                *num - estimate
            } else {
                estimate - *num
            };
            total += err as u64
        }
        total / map.len() as u64
    }

    #[test]
    fn test_cm_sketch() {
        let (depth, width) = (8, 2048);
        let (total, max_value) = (1000000, 10000000);
        assert_eq!(
            average_error(depth, width, total, max_value, 1.1) <= 6,
            true
        );
        assert_eq!(
            average_error(depth, width, total, max_value, 2.0) <= 33,
            true
        );
        assert_eq!(
            average_error(depth, width, total, max_value, 3.0) <= 90,
            true
        );
    }
}
