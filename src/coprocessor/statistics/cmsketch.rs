// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use murmur3::murmur3_x64_128;
use tipb;

/// `CmSketch` is used to estimate point queries.
/// Refer:[Count-Min Sketch](https://en.wikipedia.org/wiki/Count-min_sketch)
#[derive(Clone)]
pub struct CmSketch {
    depth: usize,
    width: usize,
    count: u32,
    table: Vec<Vec<u32>>,
}

impl CmSketch {
    pub fn new(d: usize, w: usize) -> Option<CmSketch> {
        if d == 0 || w == 0 {
            None
        } else {
            Some(CmSketch {
                depth: d,
                width: w,
                count: 0,
                table: vec![vec![0; w]; d],
            })
        }
    }

    // `hash` hashes the data into two u64 using murmur hash.
    fn hash(mut bytes: &[u8]) -> (u64, u64) {
        let out = murmur3_x64_128(&mut bytes, 0).unwrap();
        (out as u64, (out >> 64) as u64)
    }

    // `insert` inserts the data into cm sketch. For each row i, the position at
    // (h1 + h2*i) % width will be incremented by one, where the (h1, h2) is the hash value
    // of data.
    pub fn insert(&mut self, bytes: &[u8]) {
        self.count = self.count.wrapping_add(1);
        let (h1, h2) = CmSketch::hash(bytes);
        for (i, row) in self.table.iter_mut().enumerate() {
            let j = (h1.wrapping_add(h2.wrapping_mul(i as u64)) % self.width as u64) as usize;
            row[j] = row[j].saturating_add(1);
        }
    }

    pub fn into_proto(self) -> tipb::CmSketch {
        let mut proto = tipb::CmSketch::default();
        let mut rows = vec![tipb::CmSketchRow::default(); self.depth];
        for (i, row) in self.table.iter().enumerate() {
            rows[i].set_counters(row.to_vec());
        }
        proto.set_rows(rows.into());
        proto
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cmp::min;
    use std::slice::from_ref;

    use rand::distributions::Distribution;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use zipf::ZipfDistribution;

    use tidb_query_datatype::codec::datum;
    use tidb_query_datatype::codec::datum::Datum;
    use tidb_query_datatype::expr::EvalContext;
    use tikv_util::collections::HashMap;

    impl CmSketch {
        fn query(&self, bytes: &[u8]) -> u32 {
            let (h1, h2) = CmSketch::hash(bytes);
            let mut vals = vec![0u32; self.depth];
            let mut min_counter = u32::max_value();
            for (i, row) in self.table.iter().enumerate() {
                let j = (h1.wrapping_add(h2.wrapping_mul(i as u64)) % self.width as u64) as usize;
                let noise = (self.count - row[j]) / (self.width as u32 - 1);
                vals[i] = row[j].saturating_sub(noise);
                min_counter = min(min_counter, row[j])
            }
            vals.sort();
            min(
                min_counter,
                vals[(self.depth - 1) / 2]
                    + (vals[self.depth / 2] - vals[(self.depth - 1) / 2]) / 2,
            )
        }

        pub fn count(&self) -> u32 {
            self.count
        }
    }

    fn average_error(depth: usize, width: usize, total: u32, max_value: usize, s: f64) -> u64 {
        let mut c = CmSketch::new(depth, width).unwrap();
        let mut map: HashMap<u64, u32> = HashMap::default();
        let gen = ZipfDistribution::new(max_value, s).unwrap();
        let mut rng = StdRng::seed_from_u64(0x01020304);
        for _ in 0..total {
            let val = gen.sample(&mut rng) as u64;
            let bytes =
                datum::encode_value(&mut EvalContext::default(), from_ref(&Datum::U64(val)))
                    .unwrap();
            c.insert(&bytes);
            let counter = map.entry(val).or_insert(0);
            *counter += 1;
        }
        let mut total = 0u64;
        for (val, num) in &map {
            let bytes =
                datum::encode_value(&mut EvalContext::default(), from_ref(&Datum::U64(*val)))
                    .unwrap();
            let estimate = c.query(&bytes);
            let err = if *num > estimate {
                *num - estimate
            } else {
                estimate - *num
            };
            total += u64::from(err)
        }
        total / map.len() as u64
    }

    #[test]
    fn test_hash() {
        let hash_result = CmSketch::hash("€".as_bytes());
        assert_eq!(hash_result.0, 0x59E3303A2FDD9555);
        assert_eq!(hash_result.1, 0x4F9D8BB3E4BC3164);

        let hash_result = CmSketch::hash("€€€€€€€€€€".as_bytes());
        assert_eq!(hash_result.0, 0xCECFEB77375EEF6F);
        assert_eq!(hash_result.1, 0xE9830BC26869E2C6);
    }

    #[test]
    fn test_cm_sketch() {
        let (depth, width) = (8, 2048);
        let (total, max_value) = (10000, 10000000);
        assert_eq!(average_error(depth, width, total, max_value, 1.1), 1);
        assert_eq!(average_error(depth, width, total, max_value, 2.0), 2);
        assert_eq!(average_error(depth, width, total, max_value, 3.0), 2);
    }
}
