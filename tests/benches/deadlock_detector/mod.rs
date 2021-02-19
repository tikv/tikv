// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{Bencher, Criterion};
use kvproto::deadlock::*;
use rand::prelude::*;
use tikv::server::lock_manager::deadlock::DetectTable;
use tikv_util::time::Duration;

struct DetectGenerator {
    rng: ThreadRng,
    range: u64,
    timestamp: u64,
}

impl DetectGenerator {
    fn new(range: u64) -> Self {
        Self {
            rng: ThreadRng::default(),
            range,
            timestamp: 0,
        }
    }

    /// Generates n detect requests with the same timestamp
    fn generate(&mut self, n: u64) -> Vec<WaitForEntry> {
        let mut entries = Vec::with_capacity(n as usize);
        (0..n).for_each(|_| {
            let mut entry = WaitForEntry::default();
            entry.set_txn(self.timestamp);
            let mut wait_for_txn = self.timestamp;
            while wait_for_txn == self.timestamp {
                wait_for_txn = self.rng.gen_range(
                    if self.timestamp < self.range {
                        0
                    } else {
                        self.timestamp - self.range
                    },
                    self.timestamp + self.range,
                );
            }
            entry.set_wait_for_txn(wait_for_txn);
            entry.set_key_hash(self.rng.gen());
            entries.push(entry);
        });
        self.timestamp += 1;
        entries
    }
}

#[derive(Debug)]
struct Config {
    n: u64,
    range: u64,
    ttl: Duration,
}

fn bench_detect(b: &mut Bencher, cfg: &Config) {
    let mut detect_table = DetectTable::new(cfg.ttl);
    let mut generator = DetectGenerator::new(cfg.range);
    b.iter(|| {
        for entry in generator.generate(cfg.n) {
            detect_table.detect(
                entry.get_txn().into(),
                entry.get_wait_for_txn().into(),
                entry.get_key_hash(),
            );
        }
    });
}

fn bench_dense_detect_without_cleanup(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_dense_detect_without_cleanup");

    let ranges = vec![
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
    ];
    for range in ranges {
        let config = Config {
            n: 10,
            range,
            ttl: Duration::from_secs(100000000),
        };
        group.bench_with_input(format!("{:?}", &config), &config, bench_detect);
    }
}

fn bench_dense_detect_with_cleanup(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_dense_detect_with_cleanup");

    let ttls = vec![1, 3, 5, 10, 100, 500, 1_000, 3_000];
    for ttl in &ttls {
        let config = Config {
            n: 10,
            range: 1000,
            ttl: Duration::from_millis(*ttl),
        };
        group.bench_with_input(format!("{:?}", &config), &config, bench_detect);
    }
    group.finish();
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args().sample_size(10);
    bench_dense_detect_without_cleanup(&mut criterion);
    bench_dense_detect_with_cleanup(&mut criterion);
    criterion.final_summary();
}
