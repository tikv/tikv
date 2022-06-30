// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use cdc::CdcEvent;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kvproto::cdcpb::ResolvedTs;
use protobuf::Message;

fn bench_cdc_event_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_cdc_event_size");

    // A typical region id.
    let region_id = 4194304;
    // A typical ts.
    let ts = 426624231625982140;
    // Benchmark from 1 region id to 131,072 region ids.
    for i in 0..18 {
        let len = 2usize.pow(i);
        let mut resolved_ts = ResolvedTs::default();
        resolved_ts.ts = ts;
        resolved_ts.regions = vec![region_id; len];

        let message_compute_size = resolved_ts.clone();
        group.bench_with_input(
            BenchmarkId::new("protobuf::Message::compute_size", len),
            &message_compute_size,
            |b, message_compute_size| {
                b.iter(|| {
                    black_box(message_compute_size.compute_size());
                });
            },
        );

        let cdc_event_size = CdcEvent::ResolvedTs(resolved_ts);
        group.bench_with_input(
            BenchmarkId::new("CdcEvent::ResolvedTs::size", len),
            &cdc_event_size,
            |b, cdc_event_size| {
                b.iter(move || {
                    black_box(cdc_event_size.size());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_cdc_event_size);
criterion_main!(benches);
