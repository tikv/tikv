use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn root_span_channel_instant_bench(c: &mut Criterion) {
    c.bench_function("new_span_root channel instant", |b| {
        let mut txs = Vec::with_capacity(100);
        let mut rxs = Vec::with_capacity(100);
        for _ in 0..100 {
            let (tx, rx) = minitrace::Collector::new(minitrace::DEFAULT_COLLECTOR);
            txs.push(tx);
            rxs.push(rx);
        }

        b.iter(|| {
            for i in 0..100 {
                let g = minitrace::new_span_root(
                    black_box(txs[i].clone()),
                    black_box(0u32),
                );
                black_box(g.enter());
            }
        });
    });
}

criterion_group!(
    benches,
    root_span_channel_instant_bench,
);
criterion_main!(benches);
