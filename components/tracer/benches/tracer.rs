use criterion::{black_box, criterion_group, criterion_main, Criterion};

#[minitrace::trace(0u32)]
fn dummy() {}

fn root_span_channel_instant_bench(c: &mut Criterion) {
    c.bench_function("new_span_root channel instant_coarse", |b| {
        let mut txs = Vec::with_capacity(100);
        let mut rxs = Vec::with_capacity(100);
        for _ in 0..100 {
            let (tx, rx) = minitrace::Collector::new(minitrace::CollectorType::Void);
            txs.push(tx);
            rxs.push(rx);
        }

        b.iter(|| {
            for i in 0..100 {
                let g = minitrace::new_span_root(black_box(txs[i].clone()), black_box(0u32));
                black_box(g.enter());
            }
        });
    });
}

fn root_span_channel_instant_bench_cmp(c: &mut Criterion) {
    c.bench_function("new_span_root channel instant", |b| {
        let mut txs = Vec::with_capacity(100);
        let mut rxs = Vec::with_capacity(100);
        for _ in 0..100 {
            let (tx, rx) = minitrace_cmp::Collector::new(minitrace_cmp::CollectorType::Void);
            txs.push(tx);
            rxs.push(rx);
        }

        b.iter(|| {
            for i in 0..100 {
                let g = minitrace_cmp::new_span_root(black_box(txs[i].clone()), black_box(0u32));
                black_box(g.enter());
            }
        });
    });
}

fn tracing_bench(c: &mut Criterion) {
    // let subscriber = tracing_subscriber::Registry::default();
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    c.bench_function("tokio tracing span", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let span = black_box(tracing::info_span!(""));
                let _enter = span.enter();
            }
        });
    });
}

fn child_span_bench(c: &mut Criterion) {
    c.bench_function("new_span channel instant", |b| {
        let (tx, _rx) = minitrace::Collector::new(minitrace::DEFAULT_COLLECTOR);

        b.iter(|| {
            let g = minitrace::new_span_root(black_box(tx.clone()), black_box(0u32));
            let _e = black_box(g.enter());

            for _ in 0..100 {
                black_box(dummy());
            }
        });
    });
}

// fn instant_coarse_bench(c: &mut Criterion) {
//     c.bench_function("instant coarse", |b| {
//         b.iter(|| tikv_util::time::Instant::now_coarse());
//     });
// }

// fn instant_bench(c: &mut Criterion) {
//     c.bench_function("instant", |b| {
//         b.iter(|| tikv_util::time::Instant::now());
//     });
// }

criterion_group!(
    benches, 
    // root_span_channel_instant_bench_cmp,
    // root_span_channel_instant_bench,
    // tracing_bench,
    child_span_bench,
    // instant_coarse_bench,
    // instant_bench,
);
criterion_main!(benches);
