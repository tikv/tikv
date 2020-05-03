use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn instant_bench(c: &mut Criterion) {
    c.bench_function("Instant::now()", |b| b.iter(|| std::time::Instant::now()));
}

fn systime_bench(c: &mut Criterion) {
    c.bench_function("SystemTime::now()", |b| {
        b.iter(|| std::time::SystemTime::now())
    });
}

fn root_span_instant_bench(c: &mut Criterion) {
    c.bench_function("span_root new & drop instant", |b| {
        let (tx, _rx) = crossbeam::channel::unbounded();

        b.iter(|| {
            tracer::new_span_root(
                black_box("root"),
                black_box(tx.clone()),
                black_box(tracer::time_measure::TimeMeasureType::Instant),
            )
        })
    });
}

fn root_span_systime_bench(c: &mut Criterion) {
    c.bench_function("span_root new & drop systime", |b| {
        let (tx, _rx) = crossbeam::channel::unbounded();

        b.iter(|| {
            tracer::new_span_root(
                black_box("root"),
                black_box(tx.clone()),
                black_box(tracer::time_measure::TimeMeasureType::SystemTime),
            )
        })
    });
}

fn channel_clone_send_bench(c: &mut Criterion) {
    c.bench_function("crossbeam::channel clone send", |b| {
        let (tx, _rx) = crossbeam::channel::unbounded();

        b.iter(|| {
            let tx = tx.clone();
            let _ = tx.try_send(black_box(1));
        })
    });
}

fn atomic_usize_bench(c: &mut Criterion) {
    c.bench_function("AtomicUsize new & sub", |b| {
        b.iter(|| {
            let count = std::sync::atomic::AtomicUsize::new(black_box(1));
            count.fetch_sub(black_box(1), black_box(std::sync::atomic::Ordering::Release));
            std::sync::atomic::fence(black_box(std::sync::atomic::Ordering::Acquire));
        })
    });
}

fn sharded_slab_insert_take(c: &mut Criterion) {
    c.bench_function("sharded_slab insert & take", |b| {
        let slab = sharded_slab::Slab::new();

        b.iter(|| {
            let idx = slab.insert(black_box(1)).unwrap();
            let _ = slab.take(black_box(idx)).unwrap();
        })
    });
}

criterion_group!(
    benches,
    instant_bench,
    systime_bench,
    root_span_instant_bench,
    root_span_systime_bench,
    channel_clone_send_bench,
    atomic_usize_bench,
    sharded_slab_insert_take,
);
criterion_main!(benches);
