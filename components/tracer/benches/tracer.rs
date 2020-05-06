use criterion::{black_box, criterion_group, criterion_main, Criterion};

thread_local! {
    static STACK: std::cell::RefCell<Vec<i32>> = std::cell::RefCell::new(Vec::with_capacity(1024));
}

fn instant_bench(c: &mut Criterion) {
    c.bench_function("Instant::now()", |b| b.iter(|| std::time::Instant::now()));
}

fn systime_bench(c: &mut Criterion) {
    c.bench_function("SystemTime::now()", |b| {
        b.iter(|| std::time::SystemTime::now())
    });
}

fn begin_step() {
    black_box(std::time::Instant::now());
}

fn final_step() {
    black_box(std::time::Instant::now());
}

fn min_req_bench(c: &mut Criterion) {
    c.bench_function("minimum requirements", |b| {
        b.iter(move || {
            begin_step();
            final_step();
        })
    });
}

fn root_span_channel_instant_bench(c: &mut Criterion) {
    c.bench_function("new_span_root channel instant", |b| {
        let tracer::Collector { tx, rx: _ } =
            tracer::Collector::new(tracer::CollectorType::Channel);

        b.iter(|| {
            tracer::new_span_root(
                black_box("root"),
                black_box(tx.clone()),
                black_box(tracer::time_measure::TimeMeasureType::Instant),
            )
        });
    });
}

fn root_span_channel_systime_bench(c: &mut Criterion) {
    c.bench_function("new_span_root channel systime", |b| {
        let tracer::Collector { tx, rx: _ } =
            tracer::Collector::new(tracer::CollectorType::Channel);

        b.iter(|| {
            tracer::new_span_root(
                black_box("root"),
                black_box(tx.clone()),
                black_box(tracer::time_measure::TimeMeasureType::SystemTime),
            )
        });
    });
}

fn root_span_channeless_instant_bench(c: &mut Criterion) {
    c.bench_function("new_span_root channeless instant", |b| {
        let tracer::Collector { tx, rx: _ } = tracer::Collector::new(tracer::CollectorType::Void);

        b.iter(|| {
            tracer::new_span_root(
                black_box("root"),
                black_box(tx.clone()),
                black_box(tracer::time_measure::TimeMeasureType::Instant),
            )
        });
    });
}

fn root_span_channeless_systime_bench(c: &mut Criterion) {
    c.bench_function("new_span_root channeless systime", |b| {
        let tracer::Collector { tx, rx: _ } = tracer::Collector::new(tracer::CollectorType::Void);

        b.iter(|| {
            tracer::new_span_root(
                black_box("root"),
                black_box(tx.clone()),
                black_box(tracer::time_measure::TimeMeasureType::SystemTime),
            )
        });
    });
}

fn channel_clone_send_bench(c: &mut Criterion) {
    c.bench_function("crossbeam::channel clone send", |b| {
        let (tx, _rx) = crossbeam::channel::unbounded();

        b.iter(|| {
            let tx = black_box(tx.clone());
            tx.try_send(black_box(1))
        })
    });
}

fn tls_vec_push_pop_bench(c: &mut Criterion) {
    c.bench_function("thread local vector push pop", |b| {
        b.iter(|| {
            black_box(STACK.with(|vec| {
                let mut vec = black_box(vec.borrow_mut());
                vec.push(black_box(1))
            }));
            STACK.with(|vec| {
                let mut vec = black_box(vec.borrow_mut());
                vec.pop()
            })
        })
    });
}

criterion_group!(
    benches,
    min_req_bench,
    instant_bench,
    systime_bench,
    root_span_channel_instant_bench,
    root_span_channel_systime_bench,
    root_span_channeless_instant_bench,
    root_span_channeless_systime_bench,
    channel_clone_send_bench,
    tls_vec_push_pop_bench,
);
criterion_main!(benches);
