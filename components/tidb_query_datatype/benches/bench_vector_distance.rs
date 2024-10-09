// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tidb_query_datatype::codec::mysql::VectorFloat32Ref;

static TEST_CASES: &[(&str, &[f32])] = &[
    ("3d", &[1.1, 2.2, 3.3]),
    ("10d", &[1.0; 10]),
    ("100d", &[1.0; 100]),
    ("400d", &[1.0; 400]),
    ("784d", &[1.0; 784]),
    ("1000d", &[1.0; 1000]),
    ("4000d", &[1.0; 4000]),
];

fn bench_l1_distance(c: &mut Criterion) {
    for (label, va) in TEST_CASES {
        let vec_va = VectorFloat32Ref::from_f32(va);
        let vec_vb = VectorFloat32Ref::from_f32(va);

        c.bench_function(&format!("l1_distance_{}", label), |b| {
            b.iter(|| {
                black_box(black_box(vec_va).l1_distance(black_box(vec_vb)).unwrap());
            });
        });
    }
}

fn bench_l2_squared_distance(c: &mut Criterion) {
    for (label, va) in TEST_CASES {
        let vec_va = VectorFloat32Ref::from_f32(va);
        let vec_vb = VectorFloat32Ref::from_f32(va);

        c.bench_function(&format!("l2_squared_distance_{}", label), |b| {
            b.iter(|| {
                black_box(
                    black_box(vec_va)
                        .l2_squared_distance(black_box(vec_vb))
                        .unwrap(),
                );
            });
        });
    }
}

fn bench_l2_distance(c: &mut Criterion) {
    for (label, va) in TEST_CASES {
        let vec_va = VectorFloat32Ref::from_f32(va);
        let vec_vb = VectorFloat32Ref::from_f32(va);

        c.bench_function(&format!("bench_l2_distance_{}", label), |b| {
            b.iter(|| {
                black_box(black_box(vec_va).l2_distance(black_box(vec_vb)).unwrap());
            });
        });
    }
}

fn bench_inner_product(c: &mut Criterion) {
    for (label, va) in TEST_CASES {
        let vec_va = VectorFloat32Ref::from_f32(va);
        let vec_vb = VectorFloat32Ref::from_f32(va);

        c.bench_function(&format!("bench_inner_product_{}", label), |b| {
            b.iter(|| {
                black_box(black_box(vec_va).inner_product(black_box(vec_vb)).unwrap());
            });
        });
    }
}

fn bench_cosine_distance(c: &mut Criterion) {
    for (label, va) in TEST_CASES {
        let vec_va = VectorFloat32Ref::from_f32(va);
        let vec_vb = VectorFloat32Ref::from_f32(va);

        c.bench_function(&format!("bench_cosine_distance_{}", label), |b| {
            b.iter(|| {
                black_box(
                    black_box(vec_va)
                        .cosine_distance(black_box(vec_vb))
                        .unwrap(),
                );
            });
        });
    }
}

fn bench_l2_norm(c: &mut Criterion) {
    for (label, va) in TEST_CASES {
        let vec_va = VectorFloat32Ref::from_f32(va);

        c.bench_function(&format!("bench_l2_norm_{}", label), |b| {
            b.iter(|| {
                black_box(black_box(vec_va).l2_norm()).unwrap();
            });
        });
    }
}

criterion_group!(
    benches,
    bench_l1_distance,
    bench_l2_squared_distance,
    bench_l2_distance,
    bench_inner_product,
    bench_cosine_distance,
    bench_l2_norm,
);
criterion_main!(benches);