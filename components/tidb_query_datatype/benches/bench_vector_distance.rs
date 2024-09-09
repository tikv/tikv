// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tidb_query_datatype::codec::mysql::VectorFloat32Ref;

fn bench_l1_distance_3d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vb: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("l1_distance_3d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).l1_distance(black_box(vec_vb)).unwrap());
        });
    });
}

fn bench_l1_distance_784d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.0; 784];
    let vb: Vec<f32> = vec![1.0; 784];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("l1_distance_784d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).l1_distance(black_box(vec_vb)).unwrap());
        });
    });
}

fn bench_l2_squared_distance_3d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vb: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("l2_squared_distance_3d", |b| {
        b.iter(|| {
            black_box(
                black_box(vec_va)
                    .l2_squared_distance(black_box(vec_vb))
                    .unwrap(),
            );
        });
    });
}

fn bench_l2_squared_distance_784d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.0; 784];
    let vb: Vec<f32> = vec![1.0; 784];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("l2_squared_distance_784d", |b| {
        b.iter(|| {
            black_box(
                black_box(vec_va)
                    .l2_squared_distance(black_box(vec_vb))
                    .unwrap(),
            );
        });
    });
}

fn bench_l2_distance_3d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vb: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("l2_distance_3d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).l2_distance(black_box(vec_vb)).unwrap());
        });
    });
}

fn bench_l2_distance_784d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.0; 784];
    let vb: Vec<f32> = vec![1.0; 784];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("l2_distance_784d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).l2_distance(black_box(vec_vb)).unwrap());
        });
    });
}

fn bench_inner_product_3d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vb: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("inner_product_3d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).inner_product(black_box(vec_vb)).unwrap());
        });
    });
}

fn bench_inner_product_784d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.0; 784];
    let vb: Vec<f32> = vec![1.0; 784];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("inner_product_784d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).inner_product(black_box(vec_vb)).unwrap());
        });
    });
}

fn bench_cosine_distance_3d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vb: Vec<f32> = vec![1.1, 2.2, 3.3];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("cosine_distance_3d", |b| {
        b.iter(|| {
            black_box(
                black_box(vec_va)
                    .cosine_distance(black_box(vec_vb))
                    .unwrap(),
            );
        });
    });
}

fn bench_cosine_distance_784d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.0; 784];
    let vb: Vec<f32> = vec![1.0; 784];
    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());
    let vec_vb = VectorFloat32Ref::from_f32(vb.as_slice());

    c.bench_function("cosine_distance_784d", |b| {
        b.iter(|| {
            black_box(
                black_box(vec_va)
                    .cosine_distance(black_box(vec_vb))
                    .unwrap(),
            );
        });
    });
}

fn bench_l2_norm_3d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.1, 2.2, 3.3];

    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());

    c.bench_function("l2_norm_3d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).l2_norm());
        });
    });
}

fn bench_l2_norm_784d(c: &mut Criterion) {
    let va: Vec<f32> = vec![1.0; 784];

    let vec_va = VectorFloat32Ref::from_f32(va.as_slice());

    c.bench_function("l2_norm_784d", |b| {
        b.iter(|| {
            black_box(black_box(vec_va).l2_norm());
        });
    });
}

criterion_group!(
    benches,
    bench_l1_distance_3d,
    bench_l1_distance_784d,
    bench_l2_squared_distance_3d,
    bench_l2_squared_distance_784d,
    bench_l2_distance_3d,
    bench_l2_distance_784d,
    bench_inner_product_3d,
    bench_inner_product_784d,
    bench_cosine_distance_3d,
    bench_cosine_distance_784d,
    bench_l2_norm_3d,
    bench_l2_norm_784d,
);
criterion_main!(benches);
