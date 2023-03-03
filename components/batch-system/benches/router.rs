// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use batch_system::{test_runner::*, *};
use criterion::*;
use resource_control::ResourceController;

fn bench_send(c: &mut Criterion) {
    let (control_tx, control_fsm) = Runner::new(usize::MAX);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm, None);
    system.spawn("test".to_owned(), Builder::new());
    let (normal_tx, normal_fsm) = Runner::new(usize::MAX);
    let normal_box = BasicMailbox::new(normal_tx, normal_fsm, Arc::default());
    router.register(1, normal_box);

    c.bench_function("router::send", |b| {
        b.iter(|| {
            let _ = router.send(1, Message::Loop(0)).unwrap();
        })
    });
    system.shutdown();
}

fn bench_send_priority(c: &mut Criterion) {
    let (control_tx, control_fsm) = Runner::new(usize::MAX);
    let (router, mut system) = batch_system::create_system(
        &Config::default(),
        control_tx,
        control_fsm,
        Some(Arc::new(ResourceController::new("test".to_owned(), false))),
    );
    system.spawn("test".to_owned(), Builder::new());
    let (normal_tx, normal_fsm) = Runner::new(usize::MAX);
    let normal_box = BasicMailbox::new(normal_tx, normal_fsm, Arc::default());
    router.register(1, normal_box);

    c.bench_function("router::send_priority", |b| {
        b.iter(|| {
            router.send(1, Message::Loop(0)).unwrap();
        })
    });
    system.shutdown();
}

criterion_group!(benches, bench_send, bench_send_priority);
criterion_main!(benches);
