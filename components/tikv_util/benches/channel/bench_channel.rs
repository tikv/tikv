// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::channel;
use std::{thread, usize};
use test::Bencher;

use crossbeam::channel;
use futures::{Future, Stream};
use tikv_util::mpsc;

#[bench]
fn bench_thread_channel(b: &mut Bencher) {
    let (tx, rx) = channel();
    tx.send(0).unwrap();

    b.iter(|| {
        rx.try_recv().unwrap();
        tx.send(1).unwrap();
        rx.try_recv().unwrap();
        rx.try_recv().unwrap_err();
        tx.send(1).unwrap();
    });

    tx.send(0).unwrap();
    rx.try_recv().unwrap();
}

#[bench]
fn bench_util_channel(b: &mut Bencher) {
    let (tx, rx) = mpsc::channel::unbounded();
    tx.send(0).unwrap();

    b.iter(|| {
        rx.try_recv().unwrap();
        tx.send(1).unwrap();
        rx.try_recv().unwrap();
        rx.try_recv().unwrap_err();
        tx.send(1).unwrap();
    });

    tx.send(0).unwrap();
}

#[bench]
fn bench_util_loose(b: &mut Bencher) {
    let (tx, rx) = mpsc::channel::loose_bounded(480_000);
    tx.try_send(0).unwrap();

    b.iter(|| {
        rx.try_recv().unwrap();
        tx.try_send(1).unwrap();
        rx.try_recv().unwrap();
        rx.try_recv().unwrap_err();
        tx.try_send(1).unwrap();
    });

    tx.try_send(0).unwrap();
}

#[bench]
fn bench_util_loose_len(b: &mut Bencher) {
    let (tx, rx) = mpsc::channel::loose_bounded(480_000);
    tx.try_send(0).unwrap();

    b.iter(|| {
        assert_eq!(rx.len(), 1);
    });

    tx.try_send(0).unwrap();
}

#[bench]
fn bench_util_loose_queue(b: &mut Bencher) {
    let (tx, rx) = mpsc::queue::loose_bounded(480_000);
    tx.try_send(0).unwrap();

    b.iter(|| {
        rx.try_recv().unwrap();
        tx.try_send(1).unwrap();
        rx.try_recv().unwrap();
        rx.try_recv().unwrap_err();
        tx.try_send(1).unwrap();
    });

    tx.try_send(0).unwrap();
}

#[bench]
fn bench_util_loose_queue_len(b: &mut Bencher) {
    let (tx, rx) = mpsc::queue::loose_bounded(480_000);
    tx.try_send(0).unwrap();

    b.iter(|| {
        assert_eq!(rx.len(), 1);
    });

    tx.try_send(0).unwrap();
}

#[bench]
fn bench_crossbeam_channel(b: &mut Bencher) {
    let (tx, rx) = channel::unbounded();
    tx.try_send(0).unwrap();

    b.iter(|| {
        rx.try_recv().unwrap();
        tx.try_send(1).unwrap();
        rx.try_recv().unwrap();
        rx.try_recv().unwrap_err();
        tx.try_send(1).unwrap();
    });

    tx.try_send(0).unwrap();
}

#[bench]
fn bench_receiver_stream_batch(b: &mut Bencher) {
    let (tx, rx) = mpsc::batch::bounded::<i32>(128, 8);
    for _ in 0..1 {
        let tx1 = tx.clone();
        thread::spawn(move || {
            (0..usize::MAX)
                .take_while(|i| tx1.send(*i as i32).is_ok())
                .count();
        });
    }

    let mut rx = Some(mpsc::batch::BatchReceiver::new(rx, 32, Vec::new, |v, t| {
        v.push(t)
    }));

    b.iter(|| {
        let mut count = 0;
        let mut rx1 = rx.take().unwrap();
        loop {
            let (item, s) = rx1
                .into_future()
                .wait()
                .map_err(|_| unreachable!())
                .unwrap();
            rx1 = s;
            if let Some(v) = item {
                count += v.len();
                if count < 10000 {
                    continue;
                }
            }
            break;
        }
        rx = Some(rx1);
    })
}

#[bench]
fn bench_receiver_stream(b: &mut Bencher) {
    let (tx, rx) = mpsc::batch::bounded::<i32>(128, 1);
    for _ in 0..1 {
        let tx1 = tx.clone();
        thread::spawn(move || {
            (0..usize::MAX)
                .take_while(|i| tx1.send(*i as i32).is_ok())
                .count();
        });
    }

    let mut rx = Some(rx);
    b.iter(|| {
        let mut count = 0;
        let mut rx1 = rx.take().unwrap();
        loop {
            let (item, s) = rx1
                .into_future()
                .wait()
                .map_err(|_| unreachable!())
                .unwrap();
            rx1 = s;
            if item.is_some() {
                count += 1;
                if count < 10000 {
                    continue;
                }
            }
            break;
        }
        rx = Some(rx1);
    })
}
