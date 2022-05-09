// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::mpsc::channel, thread, usize};

use crossbeam::channel;
use futures::{executor::block_on, stream::StreamExt};
use test::Bencher;
use tikv_util::mpsc;

#[bench]
fn bench_thread_channel(b: &mut Bencher) {
    let (tx, rx) = channel();

    let t = thread::spawn(move || {
        let mut n2: usize = 0;
        loop {
            let n = rx.recv().unwrap();
            if n == 0 {
                return n2;
            }
            n2 += 1;
        }
    });

    let mut n1 = 0;
    b.iter(|| {
        n1 += 1;
        tx.send(1).unwrap()
    });

    tx.send(0).unwrap();
    let n2 = t.join().unwrap();
    assert_eq!(n1, n2);
}

#[bench]
fn bench_util_channel(b: &mut Bencher) {
    let (tx, rx) = mpsc::unbounded();

    let t = thread::spawn(move || {
        let mut n2: usize = 0;
        loop {
            let n = rx.recv().unwrap();
            if n == 0 {
                return n2;
            }
            n2 += 1;
        }
    });

    let mut n1 = 0;
    b.iter(|| {
        n1 += 1;
        tx.send(1).unwrap()
    });

    tx.send(0).unwrap();
    let n2 = t.join().unwrap();
    assert_eq!(n1, n2);
}

#[bench]
fn bench_util_loose(b: &mut Bencher) {
    let (tx, rx) = mpsc::loose_bounded(480_000);

    let t = thread::spawn(move || {
        let mut n2: usize = 0;
        loop {
            let n = rx.recv().unwrap();
            if n == 0 {
                return n2;
            }
            n2 += 1;
        }
    });

    let mut n1 = 0;
    b.iter(|| {
        n1 += 1;
        while tx.try_send(1).is_err() {}
    });

    while tx.try_send(0).is_err() {}

    let n2 = t.join().unwrap();
    assert_eq!(n1, n2);
}

#[bench]
fn bench_crossbeam_channel(b: &mut Bencher) {
    let (tx, rx) = channel::unbounded();

    let t = thread::spawn(move || {
        let mut n2: usize = 0;
        loop {
            let n = rx.recv().unwrap();
            if n == 0 {
                return n2;
            }
            n2 += 1;
        }
    });

    let mut n1 = 0;
    b.iter(|| {
        n1 += 1;
        tx.send(1).unwrap();
    });

    tx.send(0).unwrap();
    let n2 = t.join().unwrap();
    assert_eq!(n1, n2);
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

    let mut rx = Some(mpsc::batch::BatchReceiver::new(
        rx,
        32,
        Vec::new,
        mpsc::batch::VecCollector,
    ));

    b.iter(|| {
        let mut count = 0;
        let mut rx1 = rx.take().unwrap();
        loop {
            let (item, s) = block_on(rx1.into_future());
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
            let (item, s) = block_on(rx1.into_future());
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
