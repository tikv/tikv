// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::mpsc::channel;
use std::thread;
use test::Bencher;

use crossbeam_channel;
use tikv::util::mpsc;

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
    let (tx, rx) = mpsc::loose_bounded(480000);

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
    let (tx, rx) = crossbeam_channel::unbounded();

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
        tx.send(1)
    });

    tx.send(0);
    let n2 = t.join().unwrap();
    assert_eq!(n1, n2);
}
