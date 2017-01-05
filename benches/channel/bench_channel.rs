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

use test::Bencher;
use std::thread;
use std::sync::mpsc::channel;

use mio::{EventLoop, Handler, Sender};

struct CountHandler {
    n: usize,
}

impl Handler for CountHandler {
    type Timeout = ();
    type Message = u32;

    fn notify(&mut self, event_loop: &mut EventLoop<CountHandler>, msg: u32) {
        if msg == 0 {
            event_loop.shutdown();
            return;
        }

        self.n += 1;
    }
}

fn mio_must_send(sender: &Sender<u32>, n: u32) {
    loop {
        // Send may return notify error, we must retry.
        if sender.send(n).is_ok() {
            return;
        }
    }
}

#[bench]
fn bench_mio_channel(b: &mut Bencher) {
    let mut event_loop = EventLoop::new().unwrap();
    let sender = event_loop.channel();

    let t = thread::spawn(move || {
        let mut h = CountHandler { n: 0 };
        event_loop.run(&mut h).unwrap();
        h.n
    });

    let mut n1 = 0;
    b.iter(|| {
        n1 += 1;
        mio_must_send(&sender, 1);
    });

    mio_must_send(&sender, 0);

    let n2 = t.join().unwrap();
    assert_eq!(n1, n2);
}

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
