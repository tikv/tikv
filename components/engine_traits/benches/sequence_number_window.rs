// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use engine_traits::util::{SequenceNumber, SequenceNumberWindow};
use test::Bencher;

#[bench]
fn bench_sequence_number_window(b: &mut Bencher) {
    fn produce_random_seqno(producer: usize, number: usize) -> Vec<SequenceNumber> {
        let mock_seqno_allocator = Arc::new(AtomicU64::new(1));
        let (tx, rx) = std::sync::mpsc::sync_channel(number);
        let handles: Vec<_> = (0..producer)
            .map(|_| {
                let allocator = mock_seqno_allocator.clone();
                let count = number / producer;
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for _ in 0..count {
                        let mut sn = SequenceNumber::pre_write();
                        sn.post_write(allocator.fetch_add(1, Ordering::AcqRel));
                        tx.send(sn).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        (0..number).map(|_| rx.recv().unwrap()).collect()
    }

    let seqno = produce_random_seqno(16, 100000);
    b.iter(|| {
        let mut window = SequenceNumberWindow::default();
        for sn in &seqno {
            window.push(*sn);
        }
    })
}
