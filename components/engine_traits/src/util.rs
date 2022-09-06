// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use lazy_static::lazy_static;

use super::{Error, Result};

/// Check if key in range [`start_key`, `end_key`).
#[allow(dead_code)]
pub fn check_key_in_range(
    key: &[u8],
    region_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<()> {
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::NotInRange {
            key: key.to_vec(),
            region_id,
            start: start_key.to_vec(),
            end: end_key.to_vec(),
        })
    }
}

lazy_static! {
    static ref SEQUENCE_NUMBER_COUNTER_ALLOCATOR: AtomicU64 = AtomicU64::new(0);
    // Everytime active memtable switched, this version should be increased.
    pub static ref VERSION_COUNTER_ALLOCATOR: AtomicU64 = AtomicU64::new(0);
    // Max sequence number that was synced and persisted.
    pub static ref SYNCED_MAX_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);
}

pub trait Notifier: Send {
    fn notify_memtable_sealed(&self, seqno: u64);
    fn notify_memtable_flushed(&self, seqno: u64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SequenceNumber {
    number: u64,
    // Version is actually the counter of memtables flushed. Once the version increased, indicates
    // a memtable was flushed, then relations of a region buffered in memory could be merged into
    // one and persisted into raftdb.
    version: u64,
    // start_counter is an identity of a write. It's used to check if all seqno of writes were
    // received in the receiving end.
    start_counter: u64,
    // end_counter is the value of sequence number counter after a write. It's used for finding
    // corresponding seqno of a counter. The corresponding seqno may be smaller or equal to the
    // lastest seqno at the time of end_counter generated.
    end_counter: u64,
}

impl SequenceNumber {
    pub fn start() -> Self {
        SequenceNumber {
            number: 0,
            start_counter: SEQUENCE_NUMBER_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst) + 1,
            end_counter: 0,
            version: 0,
        }
    }

    pub fn end(&mut self, number: u64) {
        self.number = number;
        self.end_counter = SEQUENCE_NUMBER_COUNTER_ALLOCATOR.load(Ordering::SeqCst);
    }

    pub fn max(left: Self, right: Self) -> Self {
        match left.number.cmp(&right.number) {
            cmp::Ordering::Less | cmp::Ordering::Equal => right,
            cmp::Ordering::Greater => left,
        }
    }

    pub fn get_number(&self) -> u64 {
        self.number
    }

    pub fn get_version(&self) -> u64 {
        self.version
    }
}

// Receive all seqno and their counters, check the last committed seqno (a seqno
// is considered committed if all seqno before it was received), and return the
// largest sequence number received.
#[derive(Default)]
pub struct SequenceNumberWindow {
    // The sequence number doesn't be received in order, we need a buffer to
    // store received start counters which are bigger than ack_counter.
    pending_start_counter: VecDeque<bool>,
    // counter start from 1, so ack_counter as 0 means no start counter received.
    ack_counter: u64,
    // (end_counter, sequence number)
    pending_sequence: BTreeMap<u64, SequenceNumber>,
    // max corresponding sequence number before ack_counter.
    committed_seqno: u64,
    max_received_seqno: u64,
}

impl SequenceNumberWindow {
    pub fn push(&mut self, sn: SequenceNumber) {
        // start_delta - 1 is the index of `pending_start_counter`.
        let start_delta = match sn.start_counter.checked_sub(self.ack_counter) {
            Some(delta) if delta > 0 => delta as usize,
            _ => {
                assert!(sn.number <= self.max_received_seqno);
                return;
            }
        };
        self.max_received_seqno = u64::max(sn.number, self.max_received_seqno);
        // Increase the length of `pending_start_counter`
        if start_delta > self.pending_start_counter.len() {
            self.pending_start_counter.resize(start_delta, false);
        }
        // Insert the seqno of `pending_sequence`. Because an `end_counter`
        // may correspond to multiple seqno, we only keep the max seqno.
        self.pending_sequence
            .entry(sn.end_counter)
            .and_modify(|value| {
                *value = SequenceNumber::max(*value, sn);
            })
            .or_insert(sn);
        self.pending_start_counter[start_delta - 1] = true;
        // Commit seqno of the counter which all smaller counter were received.
        let mut acks = 0;
        for received in self.pending_start_counter.iter() {
            if *received {
                acks += 1;
            } else {
                break;
            }
        }
        self.pending_start_counter.drain(..acks);
        self.ack_counter += acks as u64;
        let mut sequences = self.pending_sequence.split_off(&(self.ack_counter + 1));
        std::mem::swap(&mut sequences, &mut self.pending_sequence);
        if let Some(sequence) = sequences.values().max() {
            self.committed_seqno = sequence.number;
        }
    }

    pub fn committed_seqno(&self) -> u64 {
        self.committed_seqno
    }

    pub fn pending_count(&self) -> usize {
        self.pending_start_counter.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test::Bencher;

    use super::*;

    #[test]
    fn test_sequence_number_window() {
        let mut window = SequenceNumberWindow::default();
        let mut sn1 = SequenceNumber::start();
        sn1.end(1);
        window.push(sn1);
        assert_eq!(window.committed_seqno(), 1);
        let mut sn2 = SequenceNumber::start();
        let mut sn3 = SequenceNumber::start();
        let mut sn4 = SequenceNumber::start();
        let mut sn5 = SequenceNumber::start();
        sn5.end(3);
        sn2.end(5);
        sn3.end(2);
        sn4.end(4);
        window.push(sn2);
        assert_eq!(window.committed_seqno(), 1);
        window.push(sn5);
        assert_eq!(window.committed_seqno(), 1);
        window.push(sn3);
        assert_eq!(window.committed_seqno(), 1);
        window.push(sn4);
        assert_eq!(window.committed_seqno(), 5);
        let mut sn6 = SequenceNumber::start();
        let mut sn7 = SequenceNumber::start();
        sn6.end(7);
        sn7.end(6);
        let mut sn8 = SequenceNumber::start();
        sn8.end(8);
        window.push(sn6);
        assert_eq!(window.committed_seqno(), 5);
        window.push(sn7);
        assert_eq!(window.committed_seqno(), 7);
        window.push(sn8);
        assert_eq!(window.committed_seqno(), 8);
    }

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
                            let mut sn = SequenceNumber::start();
                            sn.end(allocator.fetch_add(1, Ordering::AcqRel));
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
}
