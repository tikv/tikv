// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

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

/// An auxiliary counter to determine write order. Unlike sequence number, it is
/// guaranteed to be allocated contiguously.
static WRITE_COUNTER_ALLOCATOR: AtomicU64 = AtomicU64::new(0);
/// Everytime active memtable switched, this version should be increased.
static MEMTABLE_VERSION_COUNTER_ALLOCATOR: AtomicU64 = AtomicU64::new(0);
/// Max sequence number that was synced and persisted.
static MAX_SYNCED_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);

pub fn max_synced_sequence_number() -> u64 {
    MAX_SYNCED_SEQUENCE_NUMBER.load(Ordering::SeqCst)
}

pub fn current_memtable_version() -> u64 {
    MEMTABLE_VERSION_COUNTER_ALLOCATOR.load(Ordering::SeqCst)
}

pub trait MemtableEventNotifier: Send {
    fn notify_memtable_sealed(&self, seqno: u64);
    fn notify_memtable_flushed(&self, seqno: u64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SequenceNumber {
    number: u64,
    // Version is actually the counter of memtables flushed. Once the version increased, indicates
    // a memtable was flushed, then relations of a region buffered in memory could be merged into
    // one and persisted into raftdb.
    memtable_version: u64,
    // start_counter is an identity of a write. It's used to check if all seqno of writes were
    // received in the receiving end.
    start_counter: u64,
    // end_counter is the value of sequence number counter after a write. It's used for finding
    // corresponding seqno of a counter. The corresponding seqno may be smaller or equal to the
    // lastest seqno at the time of end_counter generated.
    end_counter: u64,
}

impl SequenceNumber {
    pub fn pre_write() -> Self {
        SequenceNumber {
            number: 0,
            start_counter: WRITE_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst) + 1,
            end_counter: 0,
            memtable_version: 0,
        }
    }

    pub fn post_write(&mut self, number: u64) {
        self.number = number;
        self.end_counter = WRITE_COUNTER_ALLOCATOR.load(Ordering::SeqCst);
    }

    pub fn max(left: Self, right: Self) -> Self {
        cmp::max_by_key(left, right, |s| s.number)
    }

    pub fn get_number(&self) -> u64 {
        self.number
    }

    pub fn get_version(&self) -> u64 {
        self.memtable_version
    }
}

/// Receive all seqno and their counters, check the last committed seqno (a
/// seqno is considered committed if all `start_counter` before its
/// `end_counter` was received), and return the largest sequence number
/// received.
#[derive(Default)]
pub struct SequenceNumberWindow {
    // Status of writes with start_counter starting from ack_start_counter+1.
    write_status_window: VecDeque<bool>,
    // writes with start_counter <= ack_start_counter are all committed.
    ack_start_counter: u64,
    // (end_counter, sequence number)
    pending_sequence: BTreeMap<u64, SequenceNumber>,
    // max corresponding sequence number before ack_start_counter.
    committed_seqno: u64,
    max_received_seqno: u64,
}

impl SequenceNumberWindow {
    pub fn push(&mut self, sn: SequenceNumber) {
        // start_delta - 1 is the index of `write_status_window`.
        let start_delta = match sn.start_counter.checked_sub(self.ack_start_counter) {
            Some(delta) if delta > 0 => delta as usize,
            _ => {
                assert!(sn.number <= self.max_received_seqno);
                return;
            }
        };
        self.max_received_seqno = u64::max(sn.number, self.max_received_seqno);
        // Increase the length of `write_status_window`
        if start_delta > self.write_status_window.len() {
            self.write_status_window.resize(start_delta, false);
        }
        // Insert the seqno of `pending_sequence`. Because an `end_counter`
        // may correspond to multiple seqno, we only keep the max seqno.
        self.pending_sequence
            .entry(sn.end_counter)
            .and_modify(|value| {
                *value = SequenceNumber::max(*value, sn);
            })
            .or_insert(sn);
        self.write_status_window[start_delta - 1] = true;
        if start_delta != 1 {
            return;
        }
        // Commit seqno of the counter which all smaller counter were received.
        let mut acks = 0;
        for received in self.write_status_window.iter() {
            if *received {
                acks += 1;
            } else {
                break;
            }
        }
        self.write_status_window.drain(..acks);
        self.ack_start_counter += acks as u64;
        let mut sequences = self
            .pending_sequence
            .split_off(&(self.ack_start_counter + 1));
        std::mem::swap(&mut sequences, &mut self.pending_sequence);
        if let Some(sequence) = sequences.values().max() {
            assert!(
                self.committed_seqno <= sequence.number,
                "committed_seqno {}, seqno{}",
                self.committed_seqno,
                sequence.number
            );
            self.committed_seqno = sequence.number;
        }
    }

    pub fn committed_seqno(&self) -> u64 {
        self.committed_seqno
    }

    pub fn pending_count(&self) -> usize {
        self.write_status_window.len()
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
        let mut sn1 = SequenceNumber::pre_write();
        sn1.post_write(1);
        window.push(sn1);
        assert_eq!(window.committed_seqno(), 1);
        let mut sn2 = SequenceNumber::pre_write();
        let mut sn3 = SequenceNumber::pre_write();
        let mut sn4 = SequenceNumber::pre_write();
        let mut sn5 = SequenceNumber::pre_write();
        sn5.post_write(3);
        sn2.post_write(5);
        sn3.post_write(2);
        sn4.post_write(4);
        window.push(sn2);
        assert_eq!(window.committed_seqno(), 1);
        window.push(sn5);
        assert_eq!(window.committed_seqno(), 1);
        window.push(sn3);
        assert_eq!(window.committed_seqno(), 1);
        window.push(sn4);
        assert_eq!(window.committed_seqno(), 5);
        let mut sn6 = SequenceNumber::pre_write();
        let mut sn7 = SequenceNumber::pre_write();
        sn6.post_write(7);
        sn7.post_write(6);
        let mut sn8 = SequenceNumber::pre_write();
        sn8.post_write(8);
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
}
