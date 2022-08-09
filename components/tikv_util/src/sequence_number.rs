use std::{
    cmp,
    collections::{BTreeMap, VecDeque},
    iter::FromIterator,
    sync::atomic::{AtomicU64, Ordering},
};

use collections::HashMap;
use lazy_static::lazy_static;

use crate::cf_defs::DATA_CFS;

lazy_static! {
    static ref SEQUENCE_NUMBER_COUNTER_ALLOCATOR: AtomicU64 = AtomicU64::new(0);
    pub static ref VERSION_COUNTER_ALLOCATOR: AtomicU64 = AtomicU64::new(0);
    pub static ref SYNCED_MAX_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);
    pub static ref FLUSHED_MAX_SEQUENCE_NUMBERS: HashMap<&'static str, AtomicU64> =
        HashMap::from_iter(DATA_CFS.iter().map(|cf| (*cf, AtomicU64::new(0))));
}

pub trait Notifier: Send {
    fn notify_memtable_sealed(&self, seqno: u64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SequenceNumber {
    pub number: u64,
    pub version: u64,
    start_counter: u64,
    end_counter: u64,
}

impl SequenceNumber {
    pub fn start() -> Self {
        SequenceNumber {
            number: 0,
            start_counter: SEQUENCE_NUMBER_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst) + 1,
            end_counter: 0,
            // start_version: VERSION_COUNTER_ALLOCATOR.load(Ordering::SeqCst),
            version: 0,
        }
    }

    pub fn end(&mut self, number: u64) {
        self.number = number;
        self.end_counter = SEQUENCE_NUMBER_COUNTER_ALLOCATOR.load(Ordering::SeqCst);
        // self.end_version = VERSION_COUNTER_ALLOCATOR.load(Ordering::SeqCst);
    }

    pub fn max(left: Self, right: Self) -> Self {
        match left.number.cmp(&right.number) {
            cmp::Ordering::Less | cmp::Ordering::Equal => right,
            cmp::Ordering::Greater => left,
        }
    }
}

#[derive(Default)]
pub struct SequenceNumberWindow {
    // The sequence number doesn't be received in order, we need a ordered set to
    // store received start counters which are bigger than last_start_counter + 1.
    pending_start_counter: VecDeque<bool>,
    // counter start from 1, so 0 means no start counter received.
    ack_counter: u64,
    // (end_counter, sequence number)
    pending_sequence: BTreeMap<u64, SequenceNumber>,
    committed_seqno: u64,
}

impl SequenceNumberWindow {
    pub fn push(&mut self, sn: SequenceNumber) {
        let start_delta = sn.start_counter.checked_sub(self.ack_counter).unwrap() as usize;
        if start_delta > self.pending_start_counter.len() {
            self.pending_start_counter.resize(start_delta, false);
        }
        self.pending_sequence
            .entry(sn.end_counter)
            .and_modify(|value| {
                *value = SequenceNumber::max(*value, sn);
            })
            .or_insert(sn);
        self.pending_start_counter[start_delta - 1] = true;
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
}

#[cfg(test)]
mod tests {
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
    }
}
