use std::{
    cmp,
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use lazy_static::lazy_static;

lazy_static! {
    pub static ref SEQUENCE_NUMBER_COUNTER_ALLOCATOR: AtomicUsize = AtomicUsize::new(1);
    pub static ref SYNCED_MAX_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Copy)]
pub struct SequenceNumber {
    pub sequence: u64,
    start_counter: usize,
    end_counter: usize,
}

impl SequenceNumber {
    pub fn start() -> Self {
        SequenceNumber {
            sequence: 0,
            start_counter: SEQUENCE_NUMBER_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst),
            end_counter: 0,
        }
    }

    pub fn end(&mut self, number: u64) {
        self.sequence = number;
        self.end_counter = SEQUENCE_NUMBER_COUNTER_ALLOCATOR.load(Ordering::SeqCst);
    }

    pub fn max(left: Self, right: Self) -> Self {
        match left.sequence.cmp(&right.sequence) {
            cmp::Ordering::Less | cmp::Ordering::Equal => right,
            cmp::Ordering::Greater => left,
        }
    }
}

#[derive(Default)]
pub struct SequenceNumberWindow {
    // The sequence number doesn't be received in order, we need a ordered set to
    // store received start counters which are bigger than last_start_counter + 1.
    pending_start_counter: BTreeSet<usize>,
    // end_counter => (sequence number, region id)
    pending_sequence: BTreeMap<usize, SequenceNumber>,
    last_start_counter: usize,
    last_sequence: Option<SequenceNumber>,
}

impl SequenceNumberWindow {
    pub fn push(&mut self, sn: SequenceNumber) -> Option<SequenceNumber> {
        if sn.start_counter == self.last_start_counter + 1 {
            self.last_start_counter += 1;
            while let Some(start_counter) = self.pending_start_counter.first().copied() {
                if start_counter == self.last_start_counter + 1 {
                    self.last_start_counter += 1;
                    self.pending_start_counter.remove(&start_counter);
                } else {
                    break;
                }
            }

            while let Some((end_counter, sequence)) = self
                .pending_sequence
                .first_key_value()
                .map(|(k, v)| (*k, *v))
            {
                if end_counter <= self.last_start_counter {
                    match self.last_sequence {
                        Some(last_sequence) => {
                            self.last_sequence = Some(SequenceNumber::max(last_sequence, sequence));
                        }
                        None => self.last_sequence = Some(sequence),
                    }
                    self.pending_sequence.remove(&end_counter);
                } else {
                    break;
                }
            }
            self.last_sequence
        } else {
            assert!(self.pending_start_counter.insert(sn.start_counter));
            self.pending_sequence.insert(sn.end_counter, sn).unwrap();
            None
        }
    }
}
