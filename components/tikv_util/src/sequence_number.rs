use std::{
    cmp,
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use lazy_static::lazy_static;

lazy_static! {
    pub static ref SEQUENCE_NUMBER_COUNTER_ALLOCATOR: AtomicUsize = AtomicUsize::new(0);
    pub static ref SYNCED_MAX_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SequenceNumber {
    pub seqno: u64,
    start_counter: usize,
    end_counter: usize,
}

impl SequenceNumber {
    pub fn start() -> Self {
        SequenceNumber {
            seqno: 0,
            start_counter: SEQUENCE_NUMBER_COUNTER_ALLOCATOR.fetch_add(1, Ordering::SeqCst) + 1,
            end_counter: 0,
        }
    }

    pub fn end(&mut self, number: u64) {
        self.seqno = number;
        self.end_counter = SEQUENCE_NUMBER_COUNTER_ALLOCATOR.load(Ordering::SeqCst);
    }

    pub fn max(left: Self, right: Self) -> Self {
        match left.seqno.cmp(&right.seqno) {
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
    last_ack_counter: usize,
    // (end_counter, sequence number)
    pending_sequence: BTreeMap<usize, SequenceNumber>,
    last_sequence: Option<SequenceNumber>,
}

impl SequenceNumberWindow {
    pub fn push(&mut self, sn: SequenceNumber) -> Option<SequenceNumber> {
        let start_delta = sn.start_counter.checked_sub(self.last_ack_counter).unwrap();
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
        if start_delta == 1 {
            let mut acks = 0;
            for received in self.pending_start_counter.iter() {
                if *received {
                    acks += 1;
                } else {
                    break;
                }
            }
            self.pending_start_counter.drain(0..acks);
            self.last_ack_counter += acks;
            let mut sequences = self
                .pending_sequence
                .split_off(&(self.last_ack_counter + 1));
            std::mem::swap(&mut sequences, &mut self.pending_sequence);
            if sequences.is_empty() {
                return None;
            }
            for (_, pending) in sequences {
                self.last_sequence = self
                    .last_sequence
                    .map(|sn| SequenceNumber::max(sn, pending))
                    .or(Some(pending));
            }
            self.last_sequence
        } else {
            None
        }
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
        assert_eq!(
            window.push(sn1),
            Some(SequenceNumber {
                seqno: 1,
                start_counter: 1,
                end_counter: 1,
            })
        );
        let mut sn2 = SequenceNumber::start();
        let mut sn3 = SequenceNumber::start();
        let mut sn4 = SequenceNumber::start();
        sn2.end(4);
        sn3.end(3);
        sn4.end(2);
        assert_eq!(window.push(sn2), None);
        assert_eq!(window.push(sn3), None);
        assert_eq!(
            window.push(sn4),
            Some(SequenceNumber {
                seqno: 4,
                start_counter: 2,
                end_counter: 4,
            })
        );
        let mut sn5 = SequenceNumber::start();
        sn5.end(10);
        assert_eq!(
            window.push(sn5),
            Some(SequenceNumber {
                seqno: 10,
                start_counter: 5,
                end_counter: 5,
            })
        );
    }
}
