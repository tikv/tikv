// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

pub trait KeyComparator: Clone {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct ByteWiseComparator {}

impl KeyComparator for ByteWiseComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs.cmp(rhs) == Ordering::Equal
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FixedLengthSuffixComparator {
    len: usize,
}

impl FixedLengthSuffixComparator {
    pub const fn new(len: usize) -> FixedLengthSuffixComparator {
        FixedLengthSuffixComparator { len }
    }
}

impl KeyComparator for FixedLengthSuffixComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        if lhs.len() < self.len {
            panic!("cannot compare with suffix {}: {:?}", self.len, lhs,);
        }
        if rhs.len() < self.len {
            panic!("cannot compare with suffix {}: {:?}", self.len, rhs,);
        }
        let (l_p, l_s) = lhs.split_at(lhs.len() - self.len);
        let (r_p, r_s) = rhs.split_at(rhs.len() - self.len);
        let res = l_p.cmp(r_p);
        match res {
            Ordering::Greater | Ordering::Less => res,
            Ordering::Equal => l_s.cmp(r_s),
        }
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        let (l_p, _) = lhs.split_at(lhs.len() - self.len);
        let (r_p, _) = rhs.split_at(rhs.len() - self.len);
        l_p == r_p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_comparator() {
        let suffix_comp = FixedLengthSuffixComparator::new(4);
        let (l, r, s) = ("abcd-0", "abcd-1", "abcd-0");
        assert!(suffix_comp.same_key(l.as_bytes(), r.as_bytes()));
        assert_eq!(
            suffix_comp.compare_key(l.as_bytes(), r.as_bytes()),
            Ordering::Less
        );
        assert_eq!(
            suffix_comp.compare_key(l.as_bytes(), s.as_bytes()),
            Ordering::Equal
        );
    }
}
