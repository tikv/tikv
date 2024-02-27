// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

pub trait KeyComparator: Clone {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool;
}

#[cfg(test)]
#[derive(Default, Debug, Clone, Copy)]
pub struct ByteWiseComparator {}

#[cfg(test)]
impl KeyComparator for ByteWiseComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs.cmp(rhs) == Ordering::Equal
    }
}
