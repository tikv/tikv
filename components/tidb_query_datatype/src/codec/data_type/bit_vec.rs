// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

/// A boolean vector, which consolidates 64 booleans into 1 u64 to save space.
///
/// `BitVec` is mainly used to implement bitmap in ChunkedVec.
#[derive(Debug, PartialEq, Clone)]
pub struct BitVec {
    data: Vec<u64>,
    length: usize,
}
const BITS: usize = 64;
impl BitVec {
    fn upper_bound(size: usize) -> usize {
        (size + BITS - 1) >> 6
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(Self::upper_bound(capacity)),
            length: 0,
        }
    }

    #[inline]
    pub fn push(&mut self, value: bool) {
        let idx = self.length >> 6;
        if idx >= self.data.len() {
            self.data.push(0);
        }

        let mask = (1_u64) << (self.length & (BITS - 1));
        self.length += 1;
        if value {
            self.data[idx] |= mask;
        } else {
            self.data[idx] &= !mask;
        }
    }

    pub fn replace(&mut self, idx: usize, value: bool) {
        assert!(idx < self.length);
        let mask = (1_u64) << (idx & (BITS - 1));
        let pos = idx >> 6;
        if value {
            self.data[pos] |= mask;
        } else {
            self.data[pos] &= !mask;
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.length {
            self.length = len;
            self.data.truncate(Self::upper_bound(len));
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len() << 6
    }

    pub fn append(&mut self, other: &mut Self) {
        for i in 0..other.len() {
            self.push(other.get(i));
        }
        other.truncate(0);
    }

    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        assert!(idx < self.length);
        let mask = (1_u64) << (idx & (BITS - 1));
        let pos = idx >> 6;
        (self.data[pos] & mask) != 0
    }
}

pub struct BitAndIterator<'a> {
    vecs: &'a [&'a BitVec],
    or: u64,
    cnt: usize,
    output_rows: usize,
}

impl<'a> BitAndIterator<'a> {
    pub fn new(vecs: &'a [&'a BitVec], output_rows: usize) -> Self {
        for i in vecs {
            if i.len() != output_rows {
                panic!("column length doesn't match");
            }
        }
        Self {
            vecs,
            or: 0,
            cnt: 0,
            output_rows,
        }
    }
}

impl<'a> Iterator for BitAndIterator<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cnt == self.output_rows {
            return None;
        }
        if self.cnt % BITS == 0 {
            let mut result: u64 = 0xffffffffffffffff;
            let idx = self.cnt / BITS;
            for i in self.vecs {
                result &= i.data[idx];
            }
            self.or = result;
        }
        self.cnt += 1;
        let val = self.or & 0x1 == 1;
        self.or >>= 1;
        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_capacity() {
        BitVec::with_capacity(0);
        BitVec::with_capacity(8);
        BitVec::with_capacity(233);
    }

    #[test]
    fn test_len_is_empty() {
        assert_eq!(BitVec::with_capacity(0).len(), 0);
        assert_eq!(BitVec::with_capacity(8).len(), 0);
        assert_eq!(BitVec::with_capacity(233).len(), 0);
        assert!(BitVec::with_capacity(0).is_empty());
        assert!(BitVec::with_capacity(8).is_empty());
        assert!(BitVec::with_capacity(233).is_empty());
        let mut x = BitVec::with_capacity(233);
        x.push(false);
        assert_eq!(x.len(), 1);
        assert!(!x.is_empty());
        x.push(true);
        assert_eq!(x.len(), 2);
        assert!(!x.is_empty());
        for _ in 0..2333 {
            x.push(false);
        }
        assert_eq!(x.len(), 2 + 2333);
        assert!(!x.is_empty());
        for i in 0..2333 {
            x.replace(i, true);
        }
        assert_eq!(x.len(), 2 + 2333);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_push() {
        let mut x = BitVec::with_capacity(0);
        x.push(false);
        x.push(true);
        assert_eq!(x.get(0), false);
        assert_eq!(x.get(1), true);
    }

    #[test]
    fn test_push_all_combinations() {
        let mut x = BitVec::with_capacity(0);
        for i in 0..256 {
            for bit in 0..8 {
                x.push(i & (1 << bit) != 0);
            }
            for bit in 0..8 {
                assert_eq!(x.get(i * 8 + bit), i & (1 << bit) != 0);
            }
        }
    }

    #[test]
    fn test_push_on_edge() {
        let mut x = BitVec::with_capacity(0);
        let mut base = 0;
        for _ in 0..8 {
            for i in 0..256 {
                for bit in 0..8 {
                    x.push(i & (1 << bit) != 0);
                }
                for bit in 0..8 {
                    assert_eq!(x.get(base + i * 8 + bit), i & (1 << bit) != 0);
                }
            }
            x.push(false); // try to mis-align boolean values on 8 bound
            base += 256 * 8 + 1;
        }
    }

    #[test]
    fn test_replace() {
        let mut x = BitVec::with_capacity(0);
        x.push(false);
        x.push(true);
        assert!(!x.get(0));
        assert!(x.get(1));
        x.replace(0, true);
        assert!(x.get(0));
        assert!(x.get(1));
        x.replace(0, false);
        assert!(!x.get(0));
        assert!(x.get(1));
    }

    #[test]
    fn test_replace_all_combinations() {
        let mut x = BitVec::with_capacity(0);
        for i in 0..256 {
            for bit in 0..8 {
                x.push(i & (1 << bit) != 0);
            }
            x.replace(i * 8, false);
            for bit in 1..8 {
                assert_eq!(x.get(i * 8 + bit), i & (1 << bit) != 0);
            }
            assert!(!x.get(i * 8));
        }
    }

    #[test]
    fn test_append() {
        let mut x = BitVec::with_capacity(0);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        let mut y = BitVec::with_capacity(0);
        y.push(false);
        y.push(true);
        y.push(false);
        y.push(true);
        y.push(false);
        y.push(true);
        y.push(false);
        y.push(true);
        y.push(false);
        x.append(&mut y);
        for i in 0..16 {
            assert_eq!(x.get(i), i % 2 == 0);
        }
        assert_eq!(x.len(), 16);
        assert_eq!(y.len(), 0);
    }

    #[test]
    fn test_truncate() {
        let mut x = BitVec::with_capacity(0);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.truncate(4);
        assert_eq!(x.len(), 4);
        x.truncate(100);
        assert_eq!(x.len(), 4);
        x.truncate(0);
        assert_eq!(x.len(), 0);
    }

    #[test]
    fn test_bit_and_iterator_empty() {
        let mut cnt = 0;
        for i in BitAndIterator::new(&[], 2333) {
            assert!(i);
            cnt += 1;
        }
        assert_eq!(cnt, 2333);
    }

    #[test]
    fn test_bit_and_iterator() {
        let mut cnt = 0;
        let mut vec1 = BitVec::with_capacity(0);
        let mut vec2 = BitVec::with_capacity(0);
        let mut vec3 = BitVec::with_capacity(0);
        let mut vec5 = BitVec::with_capacity(0);
        let size = 2333;
        for i in 0..size {
            vec1.push(true);
            vec2.push(i % 2 == 0);
            vec3.push(i % 3 == 0);
            vec5.push(i % 5 == 0);
        }
        for (idx, i) in BitAndIterator::new(&[&vec1, &vec2, &vec3, &vec5], size).enumerate() {
            assert_eq!(i, idx % 30 == 0);
            cnt += 1;
        }
        assert_eq!(cnt, size);
    }
}
