#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedBoolVec {
    data: Vec<u8>,
    length: usize,
}

impl ChunkedBoolVec {
    fn upper_bound(size: usize) -> usize {
        (size + 7) >> 3
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(Self::upper_bound(capacity)),
            length: 0,
        }
    }

    pub fn push(&mut self, value: bool) {
        let idx = self.length >> 3;
        if idx >= self.data.len() {
            self.data.push(0);
        }
        self.length += 1;
        self.replace(self.length - 1, value);
    }

    pub fn replace(&mut self, idx: usize, value: bool) {
        assert!(idx < self.length);
        let mask = (1 << (idx & 7)) as u8;
        let pos = idx >> 3;
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
        self.data.len() << 3
    }

    pub fn append(&mut self, other: &mut Self) {
        for i in 0..other.len() {
            self.push(other.get(i));
        }
        other.truncate(0);
    }

    pub fn get(&self, idx: usize) -> bool {
        assert!(idx < self.length);
        let mask = (1 << (idx & 7)) as u8;
        let pos = idx >> 3;
        (self.data[pos] & mask) != 0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_with_capacity() {
        ChunkedBoolVec::with_capacity(0);
        ChunkedBoolVec::with_capacity(8);
        ChunkedBoolVec::with_capacity(233);
    }

    #[test]
    fn test_len_is_empty() {
        assert_eq!(ChunkedBoolVec::with_capacity(0).len(), 0);
        assert_eq!(ChunkedBoolVec::with_capacity(8).len(), 0);
        assert_eq!(ChunkedBoolVec::with_capacity(233).len(), 0);
        assert!(ChunkedBoolVec::with_capacity(0).is_empty());
        assert!(ChunkedBoolVec::with_capacity(8).is_empty());
        assert!(ChunkedBoolVec::with_capacity(233).is_empty());
        let mut x = ChunkedBoolVec::with_capacity(233);
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
        let mut x = ChunkedBoolVec::with_capacity(0);
        x.push(false);
        x.push(true);
        assert_eq!(x.get(0), false);
        assert_eq!(x.get(1), true);
    }

    #[test]
    fn test_push_all_combinations() {
        let mut x = ChunkedBoolVec::with_capacity(0);
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
        let mut x = ChunkedBoolVec::with_capacity(0);
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
        let mut x = ChunkedBoolVec::with_capacity(0);
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
        let mut x = ChunkedBoolVec::with_capacity(0);
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
        let mut x = ChunkedBoolVec::with_capacity(0);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        let mut y = ChunkedBoolVec::with_capacity(0);
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
        let mut x = ChunkedBoolVec::with_capacity(0);
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
}
