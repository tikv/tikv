// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
    sync::Arc,
};

use tikv_util::buffer_vec::BufferVec;

/// `Set` stores set.
///
/// Inside `ChunkedVecSet`:
/// - `data` stores the real set data.
/// - `value` is a bitmap for set data
///
/// Take `data` = 'ab' as an example:
///
/// Set('a','b') -> 11B
/// Set('a')     -> 01B
/// Set('')      -> 00B
#[derive(Clone, Debug)]
pub struct Set {
    data: Arc<BufferVec>,

    // TIDB makes sure there will be no more than 64 bits
    // https://github.com/pingcap/tidb/blob/master/types/set.go
    value: u64,
}

impl Set {
    pub fn new(data: Arc<BufferVec>, value: u64) -> Self {
        Self { data, value }
    }
    pub fn value(&self) -> u64 {
        self.value
    }
    pub fn as_ref(&self) -> SetRef<'_> {
        SetRef {
            data: &self.data,
            value: self.value,
        }
    }
}

impl Display for Set {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Eq for Set {}

impl PartialEq for Set {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for Set {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for Set {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl crate::codec::data_type::AsMySQLBool for Set {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::codec::Result<bool> {
        Ok(self.value > 0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SetRef<'a> {
    data: &'a BufferVec,
    value: u64,
}

impl<'a> SetRef<'a> {
    pub fn new(data: &'a BufferVec, value: u64) -> Self {
        Self { data, value }
    }
    pub fn to_owned(self) -> Set {
        Set {
            data: Arc::new(self.data.clone()),
            value: self.value,
        }
    }
    pub fn is_set(&self, idx: usize) -> bool {
        self.value & (1 << idx) != 0
    }
    pub fn is_empty(&self) -> bool {
        self.value == 0
    }
    pub fn value(&self) -> u64 {
        self.value
    }
}

impl<'a> Display for SetRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buf: Vec<u8> = Vec::new();
        if self.value > 0 {
            for idx in 0..self.data.len() {
                if !self.is_set(idx) {
                    continue;
                }

                if !buf.is_empty() {
                    buf.push(b',');
                }
                buf.extend_from_slice(&self.data[idx]);
            }
        }

        // TODO: Check the requirements and intentions of to_string usage.
        write!(f, "{}", String::from_utf8_lossy(buf.as_slice()))
    }
}

impl<'a> Eq for SetRef<'a> {}

impl<'a> PartialEq for SetRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<'a> Ord for SetRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<'a> PartialOrd for SetRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![
            (vec!["a", "b", "c"], 0b001, "a"),
            (vec!["a", "b", "c"], 0b011, "a,b"),
            (vec!["a", "b", "c"], 0b111, "a,b,c"),
            (vec!["a", "b", "c"], 0b101, "a,c"),
        ];

        for (data, value, expect) in cases {
            let mut buf = BufferVec::new();
            for v in data {
                buf.push(v)
            }

            let s = Set {
                data: Arc::new(buf),
                value,
            };

            assert_eq!(s.as_ref().to_string(), expect.to_string())
        }
    }

    #[test]
    fn test_is_set() {
        let mut buf = BufferVec::new();
        for v in &["a", "b", "c"] {
            buf.push(v)
        }

        let s = Set {
            data: Arc::new(buf),
            value: 0b101,
        };

        assert!(s.as_ref().is_set(0));
        assert!(!s.as_ref().is_set(1));
        assert!(s.as_ref().is_set(2));
    }

    #[test]
    fn test_is_empty() {
        let mut buf = BufferVec::new();
        for v in &["a", "b", "c"] {
            buf.push(v)
        }

        let s = Set {
            data: Arc::new(buf),
            value: 0b101,
        };

        assert!(!s.as_ref().is_empty());

        let s = Set {
            data: s.data,
            value: 0b000,
        };

        assert!(s.as_ref().is_empty());
    }
}
