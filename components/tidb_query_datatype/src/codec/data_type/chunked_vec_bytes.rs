// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::bit_vec::BitVec;
use super::{Bytes, BytesRef};
use super::{ChunkRef, ChunkedVec, UnsafeRefInto};
use crate::impl_chunked_vec_common;

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecBytes {
    data: Vec<u8>,
    bitmap: BitVec,
    length: usize,
    var_offset: Vec<usize>,
}

/// A vector storing `Option<Bytes>` with a compact layout.
///
/// Inside `ChunkedVecBytes`, `bitmap` indicates if an element at given index is null,
/// and `data` stores actual data. Bytes data are stored adjacent to each other in
/// `data`. If element at a given index is null, then it takes no space in `data`.
/// Otherwise, contents of the `Bytes` are stored, and `var_offset` indicates the starting
/// position of each element.
impl ChunkedVecBytes {
    impl_chunked_vec_common! { Bytes }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    pub fn to_vec(&self) -> Vec<Option<Bytes>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn push_data(&mut self, mut value: Bytes) {
        self.bitmap.push(true);
        self.data.append(&mut value);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push_data_ref(&mut self, value: BytesRef) {
        self.bitmap.push(true);
        self.data.extend_from_slice(value);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push_ref(&mut self, value: Option<BytesRef>) {
        if let Some(x) = value {
            self.push_data_ref(x);
        } else {
            self.push_null();
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.data.truncate(self.var_offset[len]);
            self.bitmap.truncate(len);
            self.var_offset.truncate(len + 1);
            self.length = len;
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity().max(self.length)
    }

    pub fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data);
        self.bitmap.append(&mut other.bitmap);
        let var_offset_last = *self.var_offset.last().unwrap();
        for i in 1..other.var_offset.len() {
            self.var_offset.push(other.var_offset[i] + var_offset_last);
        }
        self.length += other.length;
        other.var_offset = vec![0];
        other.length = 0;
    }

    pub fn get(&self, idx: usize) -> Option<BytesRef> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            Some(&self.data[self.var_offset[idx]..self.var_offset[idx + 1]])
        } else {
            None
        }
    }
}

impl ChunkedVec<Bytes> for ChunkedVecBytes {
    fn chunked_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }
    fn chunked_push(&mut self, value: Option<Bytes>) {
        self.push(value)
    }
}

impl<'a> ChunkRef<'a, BytesRef<'a>> for &'a ChunkedVecBytes {
    fn get_option_ref(self, idx: usize) -> Option<BytesRef<'a>> {
        self.get(idx)
    }

    fn phantom_data(self) -> Option<BytesRef<'a>> {
        None
    }
}

impl Into<ChunkedVecBytes> for Vec<Option<Bytes>> {
    fn into(self) -> ChunkedVecBytes {
        ChunkedVecBytes::from_vec(self)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecBytes> for &'a ChunkedVecBytes {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecBytes {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_slice_vec() {
        let test_bytes: &[Option<Bytes>] = &[
            None,
            Some("我好菜啊".as_bytes().to_vec()),
            None,
            Some("我菜爆了".as_bytes().to_vec()),
            Some("我失败了".as_bytes().to_vec()),
            None,
            Some("💩".as_bytes().to_vec()),
            None,
        ];
        assert_eq!(ChunkedVecBytes::from_slice(test_bytes).to_vec(), test_bytes);
        assert_eq!(
            ChunkedVecBytes::from_slice(&test_bytes.to_vec()).to_vec(),
            test_bytes
        );
    }

    #[test]
    fn test_basics() {
        let mut x: ChunkedVecBytes = ChunkedVecBytes::with_capacity(0);
        x.push(None);
        x.push(Some("我好菜啊".as_bytes().to_vec()));
        x.push(None);
        x.push(Some("我菜爆了".as_bytes().to_vec()));
        x.push(Some("我失败了".as_bytes().to_vec()));
        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some("我好菜啊".as_bytes()));
        assert_eq!(x.get(2), None);
        assert_eq!(x.get(3), Some("我菜爆了".as_bytes()));
        assert_eq!(x.get(4), Some("我失败了".as_bytes()));
        assert_eq!(x.len(), 5);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let test_bytes: &[Option<Bytes>] = &[
            None,
            None,
            Some("我好菜啊".as_bytes().to_vec()),
            None,
            Some("我菜爆了".as_bytes().to_vec()),
            Some("我失败了".as_bytes().to_vec()),
            None,
            Some("💩".as_bytes().to_vec()),
            None,
        ];
        let mut chunked_vec = ChunkedVecBytes::from_slice(test_bytes);
        chunked_vec.truncate(100);
        assert_eq!(chunked_vec.len(), 9);
        chunked_vec.truncate(3);
        assert_eq!(chunked_vec.len(), 3);
        assert_eq!(chunked_vec.get(0), None);
        assert_eq!(chunked_vec.get(1), None);
        assert_eq!(chunked_vec.get(2), Some("我好菜啊".as_bytes()));
        chunked_vec.truncate(2);
        assert_eq!(chunked_vec.len(), 2);
        assert_eq!(chunked_vec.get(0), None);
        assert_eq!(chunked_vec.get(1), None);
        chunked_vec.truncate(1);
        assert_eq!(chunked_vec.len(), 1);
        assert_eq!(chunked_vec.get(0), None);
        chunked_vec.truncate(0);
        assert_eq!(chunked_vec.len(), 0);
    }

    #[test]
    fn test_append() {
        let test_bytes_1: &[Option<Bytes>] =
            &[None, None, Some("我好菜啊".as_bytes().to_vec()), None];
        let test_bytes_2: &[Option<Bytes>] = &[
            None,
            Some("我菜爆了".as_bytes().to_vec()),
            Some("我失败了".as_bytes().to_vec()),
            None,
            Some("💩".as_bytes().to_vec()),
            None,
        ];
        let mut chunked_vec_1 = ChunkedVecBytes::from_slice(test_bytes_1);
        let mut chunked_vec_2 = ChunkedVecBytes::from_slice(test_bytes_2);
        chunked_vec_1.append(&mut chunked_vec_2);
        assert_eq!(chunked_vec_1.len(), 10);
        assert!(chunked_vec_2.is_empty());
        assert_eq!(
            chunked_vec_1.to_vec(),
            &[
                None,
                None,
                Some("我好菜啊".as_bytes().to_vec()),
                None,
                None,
                Some("我菜爆了".as_bytes().to_vec()),
                Some("我失败了".as_bytes().to_vec()),
                None,
                Some("💩".as_bytes().to_vec()),
                None,
            ]
        );
    }
}
