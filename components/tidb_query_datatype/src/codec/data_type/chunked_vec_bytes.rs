// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{bit_vec::BitVec, Bytes, BytesRef, ChunkRef, ChunkedVec, UnsafeRefInto};
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
    #[inline]
    pub fn push_data_ref(&mut self, value: BytesRef<'_>) {
        self.bitmap.push(true);
        self.data.extend_from_slice(value);
        self.finish_append();
    }

    #[inline]
    fn finish_append(&mut self) {
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    #[inline]
    pub fn push_ref(&mut self, value: Option<BytesRef<'_>>) {
        if let Some(x) = value {
            self.push_data_ref(x);
        } else {
            self.push_null();
        }
    }
    #[inline]
    pub fn get(&self, idx: usize) -> Option<BytesRef<'_>> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            Some(&self.data[self.var_offset[idx]..self.var_offset[idx + 1]])
        } else {
            None
        }
    }

    pub fn into_writer(self) -> BytesWriter {
        BytesWriter { chunked_vec: self }
    }
}

impl ChunkedVec<Bytes> for ChunkedVecBytes {
    impl_chunked_vec_common! { Bytes }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    #[inline]
    fn push_data(&mut self, mut value: Bytes) {
        self.bitmap.push(true);
        self.data.append(&mut value);
        self.finish_append();
    }

    #[inline]
    fn push_null(&mut self) {
        self.bitmap.push(false);
        self.finish_append();
    }

    fn len(&self) -> usize {
        self.length
    }

    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.data.truncate(self.var_offset[len]);
            self.bitmap.truncate(len);
            self.var_offset.truncate(len + 1);
            self.length = len;
        }
    }

    fn capacity(&self) -> usize {
        self.data.capacity().max(self.length)
    }

    fn append(&mut self, other: &mut Self) {
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

    fn to_vec(&self) -> Vec<Option<Bytes>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }
}

pub struct BytesWriter {
    chunked_vec: ChunkedVecBytes,
}

pub struct PartialBytesWriter {
    chunked_vec: ChunkedVecBytes,
}

pub struct BytesGuard {
    chunked_vec: ChunkedVecBytes,
}

impl BytesGuard {
    pub fn into_inner(self) -> ChunkedVecBytes {
        self.chunked_vec
    }
}

impl BytesWriter {
    pub fn begin(self) -> PartialBytesWriter {
        PartialBytesWriter {
            chunked_vec: self.chunked_vec,
        }
    }

    pub fn write(mut self, data: Option<Bytes>) -> BytesGuard {
        self.chunked_vec.push(data);
        BytesGuard {
            chunked_vec: self.chunked_vec,
        }
    }

    pub fn write_ref(mut self, data: Option<BytesRef<'_>>) -> BytesGuard {
        self.chunked_vec.push_ref(data);
        BytesGuard {
            chunked_vec: self.chunked_vec,
        }
    }

    pub fn write_from_char_iter(self, iter: impl Iterator<Item = char>) -> BytesGuard {
        let mut writer = self.begin();
        for c in iter {
            let mut buf = [0; 4];
            let result = c.encode_utf8(&mut buf);
            writer.partial_write(result.as_bytes());
        }
        writer.finish()
    }

    pub fn write_from_byte_iter(mut self, iter: impl Iterator<Item = u8>) -> BytesGuard {
        self.chunked_vec.data.extend(iter);
        self.chunked_vec.bitmap.push(true);
        self.chunked_vec.finish_append();
        BytesGuard {
            chunked_vec: self.chunked_vec,
        }
    }
}

impl<'a> PartialBytesWriter {
    pub fn partial_write(&mut self, data: BytesRef<'_>) {
        self.chunked_vec.data.extend_from_slice(data);
    }

    pub fn finish(mut self) -> BytesGuard {
        self.chunked_vec.bitmap.push(true);
        self.chunked_vec.finish_append();
        BytesGuard {
            chunked_vec: self.chunked_vec,
        }
    }
}

impl<'a> ChunkRef<'a, BytesRef<'a>> for &'a ChunkedVecBytes {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<BytesRef<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<BytesRef<'a>> {
        None
    }
}

impl From<Vec<Option<Bytes>>> for ChunkedVecBytes {
    fn from(v: Vec<Option<Bytes>>) -> ChunkedVecBytes {
        ChunkedVecBytes::from_vec(v)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecBytes> for &'a ChunkedVecBytes {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecBytes {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(ChunkedVecBytes::from_slice(test_bytes).to_vec(), test_bytes);
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

    fn repeat(data: Bytes, cnt: usize) -> Bytes {
        let mut x = vec![];
        for _ in 0..cnt {
            x.append(&mut data.clone())
        }
        x
    }

    #[test]
    fn test_writer() {
        let test_bytes: &[Option<Bytes>] = &[
            None,
            None,
            Some(
                "TiDB 是PingCAP 公司自主设计、研发的开源分布式关系型数据库，"
                    .as_bytes()
                    .to_vec(),
            ),
            None,
            Some(
                "是一款同时支持在线事务处理与在线分析处理(HTAP)的融合型分布式数据库产品。"
                    .as_bytes()
                    .to_vec(),
            ),
            Some("🐮🐮🐮🐮🐮".as_bytes().to_vec()),
            Some("我成功了".as_bytes().to_vec()),
            None,
            Some("💩💩💩".as_bytes().to_vec()),
            None,
        ];
        let mut chunked_vec = ChunkedVecBytes::with_capacity(0);
        for test_byte in test_bytes {
            let writer = chunked_vec.into_writer();
            let guard = writer.write(test_byte.to_owned());
            chunked_vec = guard.into_inner();
        }
        assert_eq!(chunked_vec.to_vec(), test_bytes);

        let mut chunked_vec = ChunkedVecBytes::with_capacity(0);
        for test_byte in test_bytes {
            let writer = chunked_vec.into_writer();
            let guard = writer.write(test_byte.clone());
            chunked_vec = guard.into_inner();
        }
        assert_eq!(chunked_vec.to_vec(), test_bytes);

        let mut chunked_vec = ChunkedVecBytes::with_capacity(0);
        for test_byte in test_bytes {
            let writer = chunked_vec.into_writer();
            let guard = match test_byte.clone() {
                Some(x) => {
                    let mut writer = writer.begin();
                    writer.partial_write(x.as_slice());
                    writer.partial_write(x.as_slice());
                    writer.partial_write(x.as_slice());
                    writer.finish()
                }
                None => writer.write(None),
            };
            chunked_vec = guard.into_inner();
        }
        assert_eq!(
            chunked_vec.to_vec(),
            test_bytes
                .iter()
                .map(|x| x.as_ref().map(|x| repeat(x.to_vec(), 3)))
                .collect::<Vec<Option<Bytes>>>()
        );
    }
}

#[cfg(test)]
mod benches {
    use super::*;

    #[bench]
    fn bench_bytes_append(b: &mut test::Bencher) {
        let mut bytes_vec: Vec<u8> = vec![];
        for _i in 0..10 {
            bytes_vec.append(&mut b"2333333333".to_vec());
        }
        b.iter(|| {
            let mut chunked_vec_bytes = ChunkedVecBytes::with_capacity(10000);
            for _i in 0..5000 {
                chunked_vec_bytes.push_data_ref(bytes_vec.as_slice());
                chunked_vec_bytes.push(None);
            }
        });
    }

    #[bench]
    fn bench_bytes_iterate(b: &mut test::Bencher) {
        let mut bytes_vec: Vec<u8> = vec![];
        for _i in 0..10 {
            bytes_vec.append(&mut b"2333333333".to_vec());
        }
        let mut chunked_vec_bytes = ChunkedVecBytes::with_capacity(10000);
        for _i in 0..5000 {
            chunked_vec_bytes.push(Some(bytes_vec.clone()));
            chunked_vec_bytes.push(None);
        }
        b.iter(|| {
            let mut sum = 0;
            for i in 0..10000 {
                if let Some(x) = chunked_vec_bytes.get(i) {
                    for i in x {
                        sum += *i as usize;
                    }
                }
            }
            sum
        });
    }
}
