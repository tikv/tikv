// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::{Error, Result};
use codec::prelude::*;
use num_traits::PrimInt;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::marker::PhantomData;

pub enum RowSlice<'a> {
    Small {
        origin: &'a [u8],
        non_null_ids: LEBytes<'a, u8>,
        null_ids: LEBytes<'a, u8>,
        offsets: LEBytes<'a, u16>,
        values: LEBytes<'a, u8>,
    },
    Big {
        origin: &'a [u8],
        non_null_ids: LEBytes<'a, u32>,
        null_ids: LEBytes<'a, u32>,
        offsets: LEBytes<'a, u32>,
        values: LEBytes<'a, u8>,
    },
}

impl RowSlice<'_> {
    /// # Panics
    ///
    /// Panics if the value of first byte is not 128(v2 version code)
    pub fn from_bytes(mut data: &[u8]) -> Result<RowSlice> {
        let origin = data;
        assert_eq!(data.read_u8()?, super::CODEC_VERSION);
        let is_big = super::Flags::from_bits_truncate(data.read_u8()?) == super::Flags::BIG;

        // read ids count
        let non_null_cnt = data.read_u16_le()? as usize;
        let null_cnt = data.read_u16_le()? as usize;
        let row = if is_big {
            RowSlice::Big {
                origin,
                non_null_ids: read_le_bytes(&mut data, non_null_cnt)?,
                null_ids: read_le_bytes(&mut data, null_cnt)?,
                offsets: read_le_bytes(&mut data, non_null_cnt)?,
                values: LEBytes::new(data),
            }
        } else {
            RowSlice::Small {
                origin,
                non_null_ids: read_le_bytes(&mut data, non_null_cnt)?,
                null_ids: read_le_bytes(&mut data, null_cnt)?,
                offsets: read_le_bytes(&mut data, non_null_cnt)?,
                values: LEBytes::new(data),
            }
        };
        Ok(row)
    }

    /// Search `id` in non-null ids
    ///
    /// Returns the `start` position and `offset` in `values` field if found, otherwise returns `None`
    ///
    /// # Errors
    ///
    /// If the id is found with no offset(It will only happen when the row data is broken),
    /// `Error::ColumnOffset` will be returned.
    pub fn search_in_non_null_ids(&self, id: i64) -> Result<Option<(usize, usize)>> {
        if !self.id_valid(id) {
            return Ok(None);
        }
        match self {
            RowSlice::Big {
                non_null_ids,
                offsets,
                ..
            } => {
                if let Ok(idx) = non_null_ids.binary_search(&(id as u32)) {
                    let offset = offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                    let start = if idx > 0 {
                        // Previous `offsets.get(idx)` indicates it's ok to index `idx - 1`
                        unsafe { offsets.get_unchecked(idx - 1) as usize }
                    } else {
                        0usize
                    };
                    return Ok(Some((start, (offset as usize))));
                }
            }
            RowSlice::Small {
                non_null_ids,
                offsets,
                ..
            } => {
                if let Ok(idx) = non_null_ids.binary_search(&(id as u8)) {
                    let offset = offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                    let start = if idx > 0 {
                        // Previous `offsets.get(idx)` indicates it's ok to index `idx - 1`
                        unsafe { offsets.get_unchecked(idx - 1) as usize }
                    } else {
                        0usize
                    };
                    return Ok(Some((start, (offset as usize))));
                }
            }
        }
        Ok(None)
    }

    /// Search `id` in null ids
    ///
    /// Returns true if found
    pub fn search_in_null_ids(&self, id: i64) -> bool {
        if !self.id_valid(id) {
            return false;
        }
        match self {
            RowSlice::Big { null_ids, .. } => null_ids.binary_search(&(id as u32)).is_ok(),
            RowSlice::Small { null_ids, .. } => null_ids.binary_search(&(id as u8)).is_ok(),
        }
    }

    #[inline]
    fn id_valid(&self, id: i64) -> bool {
        let upper: i64 = if self.is_big() {
            i64::from(u32::max_value())
        } else {
            i64::from(u8::max_value())
        };
        id > 0 && id <= upper
    }

    #[inline]
    fn is_big(&self) -> bool {
        match self {
            RowSlice::Big { .. } => true,
            RowSlice::Small { .. } => false,
        }
    }

    #[inline]
    pub fn values(&self) -> &[u8] {
        match self {
            RowSlice::Big { values, .. } => values.slice,
            RowSlice::Small { values, .. } => values.slice,
        }
    }

    #[inline]
    pub fn origin(&self) -> &[u8] {
        match self {
            RowSlice::Big { origin, .. } => *origin,
            RowSlice::Small { origin, .. } => *origin,
        }
    }

    #[inline]
    pub fn get(&self, column_id: i64) -> Result<Option<&[u8]>> {
        if let Some((start, end)) = self.search_in_non_null_ids(column_id)? {
            Ok(Some(self.values().get(start..end).ok_or_else(|| {
                Error::CorruptedData(log_wrappers::Value(self.origin()).to_string())
            })?))
        } else {
            Ok(None)
        }
    }
}

/// Decodes `len` number of ints from `buf` in little endian
///
/// Note:
/// This method is only implemented on little endianness currently, since x86 use little endianness.
#[cfg(target_endian = "little")]
#[inline]
fn read_le_bytes<'a, T>(buf: &mut &'a [u8], len: usize) -> Result<LEBytes<'a, T>>
where
    T: PrimInt,
{
    let bytes_len = std::mem::size_of::<T>() * len;
    if buf.len() < bytes_len {
        return Err(Error::unexpected_eof());
    }
    let slice = &buf[..bytes_len];
    buf.advance(bytes_len);
    Ok(LEBytes::new(slice))
}

#[cfg(target_endian = "little")]
pub struct LEBytes<'a, T: PrimInt> {
    slice: &'a [u8],
    _marker: PhantomData<T>,
}

#[cfg(target_endian = "little")]
impl<'a, T: PrimInt> LEBytes<'a, T> {
    fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            _marker: PhantomData::default(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<T> {
        if std::mem::size_of::<T>() * index >= self.slice.len() {
            None
        } else {
            unsafe { Some(self.get_unchecked(index)) }
        }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> T {
        let ptr = self.slice.as_ptr() as *const T;
        let ptr = ptr.add(index);
        std::ptr::read_unaligned(ptr)
    }

    #[inline]
    fn binary_search(&self, value: &T) -> std::result::Result<usize, usize> {
        let mut size = self.slice.len() / std::mem::size_of::<T>();
        if size == 0 {
            return Err(0);
        }
        let mut base = 0usize;

        // Note that the count of ids is not greater than `u16::MAX`. The number
        // of binary search steps will not over 16 unless the data is corrupted.
        // Let's relex to 20.
        let mut steps = 20usize;

        while steps > 0 && size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.get_unchecked(mid) }.cmp(value);
            base = if cmp == Greater { base } else { mid };
            size -= half;
            steps -= 1;
        }

        let cmp = unsafe { self.get_unchecked(base) }.cmp(value);
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::encoder_for_test::{Column, RowEncoder};
    use super::{read_le_bytes, RowSlice};
    use crate::codec::data_type::ScalarValue;
    use crate::expr::EvalContext;
    use codec::prelude::NumberEncoder;
    use std::u16;

    #[test]
    fn test_read_le_bytes() {
        let data = vec![1, 128, 512, u16::MAX, 256];
        let mut buf = vec![];
        for n in &data {
            buf.write_u16_le(*n).unwrap();
        }

        for i in 1..=data.len() {
            let le_bytes = read_le_bytes::<u16>(&mut buf.as_slice(), i).unwrap();
            for j in 0..i {
                assert_eq!(unsafe { le_bytes.get_unchecked(j) }, data[j]);
            }
        }
    }

    fn encoded_data_big() -> Vec<u8> {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(356, 2),
            Column::new(33, ScalarValue::Int(None)), //0x21
            Column::new(3, 3),
            Column::new(64123, 5),
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    fn encoded_data() -> Vec<u8> {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3),
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    #[test]
    fn test_search_in_non_null_ids() {
        let data = encoded_data_big();
        let big_row = RowSlice::from_bytes(&data).unwrap();
        assert!(big_row.is_big());
        assert_eq!(big_row.search_in_non_null_ids(33).unwrap(), None);
        assert_eq!(big_row.search_in_non_null_ids(333).unwrap(), None);
        assert_eq!(
            big_row
                .search_in_non_null_ids(i64::from(u32::max_value()) + 2)
                .unwrap(),
            None
        );
        assert_eq!(Some((0, 2)), big_row.search_in_non_null_ids(1).unwrap());
        assert_eq!(Some((3, 4)), big_row.search_in_non_null_ids(356).unwrap());
        assert_eq!(Some((4, 5)), big_row.search_in_non_null_ids(64123).unwrap());
        assert_eq!(None, big_row.search_in_non_null_ids(64124).unwrap());

        let data = encoded_data();
        let row = RowSlice::from_bytes(&data).unwrap();
        assert!(!row.is_big());
        assert_eq!(row.search_in_non_null_ids(33).unwrap(), None);
        assert_eq!(row.search_in_non_null_ids(35).unwrap(), None);
        assert_eq!(
            row.search_in_non_null_ids(i64::from(u8::max_value()) + 2)
                .unwrap(),
            None
        );
        assert_eq!(Some((0, 2)), row.search_in_non_null_ids(1).unwrap());
        assert_eq!(Some((2, 3)), row.search_in_non_null_ids(3).unwrap());
    }

    #[test]
    fn test_search_in_null_ids() {
        let data = encoded_data_big();
        let row = RowSlice::from_bytes(&data).unwrap();
        assert!(row.search_in_null_ids(0x21));
        assert!(!row.search_in_null_ids(3));
        assert!(!row.search_in_null_ids(333));
        assert!(!row.search_in_null_ids(0xCC21));
        assert!(!row.search_in_null_ids(0xFF0021));
        assert!(!row.search_in_null_ids(0xFF00000021));

        let data = encoded_data();
        let row = RowSlice::from_bytes(&data).unwrap();
        assert!(row.search_in_null_ids(0x21));
        assert!(!row.search_in_null_ids(3));
        assert!(!row.search_in_null_ids(333));
        assert!(!row.search_in_null_ids(0xCC21));
        assert!(!row.search_in_null_ids(0xFF0021));
        assert!(!row.search_in_null_ids(0xFF00000021));
    }
}

#[cfg(test)]
mod benches {
    use super::super::encoder_for_test::{Column, RowEncoder};
    use super::RowSlice;
    use crate::codec::data_type::ScalarValue;
    use crate::expr::EvalContext;
    use test::black_box;

    fn encoded_data(len: usize) -> Vec<u8> {
        let mut cols = vec![];
        for i in 0..(len as i64) {
            if i % 10 == 0 {
                cols.push(Column::new(i, ScalarValue::Int(None)))
            } else {
                cols.push(Column::new(i, i))
            }
        }
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    #[bench]
    fn bench_search_in_non_null_ids(b: &mut test::Bencher) {
        let data = encoded_data(10);

        b.iter(|| {
            let row = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(row.search_in_non_null_ids(3))
        });
    }

    #[bench]
    fn bench_search_in_non_null_ids_middle(b: &mut test::Bencher) {
        let data = encoded_data(100);

        b.iter(|| {
            let row = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(row.search_in_non_null_ids(89))
        });
    }

    #[bench]
    fn bench_search_in_null_ids_middle(b: &mut test::Bencher) {
        let data = encoded_data(100);

        b.iter(|| {
            let row = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(row.search_in_non_null_ids(20))
        });
    }

    #[bench]
    fn bench_search_in_non_null_ids_big(b: &mut test::Bencher) {
        let data = encoded_data(350);

        b.iter(|| {
            let row = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(row.search_in_non_null_ids(257))
        });
    }

    #[bench]
    fn bench_from_bytes_big(b: &mut test::Bencher) {
        let data = encoded_data(350);

        b.iter(|| {
            let row = RowSlice::from_bytes(black_box(&data)).unwrap();
            black_box(&row);
        });
    }
}
