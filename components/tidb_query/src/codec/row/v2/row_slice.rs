// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(dead_code)]
use crate::codec::{Error, Result};
use codec::prelude::*;
use num_traits::PrimInt;
use tikv_util::codec::read_slice;

#[derive(Default, Debug)]
struct RowSlice<'a> {
    is_big: bool,
    non_null_ids: &'a [u8],
    null_ids: &'a [u8],
    offsets: &'a [u16],

    non_null_ids_big: &'a [u32],
    null_ids_big: &'a [u32],
    offsets_big: &'a [u32],

    values: &'a [u8],
}

impl RowSlice<'_> {
    fn from_bytes(mut data: &[u8]) -> Result<RowSlice> {
        assert_eq!(data.read_u8()?, super::CODEC_VERSION);
        let mut row = RowSlice::default();
        row.is_big = data.read_u8()? == super::BIG_ID_FLAG;

        // read ids count
        let non_null_cnt = data.read_u16_le()? as usize;
        let null_cnt = data.read_u16_le()? as usize;
        if row.is_big {
            row.non_null_ids_big = read_ints_le(&mut data, non_null_cnt)?;
            row.null_ids_big = read_ints_le(&mut data, null_cnt)?;
            row.offsets_big = read_ints_le(&mut data, non_null_cnt)?;
        } else {
            row.non_null_ids = read_ints_le(&mut data, non_null_cnt)?;
            row.null_ids = read_ints_le(&mut data, null_cnt)?;
            row.offsets = read_ints_le(&mut data, non_null_cnt)?;
        }
        row.values = data;
        Ok(row)
    }

    /// Search `id` in non-null ids
    ///
    /// Returns the `start` position and `offset` in `values` field if found, otherwise returns `None`
    fn search_in_non_null_ids(&self, id: i64) -> Result<Option<(usize, usize)>> {
        if !self.id_valid(id) {
            return Ok(None);
        }
        if self.is_big {
            if let Ok(idx) = self.non_null_ids_big.binary_search(&(id as u32)) {
                let offset = self.offsets_big.get(idx).ok_or(Error::ColumnOffset(idx))?;
                let start = if idx > 0 {
                    self.offsets_big[idx - 1] as usize
                } else {
                    0usize
                };
                return Ok(Some((start, (*offset as usize))));
            }
        } else {
            if let Ok(idx) = self.non_null_ids.binary_search(&(id as u8)) {
                let offset = self.offsets.get(idx).ok_or(Error::ColumnOffset(idx))?;
                let start = if idx > 0 {
                    self.offsets[idx - 1] as usize
                } else {
                    0usize
                };
                return Ok(Some((start, (*offset as usize))));
            }
        }
        Ok(None)
    }

    /// Search `id` in null ids
    ///
    /// Returns true if found
    fn search_in_null_ids(&self, id: i64) -> bool {
        if self.is_big {
            self.null_ids_big.binary_search(&(id as u32)).is_ok()
        } else {
            self.null_ids.binary_search(&(id as u8)).is_ok()
        }
    }

    fn id_valid(&self, id: i64) -> bool {
        let upper: i64 = if self.is_big {
            i64::from(u32::max_value())
        } else {
            i64::from(u8::max_value())
        };
        id > 0 && id <= upper
    }
}

/// Decodes `len` number of ints from `buf` in little endian
#[inline]
fn read_ints_le<'a, T>(buf: &mut &'a [u8], len: usize) -> Result<&'a [T]>
where
    T: PrimInt,
{
    use std::{mem, slice};
    let bytes_len = mem::size_of::<T>() * len;
    let bytes = read_slice(buf, bytes_len)?.as_ptr() as *mut T;
    let buf = unsafe { slice::from_raw_parts_mut(bytes, len) };
    for n in buf.iter_mut() {
        *n = n.to_le()
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::super::encoder;
    use super::{read_ints_le, RowSlice};
    use crate::codec::Datum;
    use codec::prelude::NumberEncoder;
    use std::u16;

    #[test]
    fn test_read_ints() {
        let data = vec![1, 128, 512, u16::MAX, 256];
        let mut buf = vec![];
        for n in &data {
            buf.write_u16_le(*n).unwrap();
        }

        assert_eq!(
            &data[0..3],
            read_ints_le::<u16>(&mut buf.as_slice(), 3).unwrap()
        );
        assert_eq!(
            &data[0..4],
            read_ints_le::<u16>(&mut buf.as_slice(), 4).unwrap()
        );
    }

    fn encoded_data_big() -> Vec<u8> {
        let ids: Vec<i64> = vec![1, 356, 33, 3];
        let values = vec![Datum::I64(1000), Datum::I64(2), Datum::Null, Datum::U64(3)];
        encoder::encode(values, &ids).unwrap()
    }

    fn encoded_data() -> Vec<u8> {
        let ids: Vec<i64> = vec![1, 33, 3];
        let values = vec![Datum::I64(1000), Datum::Null, Datum::U64(3)];
        encoder::encode(values, &ids).unwrap()
    }

    #[test]
    fn test_search_in_non_null_ids() {
        let data = encoded_data_big();
        let big_row = RowSlice::from_bytes(&data).unwrap();
        assert!(big_row.is_big);
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

        let data = encoded_data();
        let row = RowSlice::from_bytes(&data).unwrap();
        assert!(!row.is_big);
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
        assert!(row.search_in_null_ids(33));
        assert!(!row.search_in_null_ids(3));
        assert!(!row.search_in_null_ids(333));
    }
}

#[cfg(test)]
mod benches {
    use super::super::encoder;
    use super::RowSlice;
    use crate::codec::Datum;
    use test::black_box;

    fn encoded_data(len: usize) -> Vec<u8> {
        let mut ids = Vec::with_capacity(len);
        let mut datums = Vec::with_capacity(len);
        for i in 0..(len as i64) {
            ids.push(i);
            if i % 10 == 0 {
                datums.push(Datum::Null)
            } else {
                datums.push(Datum::I64(i));
            }
        }
        encoder::encode(datums, &ids).unwrap()
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
