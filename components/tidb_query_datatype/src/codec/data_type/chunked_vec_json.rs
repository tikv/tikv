// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use super::{bit_vec::BitVec, ChunkRef, ChunkedVec, Json, JsonRef, JsonType, UnsafeRefInto};
use crate::impl_chunked_vec_common;

/// A vector storing `Option<Json>` with a compact layout.
///
/// Inside `ChunkedVecJson`, `bitmap` indicates if an element at given index is null,
/// and `data` stores actual data. Json data are stored adjacent to each other in
/// `data`. If element at a given index is null, then it takes no space in `data`.
/// Otherwise, a one byte `json_type` and variable size json data is stored in `data`,
/// and `var_offset` indicates the starting position of each element.
#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecJson {
    data: Vec<u8>,
    bitmap: BitVec,
    length: usize,
    var_offset: Vec<usize>,
}

impl ChunkedVecJson {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<JsonRef<'_>> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            let json_type = JsonType::try_from(self.data[self.var_offset[idx]]).unwrap();
            let sliced_data = &self.data[self.var_offset[idx] + 1..self.var_offset[idx + 1]];
            Some(JsonRef::new(json_type, sliced_data))
        } else {
            None
        }
    }
}

impl ChunkedVec<Json> for ChunkedVecJson {
    impl_chunked_vec_common! { Json }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    #[inline]
    fn push_data(&mut self, mut value: Json) {
        self.bitmap.push(true);
        self.data.push(value.get_type() as u8);
        self.data.append(&mut value.value);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    #[inline]
    fn push_null(&mut self) {
        self.bitmap.push(false);
        self.var_offset.push(self.data.len());
        self.length += 1;
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

    fn to_vec(&self) -> Vec<Option<Json>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }
}

impl<'a> ChunkRef<'a, JsonRef<'a>> for &'a ChunkedVecJson {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<JsonRef<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<JsonRef<'a>> {
        None
    }
}

impl From<Vec<Option<Json>>> for ChunkedVecJson {
    fn from(v: Vec<Option<Json>>) -> ChunkedVecJson {
        ChunkedVecJson::from_vec(v)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecJson> for &'a ChunkedVecJson {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecJson {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_vec() {
        let test_json: &[Option<Json>] = &[
            None,
            Some(Json::from_str_val("我好菜啊").unwrap()),
            None,
            Some(Json::from_str_val("我菜爆了").unwrap()),
            Some(Json::from_str_val("我失败了").unwrap()),
            Some(Json::from_f64(2.333).unwrap()),
            Some(Json::from_str_val("💩").unwrap()),
            None,
        ];
        assert_eq!(ChunkedVecJson::from_slice(test_json).to_vec(), test_json);
        assert_eq!(ChunkedVecJson::from_slice(test_json).to_vec(), test_json);
    }

    #[test]
    fn test_basics() {
        let mut x: ChunkedVecJson = ChunkedVecJson::with_capacity(0);
        x.push(None);
        x.push(Some(Json::from_str_val("我好菜啊").unwrap()));
        x.push(None);
        x.push(Some(Json::from_str_val("我菜爆了").unwrap()));
        x.push(Some(Json::from_str_val("我失败了").unwrap()));
        assert_eq!(x.get(0), None);
        assert_eq!(
            x.get(1),
            Some(Json::from_str_val("我好菜啊").unwrap().as_ref())
        );
        assert_eq!(x.get(2), None);
        assert_eq!(
            x.get(3),
            Some(Json::from_str_val("我菜爆了").unwrap().as_ref())
        );
        assert_eq!(
            x.get(4),
            Some(Json::from_str_val("我失败了").unwrap().as_ref())
        );
        assert_eq!(x.len(), 5);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let test_json: &[Option<Json>] = &[
            None,
            None,
            Some(Json::from_str_val("我好菜啊").unwrap()),
            None,
            Some(Json::from_str_val("我菜爆了").unwrap()),
            Some(Json::from_str_val("我失败了").unwrap()),
            Some(Json::from_f64(2.333).unwrap()),
            Some(Json::from_str_val("💩").unwrap()),
            None,
        ];
        let mut chunked_vec = ChunkedVecJson::from_slice(test_json);
        chunked_vec.truncate(100);
        assert_eq!(chunked_vec.len(), 9);
        chunked_vec.truncate(3);
        assert_eq!(chunked_vec.len(), 3);
        assert_eq!(chunked_vec.get(0), None);
        assert_eq!(chunked_vec.get(1), None);
        assert_eq!(
            chunked_vec.get(2),
            Some(Json::from_str_val("我好菜啊").unwrap().as_ref())
        );
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
        let test_json_1: &[Option<Json>] = &[
            None,
            None,
            Some(Json::from_str_val("我好菜啊").unwrap()),
            None,
        ];
        let test_json_2: &[Option<Json>] = &[
            Some(Json::from_str_val("我菜爆了").unwrap()),
            Some(Json::from_str_val("我失败了").unwrap()),
            Some(Json::from_f64(2.333).unwrap()),
            Some(Json::from_str_val("💩").unwrap()),
            None,
        ];
        let mut chunked_vec_1 = ChunkedVecJson::from_slice(test_json_1);
        let mut chunked_vec_2 = ChunkedVecJson::from_slice(test_json_2);
        chunked_vec_1.append(&mut chunked_vec_2);
        assert_eq!(chunked_vec_1.len(), 9);
        assert!(chunked_vec_2.is_empty());
        assert_eq!(
            chunked_vec_1.to_vec(),
            &[
                None,
                None,
                Some(Json::from_str_val("我好菜啊").unwrap()),
                None,
                Some(Json::from_str_val("我菜爆了").unwrap()),
                Some(Json::from_str_val("我失败了").unwrap()),
                Some(Json::from_f64(2.333).unwrap()),
                Some(Json::from_str_val("💩").unwrap()),
                None,
            ]
        );
    }
}
