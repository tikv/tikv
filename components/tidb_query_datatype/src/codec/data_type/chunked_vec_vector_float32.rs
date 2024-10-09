// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use super::{
    bit_vec::BitVec, ChunkRef, ChunkedVec, UnsafeRefInto, VectorFloat32, VectorFloat32Ref,
};
use crate::impl_chunked_vec_common;

/// A vector storing `Option<VectorFloat32>` with a compact layout.
///
/// Inside `ChunkedVecVectorFloat32`, `bitmap` indicates if an element at given
/// index is null, and `data` stores actual data. VectorFloat32 data are stored
/// adjacent to each other in `data`. If element at a given index is null, then
/// it takes no space in `data`. Otherwise, a variable size VectorFloat32 data
/// is stored in `data`, and `var_offset` indicates the starting position of
/// each element.
#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecVectorFloat32 {
    data: Vec<f32>, // Only contains the data part, without any length prefix
    bitmap: BitVec,
    length: usize,
    var_offset: Vec<usize>,
}

impl ChunkedVecVectorFloat32 {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<VectorFloat32Ref<'_>> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            let sliced_data = &self.data[self.var_offset[idx]..self.var_offset[idx + 1]];
            Some(VectorFloat32Ref::from_f32(sliced_data))
        } else {
            None
        }
    }
}

impl ChunkedVec<VectorFloat32> for ChunkedVecVectorFloat32 {
    impl_chunked_vec_common! { VectorFloat32 }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    #[inline]
    fn push_data(&mut self, value: VectorFloat32) {
        self.bitmap.push(true);
        self.data.extend_from_slice(value.as_ref().data());
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

    fn to_vec(&self) -> Vec<Option<VectorFloat32>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }
}

impl<'a> ChunkRef<'a, VectorFloat32Ref<'a>> for &'a ChunkedVecVectorFloat32 {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<VectorFloat32Ref<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<VectorFloat32Ref<'a>> {
        None
    }
}

impl From<Vec<Option<VectorFloat32>>> for ChunkedVecVectorFloat32 {
    fn from(v: Vec<Option<VectorFloat32>>) -> ChunkedVecVectorFloat32 {
        ChunkedVecVectorFloat32::from_vec(v)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecVectorFloat32> for &'a ChunkedVecVectorFloat32 {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecVectorFloat32 {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_data_and_get() {
        let mut chunked_vec = ChunkedVecVectorFloat32::with_capacity(5);

        chunked_vec.push_data(VectorFloat32::copy_from_f32(&[1.1, 2.2, 3.3]));
        chunked_vec.push_data(VectorFloat32::copy_from_f32(&[4.4, 5.5]));
        // push a null value
        chunked_vec.push_null();
        chunked_vec.push_data(VectorFloat32::copy_from_f32(&[6.6, 7.7, 8.8, 9.9]));

        assert_eq!(chunked_vec.len(), 4);

        let vector1 = chunked_vec.get(0).unwrap().to_string();
        assert_eq!(vector1, "[1.1,2.2,3.3]");

        let vector2 = chunked_vec.get(1).unwrap().to_string();
        assert_eq!(vector2, "[4.4,5.5]");

        // check if null value is right
        assert!(chunked_vec.get(2).is_none());

        let vector3 = chunked_vec.get(3).unwrap().to_string();
        assert_eq!(vector3, "[6.6,7.7,8.8,9.9]");
    }
}