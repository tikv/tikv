use super::bit_vec::BitVec;
use super::{ChunkRef, ChunkedVec, UnsafeRefInto};
use super::{Enum, EnumRef};
use crate::impl_chunked_vec_common;
use std::sync::Arc;
use tikv_util::buffer_vec::BufferVec;

#[derive(Debug, Clone)]
pub struct ChunkedVecEnum {
    data: Arc<BufferVec>,
    bitmap: BitVec,
    // MySQL Enum is 1-based index, value == 0 means this enum is ''
    value: Vec<usize>,
}

impl ChunkedVecEnum {
    impl_chunked_vec_common! { Enum }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Arc::new(BufferVec::new()),
            bitmap: BitVec::with_capacity(capacity),
            value: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    #[inline]
    pub fn push_data(&mut self, value: Enum) {
        self.bitmap.push(true);
        self.value.push(value.value());
    }

    #[inline]
    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.value.push(0);
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.bitmap.truncate(len);
            self.value.truncate(len);
        }
    }

    pub fn capacity(&self) -> usize {
        self.bitmap.capacity().max(self.value.capacity())
    }

    pub fn append(&mut self, other: &mut Self) {
        self.value.append(&mut other.value);
        self.bitmap.append(&mut other.bitmap);
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<EnumRef> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            Some(EnumRef::new(&self.data, *self.value.get(idx).unwrap()))
        } else {
            None
        }
    }

    pub fn to_vec(&self) -> Vec<Option<Enum>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(if self.bitmap.get(i) {
                Some(Enum::new(self.data.clone(), *self.value.get(i).unwrap()))
            } else {
                None
            });
        }
        x
    }
}

impl PartialEq for ChunkedVecEnum {
    fn eq(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        for idx in 0..self.data.len() {
            if self.data[idx] != other.data[idx] {
                return false;
            }
        }

        if !self.bitmap.eq(&other.bitmap) {
            return false;
        }

        if !self.value.eq(&other.value) {
            return false;
        }

        true
    }
}

impl<'a> ChunkRef<'a, EnumRef<'a>> for &'a ChunkedVecEnum {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<EnumRef<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<EnumRef<'a>> {
        None
    }
}

impl ChunkedVec<Enum> for ChunkedVecEnum {
    fn chunked_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    #[inline]
    fn chunked_push(&mut self, value: Option<Enum>) {
        self.push(value)
    }
}

impl Into<ChunkedVecEnum> for Vec<Option<Enum>> {
    fn into(self) -> ChunkedVecEnum {
        ChunkedVecEnum::from_vec(self)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecEnum> for &'a ChunkedVecEnum {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecEnum {
        std::mem::transmute(self)
    }
}
