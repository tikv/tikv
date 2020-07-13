// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

impl<'a, T: Evaluable + EvaluableRet> ChunkRef<'a, &'a T> for &'a NotChunkedVec<T> {
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.data[idx].as_ref()
    }

    fn phantom_data(self) -> Option<&'a T> {
        None
    }
}

impl<'a> ChunkRef<'a, BytesRef<'a>> for &'a NotChunkedVec<Bytes> {
    fn get_option_ref(self, idx: usize) -> Option<BytesRef<'a>> {
        self.data[idx].as_deref()
    }

    fn phantom_data(self) -> Option<BytesRef<'a>> {
        None
    }
}

impl<'a> ChunkRef<'a, JsonRef<'a>> for &'a NotChunkedVec<Json> {
    fn get_option_ref(self, idx: usize) -> Option<JsonRef<'a>> {
        self.data[idx].as_ref().map(|x| x.as_ref())
    }

    fn phantom_data(self) -> Option<JsonRef<'a>> {
        None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct NotChunkedVec<T> {
    data: Vec<Option<T>>,
}

impl<T: Sized + Clone> NotChunkedVec<T> {
    pub fn from_slice(slice: &[Option<T>]) -> Self {
        Self {
            data: slice.to_vec(),
        }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, value: Option<T>) {
        self.data.push(value)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len)
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data)
    }

    pub fn as_vec(&self) -> Vec<Option<T>> {
        self.data.clone()
    }
}

impl<T> Into<NotChunkedVec<T>> for Vec<Option<T>> {
    fn into(self) -> NotChunkedVec<T> {
        NotChunkedVec { data: self }
    }
}

impl<'a, T: Evaluable> UnsafeRefInto<&'static NotChunkedVec<T>> for &'a NotChunkedVec<T> {
    unsafe fn unsafe_into(self) -> &'static NotChunkedVec<T> {
        std::mem::transmute(self)
    }
}

impl<'a> UnsafeRefInto<&'static NotChunkedVec<Bytes>> for &'a NotChunkedVec<Bytes> {
    unsafe fn unsafe_into(self) -> &'static NotChunkedVec<Bytes> {
        std::mem::transmute(self)
    }
}

impl<'a> UnsafeRefInto<&'static NotChunkedVec<Json>> for &'a NotChunkedVec<Json> {
    unsafe fn unsafe_into(self) -> &'static NotChunkedVec<Json> {
        std::mem::transmute(self)
    }
}
