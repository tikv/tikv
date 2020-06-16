// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

impl<'a, T: Evaluable> ChunkRef<'a, &'a T> for &'a NotChunkedVec<T> {
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.data[idx].as_ref()
    }
}

impl<'a> ChunkRef<'a, BytesRef<'a>> for &'a NotChunkedVec<Bytes> {
    fn get_option_ref(self, idx: usize) -> Option<BytesRef<'a>> {
        self.data[idx].as_deref()
    }
}

impl<'a> ChunkRef<'a, JsonRef<'a>> for &'a NotChunkedVec<Json> {
    fn get_option_ref(self, idx: usize) -> Option<JsonRef<'a>> {
        self.data[idx].as_ref().map(|x| x.as_ref())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct NotChunkedVec<T> {
    data: Vec<Option<T>>
}

impl <T: Sized> NotChunkedVec <T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity)
        }
    }

    pub fn push(&mut self, value: Option<T>) {
        self.data.push(value)
    }

    pub fn replace(&mut self, idx: usize, value: Option<T>) {
        self.data[idx] = value
    }

    pub fn len(&self) -> usize {
        self.data.len()
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

    pub fn as_slice(&self) -> &[Option<T>] {
        self.data.as_slice()
    }
}
