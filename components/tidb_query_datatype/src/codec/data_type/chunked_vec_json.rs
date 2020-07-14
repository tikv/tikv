// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::chunked_vec_bool::ChunkedVecBool;
use super::{ChunkRef, ChunkedVec, UnsafeRefInto};
use super::{Json, JsonRef, JsonType};
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecJson {
    data: Vec<u8>,
    bitmap: ChunkedVecBool,
    length: usize,
    var_offset: Vec<usize>,
}

impl ChunkedVecJson {
    pub fn from_slice(slice: &[Option<Json>]) -> Self {
        let mut x = Self::with_capacity(slice.len());
        for i in slice {
            x.push(i.clone());
        }
        x
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: ChunkedVecBool::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    pub fn from_vec(data: Vec<Option<Json>>) -> Self {
        let mut x = Self::with_capacity(data.len());
        for element in data {
            x.push(element);
        }
        x
    }

    pub fn to_vec(&self) -> Vec<Option<Json>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_data(&mut self, mut value: Json) {
        self.bitmap.push(true);
        self.data.push(value.get_type() as u8);
        self.data.append(&mut value.value);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push(&mut self, value: Option<Json>) {
        if let Some(x) = value {
            self.push_data(x);
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

    pub fn get(&self, idx: usize) -> Option<JsonRef> {
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
    fn chunked_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }
    fn chunked_push(&mut self, value: Option<Json>) {
        self.push(value)
    }
}

impl<'a> ChunkRef<'a, JsonRef<'a>> for &'a ChunkedVecJson {
    fn get_option_ref(self, idx: usize) -> Option<JsonRef<'a>> {
        self.get(idx)
    }

    fn phantom_data(self) -> Option<JsonRef<'a>> {
        None
    }
}

impl Into<ChunkedVecJson> for Vec<Option<Json>> {
    fn into(self) -> ChunkedVecJson {
        ChunkedVecJson::from_vec(self)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecJson> for &'a ChunkedVecJson {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecJson {
        std::mem::transmute(self)
    }
}
