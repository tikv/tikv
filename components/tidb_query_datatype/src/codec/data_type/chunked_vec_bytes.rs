use super::chunked_vec_bool::ChunkedVecBool;
use super::{Bytes, BytesRef};
use super::{ChunkRef, ChunkedVec, UnsafeRefInto};

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecBytes {
    data: Vec<u8>,
    bitmap: ChunkedVecBool,
    length: usize,
    var_offset: Vec<usize>,
}

impl ChunkedVecBytes {
    pub fn from_slice(slice: &[Option<Bytes>]) -> Self {
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

    pub fn from_vec(data: Vec<Option<Bytes>>) -> Self {
        let mut x = Self::with_capacity(data.len());
        for element in data {
            x.push(element);
        }
        x
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_data(&mut self, mut value: Bytes) {
        self.bitmap.push(true);
        self.data.append(&mut value);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    pub fn push(&mut self, value: Option<Bytes>) {
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
