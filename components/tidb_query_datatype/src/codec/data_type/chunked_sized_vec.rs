use super::chunked_bool_vec::ChunkedBoolVec;
use super::{Evaluable, EvaluableRet, ChunkRef};

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecSized<T: Sized> {
    data: Vec<u8>,
    bitmap: ChunkedBoolVec,
    length: usize,
    null_cnt: usize,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Sized + Clone> ChunkedVecSized<T> {
    const TYPE_SIZE: usize = std::mem::size_of::<T>();

    pub fn from_slice(slice: &[Option<T>]) -> Self {
        let mut x = Self::with_capacity(slice.len());
        for i in slice {
            x.push(i.clone());
        }
        x
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity * Self::TYPE_SIZE),
            bitmap: ChunkedBoolVec::with_capacity(capacity),
            length: 0,
            phantom: std::marker::PhantomData,
            null_cnt: 0,
        }
    }

    pub fn from_vec(data: Vec<Option<T>>) -> Self {
        let mut x = Self::with_capacity(data.len());
        for element in data {
            x.push(element);
        }
        x
    }

    pub fn push_data(&mut self, value: T) {
        self.prepare_append();
        self.bitmap.push(true);
        let chunk_begin = (self.length - 1) * Self::TYPE_SIZE;
        let mut_ptr = &mut self.data[chunk_begin] as *mut u8 as *mut T;
        unsafe { std::ptr::write(mut_ptr, value); };
    }

    pub fn push_null(&mut self) {
        self.prepare_append();
        self.bitmap.push(false);
    }

    fn prepare_append(&mut self) {
        self.length += 1;
        self.data.resize(self.length * Self::TYPE_SIZE, 0);
    }

    pub fn push(&mut self, value: Option<T>) {
        if let Some(x) = value {
            self.push_data(x);
        } else {
            self.push_null();
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.length {
            self.length = len;
            self.data.truncate(len * Self::TYPE_SIZE);
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity() / Self::TYPE_SIZE
    }

    pub fn append(&mut self, other: &mut Self) {
        for i in 0..other.len() {
            self.push(other.get(i).cloned());
        }
        other.truncate(0);
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        assert!(idx < self.length);
        let chunk_begin = idx * Self::TYPE_SIZE;
        let chunk_end = chunk_begin + Self::TYPE_SIZE;
        let ref_data = &self.data[chunk_begin..chunk_end];
        if self.bitmap.get(idx) {
            Some(unsafe { std::mem::transmute::<&u8, &T>(&ref_data[0]) })
        } else {
            None
        }
    }

    pub fn as_vec(&self) -> Vec<Option<T>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).cloned());
        }
        x
    }
}

impl<'a, T: Evaluable + EvaluableRet> ChunkRef<'a, &'a T> for &'a ChunkedVecSized<T> {
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.get(idx)
    }

    fn phantom_data(self) -> Option<&'a T> {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basics() {
        let mut x: ChunkedVecSized<u64> = ChunkedVecSized::with_capacity(0);
        x.push(Some(1));
        x.push(Some(2));
        x.push(Some(3));
        x.push(None);
        assert_eq!(x.get(0), Some(&1));
        assert_eq!(x.get(1), Some(&2));
        assert_eq!(x.get(2), Some(&3));
        assert_eq!(x.get(3), None);
    }
}
