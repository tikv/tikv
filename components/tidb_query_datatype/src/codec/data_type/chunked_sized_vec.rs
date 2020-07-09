use super::chunked_bool_vec::ChunkedBoolVec;
use super::{Evaluable, EvaluableRet, ChunkRef};

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecSized<T: Sized> {
    data: Vec<T>,
    bitmap: ChunkedBoolVec,
    null_cnt: usize,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Sized + Clone> ChunkedVecSized<T> {
    pub fn from_slice(slice: &[Option<T>]) -> Self {
        let mut x = Self::with_capacity(slice.len());
        for i in slice {
            x.push(i.clone());
        }
        x
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: ChunkedBoolVec::with_capacity(capacity),
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
        self.bitmap.push(true);
        self.data.push(value);
    }

    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.data.push(unsafe { std::mem::zeroed() });
    }

    pub fn push(&mut self, value: Option<T>) {
        if let Some(x) = value {
            self.push_data(x);
        } else {
            self.push_null();
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
        self.bitmap.truncate(len);
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data);
        self.bitmap.append(&mut other.bitmap);
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        assert!(idx < self.data.len());
        if self.bitmap.get(idx) {
            Some(&self.data[idx])
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

    pub fn phantom_owned_data(&self) -> Option<T> {
        None
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
