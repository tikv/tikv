// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{bit_vec::BitVec, ChunkRef, ChunkedVec, Enum, EnumRef, UnsafeRefInto};
use crate::{
    codec::data_type::{retain_lifetime_transmute, ChunkedVecBytes, ChunkedVecSized, Int},
    impl_chunked_vec_common,
};

/// `ChunkedVecEnum` is a vector storing `Option<Enum>`.
///
/// Inside `ChunkedVecEnum`:
/// - `values` stores 1-based index enum data offsets, 0 means this enum is ''
/// - `names` stores the names of enum data
///
/// # Notes
///
/// `values` and `names` both maintain a duplicated bitmap.
/// We are not able to store the data in a more compact form
/// because we have to borrow the reference of `ChunkedVecSize<Int>`
/// and `ChunkedVecBytes`. The borrowed reference should have the same
/// lifetime with `ChunkedVecEnum` which cannot be achieved if we don't
/// store the owned data in struct fields.
///
/// TODO: Change to a compact format when TiDB has its Enum/Set EvalType
#[derive(Debug, Clone)]
pub struct ChunkedVecEnum {
    // MySQL Enum is 1-based index, value == 0 means this enum is ''
    values: ChunkedVecSized<Int>,
    names: ChunkedVecBytes,
}

impl ChunkedVecEnum {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<EnumRef<'_>> {
        assert!(idx < self.len());
        if let Some(value) = self.values.get_option_ref(idx) {
            let name = self.names.get(idx).unwrap();
            Some(EnumRef::new(name, unsafe {
                retain_lifetime_transmute(value)
            }))
        } else {
            None
        }
    }

    #[inline]
    pub fn as_vec_int(&self) -> &ChunkedVecSized<Int> {
        &self.values
    }

    #[inline]
    pub fn as_vec_bytes(&self) -> &ChunkedVecBytes {
        &self.names
    }
}

impl ChunkedVec<Enum> for ChunkedVecEnum {
    impl_chunked_vec_common! { Enum }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: ChunkedVecSized::<Int>::with_capacity(capacity),
            names: ChunkedVecBytes::with_capacity(capacity),
        }
    }

    #[inline]
    fn push_data(&mut self, value: Enum) {
        self.values.push_data(value.value() as i64);
        self.names.push_data_ref(value.name());
    }

    #[inline]
    fn push_null(&mut self) {
        self.values.push_null();
        self.names.push_null();
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.values.truncate(len);
            self.names.truncate(len);
        }
    }

    fn capacity(&self) -> usize {
        self.values.capacity().max(self.names.capacity())
    }

    fn append(&mut self, other: &mut Self) {
        self.values.append(&mut other.values);
        self.names.append(&mut other.names);
    }

    fn to_vec(&self) -> Vec<Option<Enum>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some(value) = self.values.get_option_ref(i) {
                let name = self.names.get(i).unwrap().to_vec();
                x.push(Some(Enum::new(name, *value as u64)));
            } else {
                x.push(None);
            }
        }
        x
    }
}

impl PartialEq for ChunkedVecEnum {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }

        if !self.values.eq(&other.values) {
            return false;
        }

        if !self.names.eq(&other.names) {
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
        self.values.get_bit_vec()
    }

    #[inline]
    fn phantom_data(self) -> Option<EnumRef<'a>> {
        None
    }
}

impl From<Vec<Option<Enum>>> for ChunkedVecEnum {
    fn from(v: Vec<Option<Enum>>) -> ChunkedVecEnum {
        ChunkedVecEnum::from_vec(v)
    }
}

impl<'a> UnsafeRefInto<&'static ChunkedVecEnum> for &'a ChunkedVecEnum {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecEnum {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> ChunkedVecEnum {
        ChunkedVecEnum::with_capacity(0)
    }

    #[test]
    fn test_basics() {
        let mut x = setup();
        x.push(None);
        x.push(Some(Enum::new("我强爆啊".as_bytes().to_vec(), 2)));
        x.push(None);
        x.push(Some(Enum::new("我好强啊".as_bytes().to_vec(), 1)));
        x.push(Some(Enum::new("我成功了".as_bytes().to_vec(), 3)));

        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some(EnumRef::new("我强爆啊".as_bytes(), &2)));
        assert_eq!(x.get(2), None);
        assert_eq!(x.get(3), Some(EnumRef::new("我好强啊".as_bytes(), &1)));
        assert_eq!(x.get(4), Some(EnumRef::new("我成功了".as_bytes(), &3)));
        assert_eq!(x.len(), 5);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let mut x = setup();
        x.push(None);
        x.push(Some(Enum::new("我强爆啊".as_bytes().to_vec(), 2)));
        x.push(None);
        x.push(Some(Enum::new("我好强啊".as_bytes().to_vec(), 1)));
        x.push(Some(Enum::new("我成功了".as_bytes().to_vec(), 3)));

        x.truncate(100);
        assert_eq!(x.len(), 5);

        x.truncate(3);
        assert_eq!(x.len(), 3);
        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some(EnumRef::new("我强爆啊".as_bytes(), &2)));
        assert_eq!(x.get(2), None);

        x.truncate(1);
        assert_eq!(x.len(), 1);
        assert_eq!(x.get(0), None);

        x.truncate(0);
        assert_eq!(x.len(), 0);
    }

    #[test]
    fn test_append() {
        let mut x = setup();
        x.push(None);
        x.push(Some(Enum::new("我强爆啊".as_bytes().to_vec(), 2)));

        let mut y = setup();
        y.push(None);
        y.push(Some(Enum::new("我好强啊".as_bytes().to_vec(), 1)));
        y.push(Some(Enum::new("我成功了".as_bytes().to_vec(), 3)));

        x.append(&mut y);
        assert_eq!(x.len(), 5);
        assert!(y.is_empty());

        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some(EnumRef::new("我强爆啊".as_bytes(), &2)));
        assert_eq!(x.get(2), None);
        assert_eq!(x.get(3), Some(EnumRef::new("我好强啊".as_bytes(), &1)));
        assert_eq!(x.get(4), Some(EnumRef::new("我成功了".as_bytes(), &3)));
    }
}
