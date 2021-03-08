// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::*;

use codec::prelude::BufferWriter;

/// A vector like container storing multiple buffers. Each buffer is a `[u8]` slice in
/// arbitrary length.
#[derive(Default, Clone)]
pub struct BufferVec {
    data: Vec<u8>,
    offsets: Vec<usize>,
}

impl std::fmt::Debug for BufferVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, item) in self.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            if item.is_empty() {
                write!(f, "null")?;
            } else {
                write!(f, "{}", log_wrappers::Value::value(item))?;
            }
        }
        write!(f, "]")
    }
}

impl BufferVec {
    /// Constructs a new, empty `BufferVec`.
    ///
    /// The `BufferVec` will not allocate until buffers are pushed onto it.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a new, empty `BufferVec` with the specified element capacity and data capacity.
    #[inline]
    pub fn with_capacity(elements_capacity: usize, data_capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(data_capacity),
            offsets: Vec::with_capacity(elements_capacity),
        }
    }

    /// Returns the number of buffers this `BufferVec` can hold without reallocating the
    /// offsets array.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.offsets.capacity()
    }

    /// Returns the number of buffers this `BufferVec` can hold without reallocating the
    /// data array.
    #[inline]
    pub fn data_capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Returns the number of contained buffers.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Returns `true` if there is no contained buffer.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the total length of all contained buffers.
    #[inline]
    pub fn total_len(&self) -> usize {
        self.data.len()
    }

    /// Appends a buffer to the back.
    #[inline]
    pub fn push(&mut self, buffer: impl AsRef<[u8]>) {
        self.offsets.push(self.data.len());
        self.data.extend_from_slice(buffer.as_ref());
    }

    /// Appends multiple buffer parts together as one buffer to the back.
    #[inline]
    pub fn concat_extend<T, I>(&mut self, buffer_parts: I)
    where
        T: AsRef<[u8]>,
        I: IntoIterator<Item = T>,
    {
        self.offsets.push(self.data.len());
        for part in buffer_parts.into_iter() {
            let slice = part.as_ref();
            self.data.extend_from_slice(slice);
        }
    }

    /// Returns a delegator that provides `extend` appends buffers together as one buffer
    /// to the back.
    ///
    /// Note that this function always creates a new buffer even if you don't call `extend`
    /// on the delegator later, which simply results in appending a new empty buffer.
    #[inline]
    pub fn begin_concat_extend(&mut self) -> WithConcatExtend<'_> {
        WithConcatExtend::init(self)
    }

    /// Removes the last buffer if there is any.
    #[inline]
    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if let Some(offset) = self.offsets.pop() {
            let res = self.data.get(offset..).map(Vec::from);
            self.data.truncate(offset);
            res
        } else {
            None
        }
    }

    /// Get the last buffer if there is any.
    #[inline]
    pub fn last(&self) -> Option<&[u8]> {
        if let Some(offset) = self.offsets.last() {
            self.data.get(*offset..)
        } else {
            None
        }
    }

    /// Removes the first `n` buffers.
    ///
    /// If `n` >= current length, all buffers will be removed.
    pub fn shift(&mut self, n: usize) {
        if n == 0 {
            return;
        }

        // Suppose we want to shift by 2 and we have the following data:
        // data:   [AABB, 00, , CACCAA]
        // offset: [0,    2,  3,3     ]
        //                    ^             : data_offset  = 3
        //                   >|      |<     : data_len     = 3
        //                   >|      |<     : n_len        = 2
        //
        // 1. Move valid data and offset forward
        // data:   [, CACCAA]
        // offset: [3,3     ]
        //
        // 2. Update offset
        // data:   [, CACCAA]
        // offset: [0,0     ]

        let len = self.len();
        if n < len {
            let data_offset = self.offsets[n];
            // 1
            self.data.drain(..data_offset);
            self.offsets.drain(..n);
            // 2
            for offset in &mut self.offsets {
                *offset -= data_offset;
            }
            assert_eq!(self.offsets[0], 0);
        } else {
            self.clear();
        }
    }

    /// Shortens the `BufferVec`, keeping the first `n` buffers and dropping the rest.
    ///
    /// If `n` >= current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, n: usize) {
        let len = self.len();
        if n < len {
            self.data.truncate(self.offsets[n]);
            self.offsets.truncate(n);
        }
    }

    /// Removes all buffers.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.offsets.clear();
    }

    /// Copies all buffers from `other` to `self`.
    pub fn copy_from(&mut self, other: &BufferVec) {
        let base_offset = self.data.len();
        self.data.extend_from_slice(&other.data);
        self.offsets.reserve(other.offsets.len());
        for offset in &other.offsets {
            self.offsets.push(*offset + base_offset);
        }
    }

    /// Copies first `n` buffers from `other` to `self`.
    ///
    /// If `n > other.len()`, only `other.len()` buffers will be copied.
    pub fn copy_n_from(&mut self, other: &BufferVec, n: usize) {
        if n >= other.len() {
            self.copy_from(other);
        } else if n > 0 {
            let base_offset = self.data.len();
            self.data.extend_from_slice(&other.data[..other.offsets[n]]);
            self.offsets.reserve(n);
            for offset in &other.offsets[..n] {
                self.offsets.push(*offset + base_offset);
            }
        }
    }

    /// Retains the elements according to a boolean array.
    ///
    /// # Panics
    ///
    /// Panics if `retain_arr` is not long enough.
    pub fn retain_by_array(&mut self, retain_arr: &[bool]) {
        // TODO: Optimize to move multiple contiguous slots.
        unsafe {
            let len = self.len();
            assert!(retain_arr.len() >= len);

            if len > 0 {
                let mut data_write_offset = 0;
                let mut offsets_write_offset = 0;
                for i in 0..len - 1 {
                    if retain_arr[i] {
                        let write_len = self.offsets[i + 1] - self.offsets[i];
                        std::ptr::copy(
                            self.data.as_ptr().add(self.offsets[i]),
                            self.data.as_mut_ptr().add(data_write_offset),
                            write_len,
                        );
                        self.offsets[offsets_write_offset] = data_write_offset;
                        offsets_write_offset += 1;
                        data_write_offset += write_len;
                    }
                }
                // The last buffer does not have an ending offset
                if retain_arr[len - 1] {
                    let write_len = self.data.len() - self.offsets[len - 1];
                    std::ptr::copy(
                        self.data.as_ptr().add(self.offsets[len - 1]),
                        self.data.as_mut_ptr().add(data_write_offset),
                        write_len,
                    );
                    self.offsets[offsets_write_offset] = data_write_offset;
                    offsets_write_offset += 1;
                    data_write_offset += write_len;
                }
                self.offsets.set_len(offsets_write_offset);
                self.data.set_len(data_write_offset);
            }
        }
    }

    /// Returns an iterator over the `BufferVec`.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            data: &self.data,
            offsets: &self.offsets,
        }
    }
}

impl std::ops::Index<usize> for BufferVec {
    type Output = [u8];

    fn index(&self, index: usize) -> &[u8] {
        assert!(index < self.offsets.len());
        let start = self.offsets[index];
        let end = self
            .offsets
            .get(index + 1)
            .copied()
            .unwrap_or_else(|| self.data.len());
        &self.data[start..end]
    }
}

impl Extend<u8> for BufferVec {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = u8>,
    {
        self.offsets.push(self.data.len());
        self.data.extend(iter);
    }
}

impl<'a> Extend<&'a u8> for BufferVec {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = &'a u8>,
    {
        self.offsets.push(self.data.len());
        self.data.extend(iter);
    }
}

pub struct WithConcatExtend<'a>(&'a mut BufferVec);

impl<'a> WithConcatExtend<'a> {
    fn init(b: &'a mut BufferVec) -> Self {
        b.offsets.push(b.data.len());
        Self(b)
    }

    pub fn finish(self) {
        // do nothing
    }
}

impl<'a> Extend<u8> for WithConcatExtend<'a> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = u8>,
    {
        self.0.data.extend(iter);
    }
}

impl<'a, 'b> Extend<&'a u8> for WithConcatExtend<'b> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = &'a u8>,
    {
        self.0.data.extend(iter);
    }
}

impl<'a> BufferWriter for WithConcatExtend<'a> {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8] {
        self.0.data.bytes_mut(size)
    }

    #[inline]
    unsafe fn advance_mut(&mut self, count: usize) {
        self.0.data.advance_mut(count)
    }

    #[inline]
    fn write_bytes(&mut self, values: &[u8]) -> codec::Result<()> {
        self.0.data.write_bytes(values)
    }
}

// TODO: Implement Index<XxxRange>

#[derive(Clone, Copy)]
pub struct Iter<'a> {
    data: &'a [u8],
    offsets: &'a [usize],
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        Self::nth(self, 0)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.offsets.len();
        (len, Some(len))
    }

    #[inline]
    fn count(self) -> usize {
        self.offsets.len()
    }

    fn last(mut self) -> Option<Self::Item> {
        let l = self.offsets.len();
        if l == 0 {
            None
        } else {
            self.nth(l - 1)
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        use std::cmp::Ordering::*;

        match (n + 1).cmp(&self.offsets.len()) {
            Less => {
                let begin_offset = self.offsets[n];
                let end_offset = self.offsets[n + 1];
                self.offsets = &self.offsets[n + 1..];
                Some(&self.data[begin_offset..end_offset])
            }
            Equal => {
                let begin_offset = self.offsets[n];
                self.offsets = &[];
                Some(&self.data[begin_offset..])
            }
            Greater => {
                self.offsets = &[];
                None
            }
        }
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut v = BufferVec::new();
        assert_eq!(format!("{:?}", v), "[]");
        assert!(v.is_empty());

        v.pop();
        assert_eq!(format!("{:?}", v), "[]");
        assert!(v.is_empty());

        v.shift(0);
        assert_eq!(format!("{:?}", v), "[]");
        assert!(v.is_empty());

        v.push(&[0xAA, 0x0, 0xB]);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xAA, 0x0, 0xB]);
        assert_eq!(format!("{:?}", v), "[AA000B]");

        v.truncate(1);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xAA, 0x0, 0xB]);
        assert_eq!(format!("{:?}", v), "[AA000B]");

        v.truncate(2);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xAA, 0x0, 0xB]);
        assert_eq!(format!("{:?}", v), "[AA000B]");

        v.shift(0);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xAA, 0x0, 0xB]);
        assert_eq!(format!("{:?}", v), "[AA000B]");

        v.pop();
        assert_eq!(v.len(), 0);
        assert_eq!(v.total_len(), 0);
        assert!(v.is_empty());
        assert_eq!(format!("{:?}", v), "[]");

        v.truncate(1);
        assert_eq!(v.len(), 0);
        assert_eq!(v.total_len(), 0);
        assert!(v.is_empty());
        assert_eq!(format!("{:?}", v), "[]");

        v.pop();
        assert_eq!(v.len(), 0);
        assert_eq!(v.total_len(), 0);
        assert!(v.is_empty());
        assert_eq!(format!("{:?}", v), "[]");

        v.push(&[0xCA, 0xB]);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 2);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xCA, 0xB]);
        assert_eq!(format!("{:?}", v), "[CA0B]");

        v.shift(1);
        assert_eq!(v.len(), 0);
        assert_eq!(v.total_len(), 0);
        assert!(v.is_empty());
        assert_eq!(format!("{:?}", v), "[]");

        v.push(&[0xCA, 0xB]);
        v.push(&[]);
        assert_eq!(v.len(), 2);
        assert_eq!(v.total_len(), 2);
        assert!(!v.is_empty());
        assert!(v[1].is_empty()); // To avoid "cannot infer type" bug..
        assert_eq!(format!("{:?}", v), "[CA0B, null]");

        v.pop();
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 2);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xCA, 0xB]);
        assert_eq!(format!("{:?}", v), "[CA0B]");

        v.push(&[]);
        v.push(&[]);
        assert_eq!(v.len(), 3);
        assert_eq!(v.total_len(), 2);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xCA, 0xB]);
        assert!(v[1].is_empty());
        assert!(v[2].is_empty());
        assert_eq!(format!("{:?}", v), "[CA0B, null, null]");

        v.push(&[0xC]);
        assert_eq!(v.len(), 4);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xCA, 0xB]);
        assert!(v[1].is_empty());
        assert!(v[2].is_empty());
        assert_eq!(v[3], [0xC]);
        assert_eq!(format!("{:?}", v), "[CA0B, null, null, 0C]");

        v.pop();
        assert_eq!(v.len(), 3);
        assert_eq!(v.total_len(), 2);
        assert!(!v.is_empty());
        assert_eq!(v[0], [0xCA, 0xB]);
        assert!(v[1].is_empty());
        assert!(v[2].is_empty());
        assert_eq!(format!("{:?}", v), "[CA0B, null, null]");

        v.shift(1);
        assert_eq!(v.len(), 2);
        assert_eq!(v.total_len(), 0);
        assert!(!v.is_empty());
        assert!(v[0].is_empty());
        assert!(v[1].is_empty());
        assert_eq!(format!("{:?}", v), "[null, null]");

        v.push(&[0xAC, 0xBB, 0x00]);
        assert_eq!(v.len(), 3);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert!(v[0].is_empty());
        assert!(v[1].is_empty());
        assert_eq!(v[2], [0xAC, 0xBB, 0x00]);
        assert_eq!(format!("{:?}", v), "[null, null, ACBB00]");

        v.shift(0);
        assert!(!v.is_empty());
        assert_eq!(format!("{:?}", v), "[null, null, ACBB00]");

        v.shift(1);
        assert_eq!(v.len(), 2);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert!(v[0].is_empty());
        assert_eq!(v[1], [0xAC, 0xBB, 0x00]);
        assert_eq!(format!("{:?}", v), "[null, ACBB00]");

        v.push(&[]);
        assert_eq!(v.len(), 3);
        assert_eq!(v.total_len(), 3);
        assert!(!v.is_empty());
        assert!(v[0].is_empty());
        assert_eq!(v[1], [0xAC, 0xBB, 0x00]);
        assert!(v[2].is_empty());
        assert_eq!(format!("{:?}", v), "[null, ACBB00, null]");

        v.shift(2);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 0);
        assert!(!v.is_empty());
        assert!(v[0].is_empty());
        assert_eq!(format!("{:?}", v), "[null]");

        v.shift(0);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 0);
        assert!(!v.is_empty());
        assert!(v[0].is_empty());
        assert_eq!(format!("{:?}", v), "[null]");

        v.shift(2);
        assert_eq!(v.len(), 0);
        assert_eq!(v.total_len(), 0);
        assert!(v.is_empty());
        assert_eq!(format!("{:?}", v), "[]");

        v.push(&[0xA]);
        v.push(&[0xB]);
        v.push(&[0xC]);
        v.push(&[0xD, 0xE]);
        v.push(&[]);
        v.push(&[]);
        assert_eq!(v.len(), 6);
        assert_eq!(v.total_len(), 5);
        assert!(!v.is_empty());
        assert_eq!(format!("{:?}", v), "[0A, 0B, 0C, 0D0E, null, null]");

        v.truncate(5);
        assert_eq!(v.len(), 5);
        assert_eq!(v.total_len(), 5);
        assert!(!v.is_empty());
        assert_eq!(format!("{:?}", v), "[0A, 0B, 0C, 0D0E, null]");

        v.shift(4);
        assert_eq!(v.len(), 1);
        assert_eq!(v.total_len(), 0);
        assert!(!v.is_empty());
        assert_eq!(format!("{:?}", v), "[null]");

        v.concat_extend(&[vec![0xAC, 0xBB, 0x00], vec![], vec![0xCC]]);
        assert_eq!(v.len(), 2);
        assert_eq!(v.total_len(), 4);
        assert!(!v.is_empty());
        assert_eq!(format!("{:?}", v), "[null, ACBB00CC]");

        let t: Vec<Vec<u8>> = Vec::new();
        v.concat_extend(t);
        assert_eq!(v.len(), 3);
        assert_eq!(v.total_len(), 4);
        assert!(!v.is_empty());
        assert_eq!(format!("{:?}", v), "[null, ACBB00CC, null]");
    }

    #[test]
    fn test_copy_from() {
        let mut v1 = BufferVec::new();
        v1.push(&[]);
        v1.push(&[0xAA, 0xBB, 0x0C]);
        v1.push(&[]);
        v1.push(&[0x00]);

        let mut v2 = BufferVec::new();
        v2.push(&[]);
        v2.push(&[]);

        let mut v3 = v1.clone();
        v3.copy_from(&v2);
        assert_eq!(v3.len(), 6);
        assert_eq!(v3.total_len(), 4);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C, null, 00, null, null]");

        v3.truncate(3);
        assert_eq!(v3.len(), 3);
        assert_eq!(v3.total_len(), 3);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C, null]");

        v3.push(&[]);
        v3.push(&[0x00]);
        assert_eq!(v3.len(), 5);
        assert_eq!(v3.total_len(), 4);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C, null, null, 00]");

        let mut v3 = v2.clone();
        v3.copy_from(&v1);
        assert_eq!(v3.len(), 6);
        assert_eq!(v3.total_len(), 4);
        assert_eq!(format!("{:?}", v3), "[null, null, null, AABB0C, null, 00]");

        v3.pop();
        v3.shift(0);
        assert_eq!(v3.len(), 5);
        assert_eq!(v3.total_len(), 3);
        assert_eq!(format!("{:?}", v3), "[null, null, null, AABB0C, null]");

        let mut v3 = v2;
        v3.shift(2);
        v3.copy_from(&v1);
        assert_eq!(v3.len(), 4);
        assert_eq!(v3.total_len(), 4);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C, null, 00]");

        v3.shift(4);
        assert_eq!(v3.len(), 0);
        assert_eq!(v3.total_len(), 0);
        assert_eq!(format!("{:?}", v3), "[]");

        let mut v1 = BufferVec::new();
        v1.push(&[]);
        v1.push(&[0xAA, 0xBB, 0x0C]);

        let mut v2 = BufferVec::new();
        v2.push(&[0x0C, 0x00]);
        v2.push(&[]);

        let mut v3 = v2.clone();
        v3.copy_n_from(&v1, 0);
        assert_eq!(v3.len(), 2);
        assert_eq!(v3.total_len(), 2);
        assert_eq!(format!("{:?}", v3), "[0C00, null]");

        v3.push(&[0xAA]);
        assert_eq!(v3.len(), 3);
        assert_eq!(v3.total_len(), 3);
        assert_eq!(format!("{:?}", v3), "[0C00, null, AA]");

        let mut v3 = v2.clone();
        v3.copy_n_from(&v1, 1);
        assert_eq!(v3.len(), 3);
        assert_eq!(v3.total_len(), 2);
        assert_eq!(format!("{:?}", v3), "[0C00, null, null]");

        v3.push(&[0xAA]);
        assert_eq!(v3.len(), 4);
        assert_eq!(v3.total_len(), 3);
        assert_eq!(format!("{:?}", v3), "[0C00, null, null, AA]");

        v3.extend(&[0xAA, 0xAB, 0xCC]);
        assert_eq!(v3.len(), 5);
        assert_eq!(v3.total_len(), 6);
        assert_eq!(format!("{:?}", v3), "[0C00, null, null, AA, AAABCC]");

        v3.extend(&[]);
        assert_eq!(v3.len(), 6);
        assert_eq!(v3.total_len(), 6);
        assert_eq!(format!("{:?}", v3), "[0C00, null, null, AA, AAABCC, null]");

        let mut v3 = v1.clone();
        v3.copy_n_from(&v2, 1);
        assert_eq!(v3.len(), 3);
        assert_eq!(v3.total_len(), 5);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C, 0C00]");

        v3.pop();
        assert_eq!(v3.len(), 2);
        assert_eq!(v3.total_len(), 3);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C]");

        let mut v3 = v1;
        v3.copy_n_from(&v2, 2);
        assert_eq!(v3.len(), 4);
        assert_eq!(v3.total_len(), 5);
        assert_eq!(format!("{:?}", v3), "[null, AABB0C, 0C00, null]");

        v3.shift(2);
        assert_eq!(v3.len(), 2);
        assert_eq!(v3.total_len(), 2);
        assert_eq!(format!("{:?}", v3), "[0C00, null]");

        v3.copy_n_from(&v2, 5);
        assert_eq!(v3.len(), 4);
        assert_eq!(v3.total_len(), 4);
        assert_eq!(format!("{:?}", v3), "[0C00, null, 0C00, null]");

        v3.pop();
        assert_eq!(v3.len(), 3);
        assert_eq!(v3.total_len(), 4);
        assert_eq!(format!("{:?}", v3), "[0C00, null, 0C00]");
    }

    #[test]
    fn test_retain() {
        let mut v = BufferVec::new();
        assert_eq!(format!("{:?}", v), "[]");

        v.retain_by_array(&[]);
        assert_eq!(format!("{:?}", v), "[]");

        v.push(&[]);
        assert_eq!(format!("{:?}", v), "[null]");

        v.retain_by_array(&[true]);
        assert_eq!(format!("{:?}", v), "[null]");

        v.retain_by_array(&[false]);
        assert_eq!(format!("{:?}", v), "[]");

        v.push(&[0xAA, 0x00]);
        v.push(&[]);
        assert_eq!(format!("{:?}", v), "[AA00, null]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[true, true]);
        assert_eq!(format!("{:?}", v2), "[AA00, null]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[true, false]);
        assert_eq!(format!("{:?}", v2), "[AA00]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[false, true]);
        assert_eq!(format!("{:?}", v2), "[null]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[false, false]);
        assert_eq!(format!("{:?}", v2), "[]");

        v.push(&[]);
        v.push(&[0xBB, 0x00, 0xA0]);
        assert_eq!(format!("{:?}", v), "[AA00, null, null, BB00A0]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[true, true, false, true]);
        assert_eq!(format!("{:?}", v2), "[AA00, null, BB00A0]");

        v2.copy_n_from(&v, 2);
        assert_eq!(format!("{:?}", v2), "[AA00, null, BB00A0, AA00, null]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[true, false, false, true]);
        assert_eq!(format!("{:?}", v2), "[AA00, BB00A0]");

        v2.shift(1);
        assert_eq!(format!("{:?}", v2), "[BB00A0]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[false, false, true, true]);
        assert_eq!(format!("{:?}", v2), "[null, BB00A0]");

        v2.push(&[]);
        assert_eq!(format!("{:?}", v2), "[null, BB00A0, null]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[true, true, true, false]);
        assert_eq!(format!("{:?}", v2), "[AA00, null, null]");

        v2.shift(2);
        assert_eq!(format!("{:?}", v2), "[null]");

        let mut v2 = v.clone();
        v2.retain_by_array(&[false, true, true, false]);
        assert_eq!(format!("{:?}", v2), "[null, null]");

        v2.pop();
        v2.copy_from(&v);
        assert_eq!(format!("{:?}", v2), "[null, AA00, null, null, BB00A0]");

        let mut v2 = v;
        v2.retain_by_array(&[false, false, false, true]);
        assert_eq!(format!("{:?}", v2), "[BB00A0]");

        v2.pop();
        assert_eq!(format!("{:?}", v2), "[]");
    }

    #[test]
    fn test_iter() {
        let mut v = BufferVec::new();
        v.push(&[]);
        v.push(&[0xAA, 0xBB, 0x0C]);
        v.push(&[]);
        v.push(&[]);
        v.push(&[0x00]);
        v.push(&[]);

        let mut it = v.iter();
        assert_eq!(it.count(), 6);
        assert!(it.next().unwrap().is_empty());
        assert_eq!(it.count(), 5);
        assert!(it.last().unwrap().is_empty());

        let mut it = v.iter();
        assert!(it.next().unwrap().is_empty());
        assert_eq!(it.count(), 5);
        assert_eq!(it.next().unwrap(), &[0xAA, 0xBB, 0x0C]);
        assert_eq!(it.count(), 4);
        assert_eq!(it.nth(2).unwrap(), &[0x00]);
        assert_eq!(it.count(), 1);
        assert!(it.next().unwrap().is_empty());
        assert_eq!(it.count(), 0);
        assert_eq!(it.next(), None);
        assert_eq!(it.last(), None);

        let mut it = v.iter();
        assert!(it.nth(3).unwrap().is_empty());
        assert_eq!(it.count(), 2);
        assert_eq!(it.next().unwrap(), &[0x00]);
        assert_eq!(it.count(), 1);
        assert!(it.next().unwrap().is_empty());
        assert_eq!(it.count(), 0);
        assert_eq!(it.next(), None);
        assert_eq!(it.count(), 0);
        assert_eq!(it.next(), None);
        assert_eq!(it.last(), None);

        let mut it = v.iter();
        assert!(it.nth(5).unwrap().is_empty());
        assert_eq!(it.count(), 0);
        assert_eq!(it.next(), None);
        assert_eq!(it.nth(3), None);
        assert_eq!(it.count(), 0);
        assert_eq!(it.last(), None);

        let mut it = v.iter();
        assert_eq!(it.nth(6), None);
        assert_eq!(it.count(), 0);
        assert_eq!(it.next(), None);
        assert_eq!(it.nth(1), None);
    }
}
