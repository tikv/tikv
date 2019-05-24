// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::*;

/// A vector like container storing multiple buffers. Each buffer is a `[u8]` slice in
/// arbitrary length.
pub struct BufferVec {
    data: Vec<u8>,
    offsets: Vec<usize>,
}

impl Clone for BufferVec {
    fn clone(&self) -> Self {
        Self {
            data: tikv_util::vec_clone_with_capacity(&self.data),
            offsets: tikv_util::vec_clone_with_capacity(&self.offsets),
        }
    }
}

impl std::fmt::Debug for BufferVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use hex::ToHex;

        write!(f, "[")?;
        let len = self.len();
        if len > 0 {
            for i in 0..len - 1 {
                self[i].as_ref().write_hex_upper(f)?;
                write!(f, ", ")?;
            }
            self[len - 1].as_ref().write_hex_upper(f)?;
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
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
        }
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

    /// Removes the last buffer if there is any.
    #[inline]
    pub fn pop(&mut self) {
        let len = self.len();
        if len > 0 {
            self.data.truncate(self.offsets[len - 1]);
            self.offsets.pop();
        }
    }

    /// Removes the first `n` buffers.
    ///
    /// If `n` >= current length, all buffers will be removed.
    pub fn shift(&mut self, n: usize) {
        let len = self.len();
        if n < len {
            unsafe {
                let begin_offset = self.offsets[len];
                let data_len = self.data.len() - begin_offset;
                let n_len = len - n;
                std::ptr::copy(
                    self.data.as_ptr().add(begin_offset),
                    self.data.as_mut_ptr(),
                    data_len,
                );
                for i in 0..n_len {
                    self.offsets[i] -= begin_offset;
                }
                self.data.set_len(data_len);
                self.offsets.set_len(n_len);
            }
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
    pub fn extend(&mut self, other: &BufferVec) {
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
    pub fn extend_n(&mut self, other: &BufferVec, n: usize) {
        if n >= other.len() {
            self.extend(other);
        } else if n > 0 {
            let base_offset = self.data.len();
            self.data.extend_from_slice(&other.data[..other.offsets[n]]);
            self.offsets.reserve(n);
            for i in 0..n {
                self.offsets.push(other.offsets[i] + base_offset);
            }
        }
    }

    /// Retain the elements according to a boolean array.
    ///
    /// # Panics
    ///
    /// Panics if `retain_arr` is not long enough.
    pub fn retain_by_array(&mut self, retain_arr: &[bool]) {
        unsafe {
            let len = self.len();
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

    #[inline]
    fn index(&self, index: usize) -> &[u8] {
        assert!(index < self.offsets.len());
        if index < self.offsets.len() - 1 {
            &self.data[self.offsets[index]..self.offsets[index + 1]]
        } else {
            &self.data[self.offsets[index]..]
        }
    }
}

pub struct Iter<'a> {
    data: &'a [u8],
    offsets: &'a [usize],
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.nth(0)
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
        if n + 1 < self.offsets.len() {
            let begin_offset = self.offsets[n];
            let end_offset = self.offsets[n + 1];
            self.offsets = &self.offsets[n + 1..];
            Some(&self.data[begin_offset..end_offset])
        } else if n + 1 == self.offsets.len() {
            let begin_offset = self.offsets[n];
            self.offsets = &[];
            Some(&self.data[begin_offset..])
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {}
