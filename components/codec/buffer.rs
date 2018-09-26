// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/// A trait to provide sequential read over a memory buffer.
///
/// The memory buffer can be `std::io::Cursor<AsRef<[u8]>>`.
pub trait BufferReader {
    /// Returns a slice starting at current position.
    ///
    /// The returned slice can be empty.
    fn bytes(&self) -> &[u8];

    /// Advances the position of internal cursor.
    fn advance(&mut self, count: usize);
}

impl<T: AsRef<[u8]>> BufferReader for ::std::io::Cursor<T> {
    fn bytes(&self) -> &[u8] {
        let pos = self.position() as usize;
        let slice = self.get_ref().as_ref();
        if pos >= slice.len() {
            return Default::default();
        }
        &slice[pos..]
    }

    fn advance(&mut self, count: usize) {
        let mut pos = self.position();
        pos += count as u64;
        self.set_position(pos);
    }
}

impl<'a, T: BufferReader + ?Sized> BufferReader for &'a mut T {
    fn bytes(&self) -> &[u8] {
        (**self).bytes()
    }

    fn advance(&mut self, count: usize) {
        (**self).advance(count)
    }
}

impl<T: BufferReader + ?Sized> BufferReader for Box<T> {
    fn bytes(&self) -> &[u8] {
        (**self).bytes()
    }

    fn advance(&mut self, count: usize) {
        (**self).advance(count)
    }
}

/// A trait to provide sequential write over a fixed size or dynamic size memory buffer.
///
/// The memory buffer can be `std::io::Cursor<AsRef<[u8]>>`, which is fixed sized, or
/// `Vec<u8>`, which is dynamically sized.
pub trait BufferWriter {
    /// Returns a mutable slice starting at current position.
    ///
    /// The caller may hint the underlying buffer to grow according to `size` if the underlying
    /// buffer is dynamically sized (i.e. is capable to grow).
    ///
    /// The size of the returned slice may be less than `size` given. For example, when underlying
    /// buffer is fixed sized and there is no enough space any more.
    ///
    /// The returned mutable slice is for writing only and should be never used for reading
    /// since it might contain uninitialized memory when underlying buffer is dynamically sized.
    /// For this reason, this function is marked `unsafe`.
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8];

    /// Advances the position of internal cursor for a previous write.
    ///
    /// The caller should ensure that advanced positions have been all written previously. If the
    /// cursor is moved beyond actually written data, it will leave uninitialized memory. For this
    /// reason, this function is marked `unsafe`.
    unsafe fn advance_mut(&mut self, count: usize);
}

impl<T: AsMut<[u8]>> BufferWriter for ::std::io::Cursor<T> {
    unsafe fn bytes_mut(&mut self, _size: usize) -> &mut [u8] {
        // `size` is ignored since this buffer is not capable to grow.
        let pos = self.position() as usize;
        let slice = self.get_mut().as_mut();
        if pos >= slice.len() {
            return Default::default();
        }
        &mut slice[pos..]
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        let mut pos = self.position();
        pos += count as u64;
        self.set_position(pos);
    }
}

impl BufferWriter for Vec<u8> {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8] {
        // Return a slice starting from `self.len()` position and has at least `size` space.

        // Ensure returned slice has enough space
        self.reserve(size);
        let ptr = self.as_mut_ptr();
        &mut ::std::slice::from_raw_parts_mut(ptr, self.capacity())[self.len()..]
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        let len = self.len();
        self.set_len(len + count);
    }
}

impl<'a, T: BufferWriter + ?Sized> BufferWriter for &'a mut T {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8] {
        (**self).bytes_mut(size)
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        (**self).advance_mut(count)
    }
}

impl<T: BufferWriter + ?Sized> BufferWriter for Box<T> {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8] {
        (**self).bytes_mut(size)
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        (**self).advance_mut(count)
    }
}
