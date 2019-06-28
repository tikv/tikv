// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/// A trait to provide sequential read over a memory buffer.
///
/// The memory buffer can be `&[u8]` or `std::io::Cursor<AsRef<[u8]>>`.
pub trait BufferReader {
    /// Returns a slice starting at current position.
    ///
    /// The returned slice can be empty.
    fn bytes(&self) -> &[u8];

    /// Advances the position of internal cursor.
    ///
    /// # Panics
    ///
    /// This function may panic in some implementors when remaining space
    /// is not large enough to advance.
    fn advance(&mut self, count: usize);
}

impl<T: AsRef<[u8]>> BufferReader for std::io::Cursor<T> {
    fn bytes(&self) -> &[u8] {
        let pos = self.position() as usize;
        let slice = self.get_ref().as_ref();
        slice.get(pos..).unwrap_or(&[])
    }

    fn advance(&mut self, count: usize) {
        let mut pos = self.position();
        pos += count as u64;
        self.set_position(pos);
    }
}

impl<'a> BufferReader for &'a [u8] {
    fn bytes(&self) -> &[u8] {
        self
    }

    fn advance(&mut self, count: usize) {
        *self = &self[count..]
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

/// A trait to provide sequential write over a fixed size
/// or dynamic size memory buffer.
///
/// The memory buffer can be `std::io::Cursor<AsRef<[u8]>>` or `&mut [u8]`,
/// which is fixed sized, or `Vec<u8>`, which is dynamically sized.
pub trait BufferWriter {
    /// Returns a mutable slice starting at current position.
    ///
    /// The caller may hint the underlying buffer to grow according to `size`
    /// if the underlying buffer is dynamically sized (i.e. is capable to grow).
    ///
    /// The size of the returned slice may be less than `size` given. For example,
    /// when underlying buffer is fixed sized and there is no enough space any more.
    ///
    /// The returned mutable slice is for writing only and should be never used for
    /// reading since it might contain uninitialized memory when underlying buffer
    /// is dynamically sized. For this reason, this function is marked `unsafe`.
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8];

    /// Advances the position of internal cursor for a previous write.
    ///
    /// The caller should ensure that advanced positions have been all written
    /// previously. If the cursor is moved beyond actually written data, it will
    /// leave uninitialized memory. For this reason, this function is marked
    /// `unsafe`.
    ///
    /// # Panics
    ///
    /// This function may panic in some implementors when remaining space
    /// is not large enough to advance.
    unsafe fn advance_mut(&mut self, count: usize);
}

impl<T: AsMut<[u8]>> BufferWriter for std::io::Cursor<T> {
    unsafe fn bytes_mut(&mut self, _size: usize) -> &mut [u8] {
        // `size` is ignored since this buffer is not capable to grow.
        let pos = self.position() as usize;
        let slice = self.get_mut().as_mut();
        slice.get_mut(pos..).unwrap_or(&mut [])
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        let mut pos = self.position();
        pos += count as u64;
        self.set_position(pos);
    }
}

impl<'a> BufferWriter for &'a mut [u8] {
    unsafe fn bytes_mut(&mut self, _size: usize) -> &mut [u8] {
        self
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        let original_self = std::mem::replace(self, &mut []);
        *self = &mut original_self[count..];
    }
}

impl BufferWriter for Vec<u8> {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8] {
        // Return a slice starting from `self.len()` position and
        // has at least `size` space.

        // Ensure returned slice has enough space
        self.reserve(size);
        let ptr = self.as_mut_ptr();
        &mut std::slice::from_raw_parts_mut(ptr, self.capacity())[self.len()..]
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

#[cfg(test)]
mod tests {
    use rand;

    use super::*;

    #[test]
    fn test_buffer_reader_cursor() {
        let mut base: Vec<u8> = Vec::with_capacity(40);
        for _ in 0..40 {
            base.push(rand::random());
        }

        let mut buffer = std::io::Cursor::new(base.clone());

        assert_eq!(buffer.bytes(), &base[0..40]);
        buffer.advance(13);
        assert_eq!(buffer.position(), 13);
        assert_eq!(buffer.bytes(), &base[13..40]);
        buffer.advance(5);
        assert_eq!(buffer.position(), 18);
        assert_eq!(buffer.bytes(), &base[18..40]);

        // Reset to valid position
        buffer.set_position(7);
        assert_eq!(buffer.bytes(), &base[7..40]);

        buffer.advance(31);
        assert_eq!(buffer.position(), 38);
        assert_eq!(buffer.bytes(), &base[38..40]);

        buffer.advance(2);
        assert_eq!(buffer.position(), 40);
        assert_eq!(buffer.bytes(), &base[40..40]);

        // Advance exceeds len
        buffer.advance(7);
        assert_eq!(buffer.position(), 47);
        assert_eq!(buffer.bytes(), &base[40..40]);

        // Reset to valid position
        buffer.set_position(0);
        assert_eq!(buffer.bytes(), &base[0..40]);

        // Reset to invalid position
        buffer.set_position(100);
        assert_eq!(buffer.bytes(), &base[40..40]);
    }

    #[test]
    fn test_buffer_reader_slice() {
        let mut base: Vec<u8> = Vec::with_capacity(40);
        for _ in 0..40 {
            base.push(rand::random());
        }

        let buffer = base.clone();
        let mut buffer = buffer.as_slice();

        assert_eq!(buffer, &base[0..40]);
        assert_eq!(buffer.bytes(), &base[0..40]);

        buffer.advance(13);
        assert_eq!(buffer, &base[13..40]);
        assert_eq!(buffer.bytes(), &base[13..40]);

        buffer.advance(5);
        assert_eq!(buffer, &base[18..40]);
        assert_eq!(buffer.bytes(), &base[18..40]);

        buffer.advance(22);
        assert_eq!(buffer, &base[40..40]);
        assert_eq!(buffer.bytes(), &base[40..40]);
    }

    #[test]
    fn test_buffer_writer_cursor() {
        unsafe {
            let mut base: Vec<u8> = Vec::with_capacity(40);
            for _ in 0..40 {
                base.push(rand::random());
            }

            // A series of bytes to write
            let mut base_write: Vec<u8> = Vec::with_capacity(100);
            for _ in 0..100 {
                base_write.push(rand::random());
            }

            let mut buffer = std::io::Cursor::new(base.clone());

            buffer.bytes_mut(13)[..13].clone_from_slice(&base_write[0..13]);
            buffer.advance_mut(13);
            assert_eq!(&buffer.get_ref()[0..13], &base_write[0..13]);
            assert_eq!(&buffer.get_ref()[13..], &base[13..]);
            assert_eq!(buffer.position(), 13);

            // Acquire 10, only write 5
            buffer.bytes_mut(10)[..5].clone_from_slice(&base_write[13..18]);
            buffer.advance_mut(5);
            assert_eq!(&buffer.get_ref()[0..18], &base_write[0..18]);
            assert_eq!(&buffer.get_ref()[18..], &base[18..]);
            assert_eq!(buffer.position(), 18);

            // Reset to valid position
            buffer.set_position(7);
            buffer.bytes_mut(16)[..16].clone_from_slice(&base_write[18..34]);
            buffer.advance_mut(16);
            assert_eq!(&buffer.get_ref()[0..7], &base_write[0..7]);
            assert_eq!(&buffer.get_ref()[7..23], &base_write[18..34]);
            assert_eq!(&buffer.get_ref()[23..], &base[23..]);
            assert_eq!(buffer.position(), 23);

            buffer.bytes_mut(15)[..15].clone_from_slice(&base_write[34..49]);
            buffer.advance_mut(15);
            assert_eq!(&buffer.get_ref()[0..7], &base_write[0..7]);
            assert_eq!(&buffer.get_ref()[7..38], &base_write[18..49]);
            assert_eq!(&buffer.get_ref()[38..], &base[38..]);
            assert_eq!(buffer.position(), 38);

            // Acquire a slice more than available
            assert_eq!(buffer.bytes_mut(5).len(), 2);
            buffer.bytes_mut(2)[..2].clone_from_slice(&base_write[49..51]);
            buffer.advance_mut(2);
            assert_eq!(&buffer.get_ref()[0..7], &base_write[0..7]);
            assert_eq!(&buffer.get_ref()[7..38], &base_write[18..49]);
            assert_eq!(&buffer.get_ref()[7..40], &base_write[18..51]);
            assert_eq!(buffer.position(), 40);

            // Reset to valid position
            buffer.set_position(0);
            buffer.bytes_mut(5)[..5].clone_from_slice(&base_write[51..56]);
            buffer.advance_mut(5);
            assert_eq!(&buffer.get_ref()[0..5], &base_write[51..56]);
            assert_eq!(&buffer.get_ref()[5..7], &base_write[5..7]);
            assert_eq!(&buffer.get_ref()[7..40], &base_write[18..51]);
            assert_eq!(buffer.position(), 5);

            // Reset to invalid position
            buffer.set_position(100);
            assert_eq!(buffer.bytes_mut(1).len(), 0);
            assert_eq!(&buffer.get_ref()[0..5], &base_write[51..56]);
            assert_eq!(&buffer.get_ref()[5..7], &base_write[5..7]);
            assert_eq!(&buffer.get_ref()[7..40], &base_write[18..51]);
        }
    }

    #[test]
    fn test_buffer_writer_slice() {
        unsafe {
            let mut base: Vec<u8> = Vec::with_capacity(40);
            for _ in 0..40 {
                base.push(rand::random());
            }

            // A series of bytes to write
            let mut base_write: Vec<u8> = Vec::with_capacity(100);
            for _ in 0..100 {
                base_write.push(rand::random());
            }

            let mut buffer = base.clone();
            let mut buffer = buffer.as_mut_slice();

            buffer.bytes_mut(13)[..13].clone_from_slice(&base_write[0..13]);
            assert_eq!(&buffer[0..13], &base_write[0..13]);
            assert_eq!(&buffer[13..], &base[13..]);
            buffer.advance_mut(13);

            // Acquire 10, only write 5.
            buffer.bytes_mut(10)[..5].clone_from_slice(&base_write[13..18]);
            assert_eq!(&buffer[0..5], &base_write[13..18]);
            assert_eq!(&buffer[5..], &base[18..]);
            buffer.advance_mut(5);

            buffer.bytes_mut(22)[..22].clone_from_slice(&base_write[18..40]);
            assert_eq!(&buffer[0..22], &base_write[18..40]);
            assert_eq!(&buffer[22..], &base[40..]);
            buffer.advance_mut(22);

            assert_eq!(buffer, &base[40..]);
        }
    }

    #[test]
    fn test_buffer_writer_vec() {
        unsafe {
            // A series of bytes to write
            let mut base_write: Vec<u8> = Vec::with_capacity(100);
            for _ in 0..100 {
                base_write.push(rand::random());
            }

            let mut buffer: Vec<u8> = Vec::with_capacity(20);
            buffer.bytes_mut(13)[..13].clone_from_slice(&base_write[0..13]);
            buffer.advance_mut(13);
            assert_eq!(&buffer[0..13], &base_write[0..13]);
            assert_eq!(buffer.len(), 13);

            // Vec remaining 7, acquire 10, only write 5
            assert!(buffer.bytes_mut(10).len() >= 10);
            buffer.bytes_mut(10)[..5].clone_from_slice(&base_write[13..18]);
            buffer.advance_mut(5);
            assert_eq!(&buffer[0..18], &base_write[0..18]);
            assert_eq!(buffer.len(), 18);

            // Reset len
            buffer.set_len(7);
            buffer.bytes_mut(16)[..16].clone_from_slice(&base_write[18..34]);
            buffer.advance_mut(16);
            assert_eq!(&buffer[0..7], &base_write[0..7]);
            assert_eq!(&buffer[7..23], &base_write[18..34]);
            assert_eq!(buffer.len(), 23);

            buffer.bytes_mut(15)[..15].clone_from_slice(&base_write[34..49]);
            buffer.advance_mut(15);
            assert_eq!(&buffer[0..7], &base_write[0..7]);
            assert_eq!(&buffer[7..38], &base_write[18..49]);
            assert_eq!(buffer.len(), 38);

            buffer.bytes_mut(2)[..2].clone_from_slice(&base_write[49..51]);
            buffer.advance_mut(2);
            assert_eq!(&buffer[0..7], &base_write[0..7]);
            assert_eq!(&buffer[7..40], &base_write[18..51]);
            assert_eq!(buffer.len(), 40);
        }
    }

    /// Test whether it is safe to store values in `Vec` after `len()`, i.e. during
    /// reallocation these values are copied.
    #[test]
    // FIXME(#4331) Don't ignore this test.
    #[ignore]
    fn test_vec_reallocate() {
        // FIXME: This test, and presumably the WriteBuffer API, relies on
        // unspecified behavior of Vec::reserve (that it copies bytes
        // beyond the length of the vector). It also depends on behavior
        // specific to the malloc implementation (that calling `reserve`
        // with a certain size _always_ reallocates).
        //
        // On at least one tested platform (Linux w/o jemalloc) the
        // expected `reserve` behavior of always reallocating does not
        // hold - malloc is free to realloc in place - so this test is
        // "fuzzy" about exactly what it expects from the allocator;
        // and it generates allocation "noise" to disrupt any
        // predictive analysis in malloc.
        //
        // Note that the test harness for this crate uses jemalloc
        // on platforms where TiKV uses jemalloc.

        let mut in_place_reallocs = 0;
        const MAX_IN_PLACE_REALLOCS: usize = 32;

        for payload_len in 1..1024 {
            let mut payload: Vec<u8> = Vec::with_capacity(payload_len);
            for _ in 0..payload_len {
                payload.push(rand::random());
            }

            // Write payload to space after `len()` before `capacity()`.
            let mut vec: Vec<u8> = Vec::with_capacity(payload_len);
            let vec_ptr = vec.as_ptr();
            unsafe {
                let slice = vec.bytes_mut(payload_len);
                slice[..payload_len].clone_from_slice(payload.as_slice());
            }

            // These are trying to defeat optimizations in malloc that might
            // cause realloc to not actually create a new allocation. See
            // the FIXME above.
            let _alloc_noise: Vec<u8> = Vec::with_capacity(payload_len);
            let _alloc_noise: Vec<u8> = Vec::with_capacity(1);

            // Re-allocate the vector space and ensure that the address is changed.
            vec.reserve(::std::cmp::max(payload_len * 3, 32));

            //assert_ne!(vec_ptr, vec.as_ptr());
            if vec_ptr == vec.as_ptr() {
                in_place_reallocs += 1;
            }

            // Move len() forward and check whether our previous written data exists.
            unsafe {
                vec.advance_mut(payload_len);
            }
            assert_eq!(vec.as_slice(), payload.as_slice());
        }

        if in_place_reallocs > MAX_IN_PLACE_REALLOCS {
            panic!("realloc test realloc enough");
        }
    }
}
