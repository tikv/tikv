// Copyright 2016 PingCAP, Inc.
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

use alloc::raw_vec::RawVec;
use std::fmt::{self, Debug, Formatter};
use std::io::{BufRead, ErrorKind, Read, Result, Write};
use std::{cmp, mem, ptr, slice};

use util::escape;

/// `PipeBuffer` is useful when you want to move data from `Write` to a `Read` or vice versa.
pub struct PipeBuffer {
    // the index of the first byte of written data.
    start: usize,
    // the index of buf that new data should be written in.
    end: usize,
    buf: RawVec<u8>,
}

impl PipeBuffer {
    pub fn new(capacity: usize) -> PipeBuffer {
        PipeBuffer {
            start: 0,
            end: 0,
            // one extra byte to indicate if buf is full or empty.
            buf: RawVec::with_capacity(capacity + 1),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        if self.end >= self.start {
            self.end - self.start
        } else {
            self.buf.cap() - self.start + self.end
        }
    }

    #[inline]
    unsafe fn buf_as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.buf.ptr(), self.buf.cap())
    }

    #[inline]
    unsafe fn buf_as_slice_mut(&mut self) -> &mut [u8] {
        slice::from_raw_parts_mut(self.buf.ptr(), self.buf.cap())
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.cap() - 1
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    #[inline]
    #[allow(len_zero)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the written buf.
    fn slice(&self) -> (&[u8], &[u8]) {
        unsafe {
            let buf = self.buf_as_slice();
            if self.end >= self.start {
                (&buf[self.start..self.end], &[])
            } else {
                (&buf[self.start..], &buf[..self.end])
            }
        }
    }

    /// Get the not written buf.
    fn slice_append(&mut self) -> (&mut [u8], &mut [u8]) {
        if self.is_full() {
            return (&mut [], &mut []);
        }
        unsafe {
            let start = self.start;
            let end = self.end;
            let cap = self.capacity();
            let buf = self.buf_as_slice_mut();

            if start == 0 {
                (&mut buf[end..cap], &mut [])
            } else if start <= end {
                let (right, left) = buf.split_at_mut(end);
                let (right, _) = right.split_at_mut(start - 1);
                (left, right)
            } else {
                (&mut buf[end..start - 1], &mut [])
            }
        }
    }

    /// Ensure the capacity of inner buf not less than `capacity`.
    ///
    /// If capacity is larger than inner buf, a larger buffer will be reallocated.
    /// Allocated buffer's capacity doesn't have to be equal to specified value.
    pub fn ensure(&mut self, capacity: usize) {
        if capacity <= self.capacity() {
            return;
        }

        let cap = self.buf.cap();
        self.buf.reserve(cap, capacity + 1 - cap);
        let new_cap = self.buf.cap();

        // After the buf being extended, we have to make the written data layout correctly.
        unsafe {
            if self.start <= self.end {
                // written data are linear, no need to move.
                return;
            } else if new_cap - cap > self.end && self.end <= cap - self.start {
                // left part can be fit in new buf and is shorter.
                // [ll...rrr...] -> [.....rrrll.]
                let left = self.buf.ptr();
                let new_pos = self.buf.ptr().offset(cap as isize);
                ptr::copy_nonoverlapping(left, new_pos, self.end);
                self.end += cap;
            } else {
                // [lll..rr..] -> [lll....rr]
                let right = self.buf.ptr().offset(self.start as isize);
                self.start = new_cap - (cap - self.start);
                let new_pos = self.buf.ptr().offset(self.start as isize);
                if self.start >= cap {
                    ptr::copy_nonoverlapping(right, new_pos, new_cap - self.start);
                } else {
                    ptr::copy(right, new_pos, new_cap - self.start);
                }
            }
        }
    }

    /// shrink the inner buf to specified capacity.
    pub fn shrink_to(&mut self, len: usize) {
        assert!(len < self.capacity());

        let new_cap = len + 1;
        // Before actually shrinking, written data need to be fit in target capacity.
        // If target capacity is too short, extra data should be truncated.
        let to_keep = cmp::min(len, self.len());
        unsafe {
            if self.start <= self.end {
                let dest = self.buf.ptr();
                if self.start >= len {
                    // [...|.ll.] -> [ll.]
                    let source = self.buf.ptr().offset(self.start as isize);
                    self.start = 0;
                    self.end = to_keep;
                    ptr::copy_nonoverlapping(source, dest, to_keep);
                } else {
                    // if we just move `self.end`, we can still use `copy_nonoverlapping`.
                    let right_len = new_cap - self.start;
                    if right_len >= to_keep {
                        // [.rr|...] -> [.rr] or [rrr|r] -> [rr.]
                        self.end = self.start + to_keep;
                        if self.end == new_cap {
                            self.end = 0;
                        }
                    } else {
                        // [..r|l.] -> [l.r]
                        self.end = to_keep - right_len;
                        let source = self.buf.ptr().offset(new_cap as isize);
                        ptr::copy_nonoverlapping(source, dest, self.end);
                    }
                }
            } else {
                let source = self.buf.ptr().offset(self.start as isize);
                let right_len = self.buf.cap() - self.start;
                if right_len >= to_keep {
                    // [ll...|.rrrrr] -> [rrrr.]
                    self.start = 0;
                    self.end = to_keep;
                    ptr::copy(source, self.buf.ptr(), to_keep);
                } else {
                    // [ll...|.rr] -> [ll.rr]
                    self.start = new_cap - right_len;
                    if self.end >= self.start {
                        self.end = self.start - 1;
                    }
                    ptr::copy(
                        source,
                        self.buf.ptr().offset(self.start as isize),
                        right_len,
                    );
                }
            }
        }
        self.buf.shrink_to_fit(new_cap);
    }

    /// Read data from `r` and fill inner buf.
    ///
    /// Please note that the buffer size will not change automatically,
    /// you have to call capacity-related method to adjust it.
    pub fn read_from<R: Read>(&mut self, r: &mut R) -> Result<usize> {
        let mut end = self.end;
        let mut readed;
        {
            let (left, right) = self.slice_append();
            match r.read(left) {
                Ok(l) => readed = l,
                Err(e) => {
                    return if e.kind() == ErrorKind::WouldBlock {
                        Ok(0)
                    } else {
                        Err(e)
                    };
                }
            }
            end += readed;
            if readed == left.len() && !right.is_empty() {
                // Can't return error because r has been read into left.
                if let Ok(l) = r.read(right) {
                    end = l;
                    readed += l;
                }
            }
        }
        if end == self.buf.cap() {
            self.end = 0;
        } else {
            self.end = end;
        }
        Ok(readed)
    }

    /// Write the inner buffer to `w`.
    pub fn write_to<W: Write>(&mut self, w: &mut W) -> Result<usize> {
        let mut start = self.start;
        let mut written;
        {
            let (left, right) = self.slice();
            match w.write(left) {
                Ok(l) => written = l,
                Err(e) => {
                    return if e.kind() == ErrorKind::WouldBlock {
                        Ok(0)
                    } else {
                        Err(e)
                    };
                }
            }
            start += written;
            if written == left.len() && !right.is_empty() {
                // Can't return error because left has written into w.
                if let Ok(l) = w.write(right) {
                    start = l;
                    written += l;
                }
            }
        }
        if start == self.buf.cap() {
            self.start = 0;
        } else {
            self.start = start;
        }
        Ok(written)
    }

    pub fn write_all_to<W: Write>(&mut self, w: &mut W) -> Result<()> {
        {
            let (left, right) = self.slice();
            w.write_all(left)?;
            w.write_all(right)?;
        }
        self.end = self.start;
        Ok(())
    }
}

impl Read for PipeBuffer {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize> {
        self.write_to(&mut buf)
    }
}

impl BufRead for PipeBuffer {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        let (left, _) = self.slice();
        Ok(left)
    }

    fn consume(&mut self, amt: usize) {
        assert!(amt <= self.len());
        if self.start <= self.end {
            self.start += amt;
        } else {
            let right_len = self.buf.cap() - self.start;
            if right_len > amt {
                self.start += amt;
            } else {
                self.start = amt - right_len;
            }
        }
    }
}

impl Write for PipeBuffer {
    fn write(&mut self, mut buf: &[u8]) -> Result<usize> {
        let min_cap = self.len() + buf.len();
        self.ensure(min_cap);
        self.read_from(&mut buf)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl PartialEq for PipeBuffer {
    fn eq(&self, right: &PipeBuffer) -> bool {
        if self.len() != right.len() {
            return false;
        }

        let (mut l1, mut r1) = self.slice();
        let (mut l2, mut r2) = right.slice();
        if l1.len() > l2.len() {
            mem::swap(&mut l1, &mut l2);
            mem::swap(&mut r1, &mut r2);
        }
        l1 == &l2[..l1.len()]
            && r1[..l2.len() - l1.len()] == l2[l1.len()..]
            && &r1[l2.len() - l1.len()..] == r2
    }
}

impl<'a> PartialEq<&'a [u8]> for PipeBuffer {
    fn eq(&self, right: &&'a [u8]) -> bool {
        if self.len() != right.len() {
            return false;
        }

        let (l, r) = self.slice();
        l == &right[..l.len()] && r == &right[l.len()..]
    }
}

impl Debug for PipeBuffer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "PipeBuffer [start: {}, end: {}, buf: {}]",
            self.start,
            self.end,
            escape(unsafe { self.buf_as_slice() })
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::*;

    use rand::{self, Rng};

    use super::*;

    fn new_sample(count: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        assert!(count <= 256);
        let mut samples = rand::sample(&mut rng, 0..255, count);
        rng.shuffle(&mut samples);
        samples
    }

    fn new_pipe_buffer(cap: usize) -> PipeBuffer {
        let s = PipeBuffer::new(cap);
        let samples = new_sample(cap);
        unsafe { ptr::copy_nonoverlapping(samples.as_ptr(), s.buf.ptr(), cap) };
        s
    }

    #[test]
    fn test_read_from() {
        let mut s = new_pipe_buffer(25);

        let cap = s.capacity();
        let padding = new_sample(cap);
        for len in 0..cap + 1 {
            let expected = new_sample(len);

            for pos in 0..cap + 1 {
                for l in 0..len + 1 {
                    s.start = pos;
                    s.end = pos;

                    let mut input = &expected[0..l];
                    assert_eq!(l, s.read_from(&mut input).unwrap());
                    assert_eq!(s, &expected[0..l]);
                    input = &expected[l..];
                    assert_eq!(len - l, s.read_from(&mut input).unwrap());
                    assert_ne!(s.start, s.buf.cap());
                    assert_ne!(s.end, s.buf.cap());
                    assert_eq!(s, expected.as_slice());

                    input = padding.as_slice();
                    assert_eq!(cap - len, s.read_from(&mut input).unwrap());
                    assert_ne!(s.start, s.buf.cap());
                    assert_ne!(s.end, s.buf.cap());
                    let mut exp = expected.clone();
                    exp.extend_from_slice(&padding[..cap - len]);
                    assert_eq!(s, exp.as_slice());
                }
            }
        }
    }

    #[test]
    fn test_write_to() {
        let mut s = new_pipe_buffer(25);

        let cap = s.capacity();
        for len in 0..cap + 1 {
            let expected = new_sample(len);

            for pos in 0..cap + 1 {
                for l in 0..len + 1 {
                    s.start = pos;
                    s.end = pos;

                    let mut input = expected.as_slice();
                    assert_eq!(len, s.read_from(&mut input).unwrap());

                    let mut w = vec![0; l];
                    {
                        let mut buf = w.as_mut_slice();
                        assert_eq!(l, s.write_to(&mut buf).unwrap());
                        assert_ne!(s.start, s.buf.cap());
                        assert_ne!(s.end, s.buf.cap());
                    }
                    assert_eq!(w, &expected[..l]);
                    assert_eq!(s, &expected[l..]);

                    let mut w = new_sample(cap);
                    assert_eq!(len - l, s.read(&mut w).unwrap());
                    assert_ne!(s.start, s.buf.cap());
                    assert_ne!(s.end, s.buf.cap());
                    assert_eq!(&w[..len - l], &expected[l..]);
                }
            }
        }
    }

    #[test]
    fn test_buf_read() {
        let mut s = new_pipe_buffer(25);

        let cap = s.capacity();
        for len in 0..cap + 1 {
            let expected = new_sample(len);

            for pos in 0..cap + 1 {
                for l in 0..len + 1 {
                    s.start = pos;
                    s.end = pos;

                    let mut input = expected.as_slice();
                    assert_eq!(len, s.read_from(&mut input).unwrap());

                    assert_eq!(s.len(), len);
                    if len == 0 {
                        assert!(s.fill_buf().unwrap().is_empty());
                    } else {
                        assert!(!s.fill_buf().unwrap().is_empty());
                    }
                    s.consume(l);
                    assert_eq!(s.len(), len - l);
                    if l == len {
                        assert!(s.fill_buf().unwrap().is_empty());
                    } else {
                        assert!(!s.fill_buf().unwrap().is_empty());
                    }
                }
            }
        }
    }

    #[test]
    fn test_shrink_to() {
        let cap = 25;
        for l in 0..cap + 1 {
            let expect = new_sample(l);

            for pos in 0..cap + 1 {
                for shrink in 0..cap {
                    let mut s = new_pipe_buffer(cap);
                    s.start = pos;
                    s.end = pos;

                    let mut input = expect.as_slice();
                    assert_eq!(l, s.read_from(&mut input).unwrap());
                    s.shrink_to(shrink);
                    assert_ne!(s.start, s.buf.cap());
                    assert_ne!(s.end, s.buf.cap());

                    assert_eq!(shrink, s.capacity());
                    if shrink > l {
                        assert_eq!(s, expect.as_slice());
                    } else {
                        assert_eq!(
                            s,
                            &expect[..shrink],
                            "l: {} pos: {} shrink: {}",
                            l,
                            pos,
                            shrink
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_ensure() {
        let cap = 25;
        for l in 0..cap + 1 {
            let expect = new_sample(l);

            for pos in 0..cap + 1 {
                for init in 0..cap + 1 {
                    let mut s = new_pipe_buffer(cap);
                    s.start = pos;
                    s.end = pos;

                    let example = new_sample(init);
                    let mut input = example.as_slice();
                    assert_eq!(init, s.read_from(&mut input).unwrap());
                    assert_eq!(s, example.as_slice());
                    s.ensure(init + l);
                    assert_ne!(s.start, s.buf.cap());
                    assert_ne!(s.end, s.buf.cap());
                    assert_eq!(s, example.as_slice());
                    input = expect.as_slice();

                    assert_eq!(l, s.read_from(&mut input).unwrap());
                    let mut exp = example.clone();
                    exp.extend_from_slice(&expect);
                    assert_eq!(s, exp.as_slice());
                }
            }
        }
    }

    #[test]
    fn test_write_all() {
        let mut s = new_pipe_buffer(25);
        let example = new_sample(25);
        s.write_all(&example).unwrap();
        let mut buf: Vec<u8> = new_sample(20);
        {
            let mut buf_w = buf.as_mut_slice();
            assert!(s.write_all_to(&mut buf_w).is_err());
        }
        {
            // write all failed should not change buffer content.
            let mut buf_w = buf.as_mut_slice();
            assert!(s.write_all_to(&mut buf_w).is_err());
        }
        buf = new_sample(25);
        let mut buf_w = buf.as_mut_slice();
        assert!(s.write_all_to(&mut buf_w).is_ok());
        assert!(s.is_empty());
    }
}
