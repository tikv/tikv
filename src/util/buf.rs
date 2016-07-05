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

use std::io::{Result, Write};
use std::collections::VecDeque;

use bytes::{ByteBuf, MutByteBuf, alloc};
pub use mio::{TryRead, TryWrite};

// `create_mem_buf` creates the buffer with fixed capacity s.
pub fn create_mem_buf(s: usize) -> MutByteBuf {
    unsafe {
        ByteBuf::from_mem_ref(alloc::heap(s.next_power_of_two()), s as u32, 0, s as u32).flip()
    }
}

pub struct SendBuffer {
    buf: VecDeque<u8>,
}

impl SendBuffer {
    pub fn new(n: usize) -> SendBuffer {
        SendBuffer { buf: VecDeque::with_capacity(n) }
    }

    pub fn send<T: Write>(&mut self, w: &mut T) -> Result<usize> {
        let count = {
            let (left, right) = self.buf.as_slices();
            let mut count = match try!(w.try_write(left)) {
                None => return Ok(0),
                Some(n) => n,
            };

            if count == left.len() {
                if let Some(n) = try!(w.try_write(right)) {
                    count += n;
                }
            }

            count
        };

        self.buf.drain(0..count);
        Ok(count)
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Write for SendBuffer {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buf.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use super::*;

    #[test]
    fn test_send_buffer() {
        let mut s = SendBuffer::new(4);
        s.write(b"0123456789").unwrap();
        assert_eq!(s.len(), 10);

        let mut w = vec![];
        s.send(&mut w).unwrap();
        assert!(s.is_empty());
        assert_eq!(w.len(), 10);
        assert_eq!(w, b"0123456789");

        s.write(b"a").unwrap();
        s.write(b"b").unwrap();
        w.clear();
        assert_eq!(s.len(), 2);

        s.send(&mut w).unwrap();
        assert!(s.is_empty());
        assert_eq!(w, b"ab");
    }
}
