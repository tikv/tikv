// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ptr;

#[derive(Clone)]
pub struct KeyBuilder {
    buf: Vec<u8>,
    start: usize,
}

impl KeyBuilder {
    pub fn new(max_size: usize, reserved_prefix_len: usize) -> Self {
        assert!(reserved_prefix_len < max_size);
        let mut buf = Vec::with_capacity(max_size);
        if reserved_prefix_len > 0 {
            unsafe { buf.set_len(reserved_prefix_len) }
        }
        Self {
            buf,
            start: reserved_prefix_len,
        }
    }

    pub fn from_vec(
        mut vec: Vec<u8>,
        reserved_prefix_len: usize,
        reserved_suffix_len: usize,
    ) -> Self {
        if vec.capacity() >= vec.len() + reserved_prefix_len + reserved_suffix_len {
            if reserved_prefix_len > 0 {
                unsafe {
                    ptr::copy(
                        vec.as_ptr(),
                        vec.as_mut_ptr().add(reserved_prefix_len),
                        vec.len(),
                    );
                    vec.set_len(vec.len() + reserved_prefix_len);
                }
            }
            Self {
                buf: vec,
                start: reserved_prefix_len,
            }
        } else {
            Self::from_slice(vec.as_slice(), reserved_prefix_len, reserved_suffix_len)
        }
    }

    pub fn from_slice(s: &[u8], reserved_prefix_len: usize, reserved_suffix_len: usize) -> Self {
        let mut buf = Vec::with_capacity(s.len() + reserved_prefix_len + reserved_suffix_len);
        if reserved_prefix_len > 0 {
            unsafe {
                buf.set_len(reserved_prefix_len);
            }
        }
        buf.extend_from_slice(s);
        Self {
            buf,
            start: reserved_prefix_len,
        }
    }

    pub fn set_prefix(&mut self, prefix: &[u8]) {
        assert!(self.start == prefix.len());
        unsafe {
            ptr::copy_nonoverlapping(prefix.as_ptr(), self.buf.as_mut_ptr(), prefix.len());
            self.start = 0;
        }
    }

    pub fn append(&mut self, content: &[u8]) {
        self.buf.extend_from_slice(content);
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.start) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.buf.len() - self.start
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf.as_slice()[self.start..]
    }

    pub fn build(mut self) -> Vec<u8> {
        if self.start == 0 {
            self.buf
        } else {
            unsafe {
                let len = self.len();
                ptr::copy(
                    self.buf.as_ptr().add(self.start),
                    self.buf.as_mut_ptr(),
                    len,
                );
                self.buf.set_len(len);
            }
            self.buf
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_builder() {
        let prefix = b"prefix-";
        let suffix = b"-suffix";

        // new
        let key1 = b"key1";
        let mut key_builder =
            KeyBuilder::new(key1.len() + prefix.len() + suffix.len(), prefix.len());
        key_builder.append(key1);
        key_builder.append(suffix);
        key_builder.set_prefix(prefix);
        assert_eq!(key_builder.len(), key1.len() + prefix.len() + suffix.len());
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key1-suffix".to_vec());

        // from_vec
        let key2 = b"key2";
        let mut key_builder = KeyBuilder::from_vec(key2.to_vec(), prefix.len(), suffix.len());
        assert_eq!(key_builder.len(), key2.len());
        key_builder.set_prefix(prefix);
        assert_eq!(key_builder.len(), key2.len() + prefix.len());
        key_builder.append(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key2-suffix".to_vec());

        // from_vec but not set prefix
        let mut key_builder = KeyBuilder::from_vec(key2.to_vec(), prefix.len(), suffix.len());
        key_builder.append(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"key2-suffix".to_vec());

        // from_vec vec has enough memory.
        let mut vec = Vec::with_capacity(key2.len() + prefix.len() + suffix.len());
        vec.extend_from_slice(key2);
        let mut key_builder = KeyBuilder::from_vec(vec, prefix.len(), suffix.len());
        key_builder.set_prefix(prefix);
        key_builder.append(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key2-suffix".to_vec());

        // from_slice
        let key3 = b"key3";
        let mut key_builder = KeyBuilder::from_slice(key3, prefix.len(), suffix.len());
        assert_eq!(key_builder.len(), key3.len());
        key_builder.set_prefix(prefix);
        assert_eq!(key_builder.len(), key3.len() + prefix.len());
        key_builder.append(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key3-suffix".to_vec());
    }
}
