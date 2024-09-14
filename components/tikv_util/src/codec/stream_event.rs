// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::io::{prelude::*, Cursor};

use byteorder::ByteOrder;
use bytes::{Buf, Bytes};

use crate::{codec::Result, Either};

// Note: maybe allow them to be different lifetime.
// But not necessary for now, so keep it simple...?
pub struct Rewrite<'a> {
    from: &'a [u8],
    to: &'a [u8],
}

pub trait Iterator {
    fn next(&mut self) -> Result<()>;

    fn valid(&self) -> bool;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
}

pub struct EventIterator<'a> {
    buf: &'a [u8],
    offset: usize,
    value_offset: usize,
    value_len: usize,

    key_buf: Vec<u8>,

    rewrite_rule: Option<Rewrite<'a>>,
}

impl EventIterator<'_> {
    pub fn new(buf: &[u8]) -> EventIterator<'_> {
        EventIterator {
            buf,
            offset: 0,
            key_buf: vec![],
            value_offset: 0,
            value_len: 0,
            rewrite_rule: None,
        }
    }

    pub fn with_rewriting<'a>(buf: &'a [u8], from: &'a [u8], to: &'a [u8]) -> EventIterator<'a> {
        EventIterator {
            buf,
            offset: 0,
            key_buf: vec![],
            value_offset: 0,
            value_len: 0,
            rewrite_rule: Some(Rewrite { from, to }),
        }
    }

    pub fn get_next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        let it = self;
        if !it.valid() {
            return Ok(None);
        }
        it.next()?;

        Ok(Some((it.key(), it.value())))
    }

    fn get_size(&mut self) -> u32 {
        let result = byteorder::LE::read_u32(&self.buf[self.offset..]);
        self.offset += 4;
        result
    }

    fn consume_key_with_len(&mut self, key_len: usize) {
        self.key_buf.clear();
        self.key_buf.reserve(key_len);
        self.key_buf
            .extend_from_slice(&self.buf[self.offset..self.offset + key_len]);
        self.offset += key_len;
    }

    fn move_to_next_key_with_rewrite(&mut self) {
        let key_len = self.get_size() as usize;
        let rewrite = self.rewrite_rule.as_ref().expect("rewrite rule not set");
        if key_len < rewrite.from.len()
            || &self.buf[self.offset..self.offset + rewrite.from.len()] != rewrite.from
        {
            self.consume_key_with_len(key_len);
            return;
        }
        self.key_buf.clear();
        self.key_buf
            .reserve(rewrite.to.len() + key_len - rewrite.from.len());
        self.key_buf.extend_from_slice(rewrite.to);
        self.key_buf
            .extend_from_slice(&self.buf[self.offset + rewrite.from.len()..self.offset + key_len]);
        self.offset += key_len;
    }

    fn fetch_key_buffer_and_move_to_value(&mut self) {
        if self.rewrite_rule.is_some() {
            self.move_to_next_key_with_rewrite()
        } else {
            let key_len = self.get_size() as usize;
            self.consume_key_with_len(key_len);
        }
    }
}

impl Iterator for EventIterator<'_> {
    fn next(&mut self) -> Result<()> {
        if self.valid() {
            self.fetch_key_buffer_and_move_to_value();

            self.value_len = self.get_size() as usize;
            self.value_offset = self.offset;
            self.offset += self.value_len;
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.offset < self.buf.len()
    }

    fn key(&self) -> &[u8] {
        &self.key_buf[..]
    }

    fn value(&self) -> &[u8] {
        &self.buf[self.value_offset..self.value_offset + self.value_len]
    }
}

#[derive(Clone)]
pub struct EventEncoder;

impl EventEncoder {
    pub fn encode_event<'e>(key: &'e [u8], value: &'e [u8]) -> [impl AsRef<[u8]> + 'e; 4] {
        let key_len = (key.len() as u32).to_le_bytes();
        let val_len = (value.len() as u32).to_le_bytes();
        [
            Either::Left(key_len),
            Either::Right(key),
            Either::Left(val_len),
            Either::Right(value),
        ]
    }

    #[allow(dead_code)]
    fn decode_event(e: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut buf = Cursor::new(Bytes::from(e.to_vec()));
        let len = buf.get_u32_le() as usize;
        let mut key = vec![0; len];
        buf.read_exact(key.as_mut_slice()).unwrap();
        let len = buf.get_u32_le() as usize;
        let mut val = vec![0; len];
        buf.read_exact(val.as_mut_slice()).unwrap();
        (key, val)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_encode_decode() {
        let mut rng = rand::thread_rng();
        for _i in 0..10 {
            let key: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let val: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let e = EventEncoder::encode_event(&key, &val);
            let mut event = vec![];
            for s in e {
                event.extend_from_slice(s.as_ref());
            }
            let (decoded_key, decoded_val) = EventEncoder::decode_event(&event);
            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }
    }

    #[test]
    fn test_decode_events() {
        let mut rng = rand::thread_rng();
        let mut event = vec![];
        let mut keys = vec![];
        let mut vals = vec![];
        let count = 20;

        for _i in 0..count {
            let key: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let val: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let e = EventEncoder::encode_event(&key, &val);
            for s in e {
                event.extend_from_slice(s.as_ref());
            }
            keys.push(key);
            vals.push(val);
        }

        let mut iter = EventIterator::new(&event);

        let mut index = 0_usize;
        loop {
            if !iter.valid() {
                break;
            }
            iter.next().unwrap();
            assert_eq!(iter.key(), keys[index]);
            assert_eq!(iter.value(), vals[index]);
            index += 1;
        }
        assert_eq!(count, index);
    }

    #[test]
    fn test_rewrite() {
        let mut rng = rand::thread_rng();
        let mut event = vec![];
        let mut keys = vec![];
        let mut vals = vec![];
        let count = 20;

        for _i in 0..count {
            let should_rewrite = rng.gen::<bool>();
            let mut key: Vec<u8> = std::iter::once(if should_rewrite { b'k' } else { b'l' })
                .chain((0..100).map(|_| rng.gen_range(0..255)))
                .collect();
            let val: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let e = EventEncoder::encode_event(&key, &val);
            for s in e {
                event.extend_from_slice(s.as_ref());
            }
            if should_rewrite {
                key[0] = b'r';
            }
            keys.push(key);
            vals.push(val);
        }

        let mut iter = EventIterator::with_rewriting(&event, b"k", b"r");

        let mut index = 0_usize;
        loop {
            if !iter.valid() {
                break;
            }
            iter.next().unwrap();
            assert_eq!(iter.key(), keys[index]);
            assert_eq!(iter.value(), vals[index]);
            index += 1;
        }
        assert_eq!(count, index);
    }
}
