// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::intrinsics::{likely, unlikely};

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::buffer::{BufferReader, BufferWriter};
use crate::{Error, Result};

pub const MAX_VARINT64_LENGTH: usize = 10;
pub const U64_SIZE: usize = std::mem::size_of::<u64>();
pub const I64_SIZE: usize = std::mem::size_of::<i64>();
pub const F64_SIZE: usize = std::mem::size_of::<f64>();

/// Byte encoding and decoding utility for primitive number types.
pub struct NumberCodec;

impl NumberCodec {
    /// Encodes an unsigned 8 bit integer `v` to `buf`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 1`.
    #[inline]
    pub fn encode_u8(buf: &mut [u8], v: u8) {
        assert!(!buf.is_empty());
        buf[0] = v;
    }

    /// Decodes an unsigned 8 bit integer from `buf`,
    /// which is previously encoded via `encode_u8`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 1`.
    #[inline]
    pub fn decode_u8(buf: &[u8]) -> u8 {
        assert!(!buf.is_empty());
        buf[0]
    }

    /// Encodes an unsigned 16 bit integer `v` to `buf`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 2`.
    #[inline]
    pub fn encode_u16(buf: &mut [u8], v: u16) {
        BigEndian::write_u16(buf, v);
    }

    /// Decodes an unsigned 16 bit integer from `buf`,
    /// which is previously encoded via `encode_u16`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 2`.
    #[inline]
    pub fn decode_u16(buf: &[u8]) -> u16 {
        BigEndian::read_u16(buf)
    }

    /// Encodes an unsigned 32 bit integer `v` to `buf`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn encode_u32(buf: &mut [u8], v: u32) {
        BigEndian::write_u32(buf, v);
    }

    /// Decodes an unsigned 32 bit integer from `buf`,
    /// which is previously encoded via `encode_u32`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn decode_u32(buf: &[u8]) -> u32 {
        BigEndian::read_u32(buf)
    }

    /// Encodes an unsigned 64 bit integer `v` to `buf`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_u64(buf: &mut [u8], v: u64) {
        BigEndian::write_u64(buf, v);
    }

    /// Decodes an unsigned 64 bit integer from `buf`,
    /// which is previously encoded via `encode_u64`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_u64(buf: &[u8]) -> u64 {
        BigEndian::read_u64(buf)
    }

    /// Encodes an unsigned 64 bit integer `v` to `buf`,
    /// which is memory-comparable in descending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_u64_desc(buf: &mut [u8], v: u64) {
        Self::encode_u64(buf, !v);
    }

    /// Decodes an unsigned 64 bit integer from `buf`,
    /// which is previously encoded via `encode_u64_desc`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_u64_desc(buf: &[u8]) -> u64 {
        !Self::decode_u64(buf)
    }

    /// Encodes a signed 64 bit integer `v` to `buf`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_i64(buf: &mut [u8], v: i64) {
        let u = super::convert::encode_i64_to_comparable_u64(v);
        Self::encode_u64(buf, u);
    }

    /// Decodes a signed 64 bit integer from `buf`,
    /// which is previously encoded via `encode_i64`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_i64(buf: &[u8]) -> i64 {
        let u = Self::decode_u64(buf);
        super::convert::decode_comparable_u64_to_i64(u)
    }

    /// Encodes a signed 64 bit integer `v` to `buf`,
    /// which is memory-comparable in descending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_i64_desc(buf: &mut [u8], v: i64) {
        let u = super::convert::encode_i64_to_comparable_u64(v);
        Self::encode_u64_desc(buf, u);
    }

    /// Decodes a signed 64 bit integer from `buf`,
    /// which is previously encoded via `encode_i64_desc`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_i64_desc(buf: &[u8]) -> i64 {
        let u = Self::decode_u64_desc(buf);
        super::convert::decode_comparable_u64_to_i64(u)
    }

    /// Encodes a 64 bit float number `v` to `buf`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_f64(buf: &mut [u8], v: f64) {
        let u = super::convert::encode_f64_to_comparable_u64(v);
        Self::encode_u64(buf, u);
    }

    /// Decodes a 64 bit float number from `buf`,
    /// which is previously encoded via `encode_f64`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_f64(buf: &[u8]) -> f64 {
        let u = Self::decode_u64(buf);
        super::convert::decode_comparable_u64_to_f64(u)
    }

    /// Encodes a 64 bit float number `v` to `buf`,
    /// which is memory-comparable in descending order.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_f64_desc(buf: &mut [u8], v: f64) {
        let u = super::convert::encode_f64_to_comparable_u64(v);
        Self::encode_u64_desc(buf, u);
    }

    /// Decodes a 64 bit float number from `buf`,
    /// which is previously encoded via `encode_f64_desc`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_f64_desc(buf: &[u8]) -> f64 {
        let u = Self::decode_u64_desc(buf);
        super::convert::decode_comparable_u64_to_f64(u)
    }

    /// Encodes an unsigned 16 bit integer `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 2`.
    #[inline]
    pub fn encode_u16_le(buf: &mut [u8], v: u16) {
        LittleEndian::write_u16(buf, v)
    }

    /// Decodes an unsigned 16 bit integer from `buf` in little endian,
    /// which is previously encoded via `encode_u16_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 2`.
    #[inline]
    pub fn decode_u16_le(buf: &[u8]) -> u16 {
        LittleEndian::read_u16(buf)
    }

    /// Encodes a signed 16 bit integer `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 2`.
    #[inline]
    pub fn encode_i16_le(buf: &mut [u8], v: i16) {
        LittleEndian::write_i16(buf, v)
    }

    /// Decodes a signed 16 bit integer from `buf` in little endian,
    /// which is previously encoded via `encode_i16_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 2`.
    #[inline]
    pub fn decode_i16_le(buf: &[u8]) -> i16 {
        LittleEndian::read_i16(buf)
    }

    /// Encodes a 32 bit float number `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn encode_f32_le(buf: &mut [u8], v: f32) {
        LittleEndian::write_f32(buf, v)
    }

    /// Decodes a 32 bit float number `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn decode_f32_le(buf: &[u8]) -> f32 {
        LittleEndian::read_f32(buf)
    }

    /// Encodes an unsigned 32 bit integer `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn encode_u32_le(buf: &mut [u8], v: u32) {
        LittleEndian::write_u32(buf, v)
    }

    /// Decodes an unsigned 32 bit integer from `buf` in little endian,
    /// which is previously encoded via `encode_u32_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn decode_u32_le(buf: &[u8]) -> u32 {
        LittleEndian::read_u32(buf)
    }

    /// Encodes a signed 32 bit integer `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn encode_i32_le(buf: &mut [u8], v: i32) {
        LittleEndian::write_i32(buf, v)
    }

    /// Decodes a signed 32 bit integer from `buf` in little endian,
    /// which is previously encoded via `encode_i32_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 4`.
    #[inline]
    pub fn decode_i32_le(buf: &[u8]) -> i32 {
        LittleEndian::read_i32(buf)
    }

    /// Encodes an unsigned 64 bit integer `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_u64_le(buf: &mut [u8], v: u64) {
        LittleEndian::write_u64(buf, v)
    }

    /// Decodes an unsigned 64 bit integer from `buf` in little endian,
    /// which is previously encoded via `encode_u64_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_u64_le(buf: &[u8]) -> u64 {
        LittleEndian::read_u64(buf)
    }

    /// Encodes a signed 64 bit integer `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_i64_le(buf: &mut [u8], v: i64) {
        LittleEndian::write_i64(buf, v)
    }

    /// Decodes a signed 64 bit integer from `buf` in little endian,
    /// which is previously encoded via `encode_i64_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_i64_le(buf: &[u8]) -> i64 {
        LittleEndian::read_i64(buf)
    }

    /// Encodes a 64 bit float number `v` to `buf` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn encode_f64_le(buf: &mut [u8], v: f64) {
        LittleEndian::write_f64(buf, v)
    }

    /// Decodes a 64 bit float number from `buf` in little endian,
    /// which is previously encoded via `encode_f64_le`.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 8`.
    #[inline]
    pub fn decode_f64_le(buf: &[u8]) -> f64 {
        LittleEndian::read_f64(buf)
    }

    /// Encodes an unsigned 64 bit integer `v` to `buf` in VarInt encoding,
    /// which is not memory-comparable. Returns the number of bytes that encoded.
    ///
    /// Note: VarInt encoding is slow, try avoid using it.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 10`.
    pub fn encode_var_u64(buf: &mut [u8], mut v: u64) -> usize {
        assert!(buf.len() >= MAX_VARINT64_LENGTH);

        unsafe {
            let mut ptr = buf.as_mut_ptr();

            while v >= 0x80 {
                // We have already checked buffer size at the beginning so that it is safe to
                // directly access the buffer.
                *ptr = 0x80 | (v & 0x7f) as u8;
                ptr = ptr.add(1);
                v >>= 7;
            }
            *ptr = v as u8;
            ptr.offset_from(buf.as_ptr()) as usize + 1
        }
    }

    /// Decodes an unsigned 64 bit integer from `buf` in VarInt encoding.
    /// Returns decoded result and the number of bytes that successfully decoded.
    ///
    /// This function is more efficient when `buf.len() >= 10`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if there is not enough space to decode the whole VarInt.
    #[inline]
    pub fn try_decode_var_u64(buf: &[u8]) -> Result<(u64, usize)> {
        #[allow(clippy::cast_lossless)]
        unsafe {
            let mut ptr = buf.as_ptr();
            let len = buf.len();
            let mut val = 0u64;
            if likely(len >= MAX_VARINT64_LENGTH) {
                // Fast path
                let mut b: u64;
                let mut shift = 0;
                // Compiler will do loop unrolling for us.
                for i in 1..=9 {
                    b = *ptr as u64;
                    val |= (b & 0x7f) << shift;
                    if b < 0x80 {
                        return Ok((val, i));
                    }
                    ptr = ptr.add(1);
                    shift += 7;
                }
                b = *ptr as u64;
                val |= (b & 0x01) << shift;
                Ok((val, 10))
            } else {
                let ptr_end = buf.as_ptr().add(len);
                // Slow path
                let mut shift = 0;
                while ptr != ptr_end && *ptr >= 0x80 {
                    val |= ((*ptr & 0x7f) as u64) << shift;
                    shift += 7;
                    ptr = ptr.add(1);
                }
                if unlikely(ptr == ptr_end) {
                    return Err(Error::eof());
                }
                val |= (*ptr as u64) << shift;
                Ok((val, ptr.offset_from(buf.as_ptr()) as usize + 1))
            }
        }
    }

    /// Encodes a signed 64 bit integer `v` to `buf` in VarInt encoding,
    /// which is not memory-comparable. Returns the number of bytes that encoded.
    ///
    /// Note: VarInt encoding is slow, try avoid using it.
    ///
    /// # Panics
    ///
    /// Panics when `buf.len() < 10`.
    #[inline]
    pub fn encode_var_i64(buf: &mut [u8], v: i64) -> usize {
        let mut uv = (v as u64) << 1;
        if unsafe { unlikely(v < 0) } {
            uv = !uv;
        }
        Self::encode_var_u64(buf, uv)
    }

    /// Decodes a signed 64 bit integer from `buf` in VarInt encoding.
    /// Returns decoded result and the number of bytes that successfully decoded.
    ///
    /// This function is more efficient when `buf.len() >= 10`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if there is not enough space to decode the whole VarInt.
    #[inline]
    pub fn try_decode_var_i64(buf: &[u8]) -> Result<(i64, usize)> {
        let (uv, decoded_bytes) = Self::try_decode_var_u64(buf)?;
        let v = uv >> 1;
        if uv & 1 == 0 {
            // no need for likely/unlikely here
            Ok((v as i64, decoded_bytes))
        } else {
            Ok((!v as i64, decoded_bytes))
        }
    }

    /// Gets the length of the first encoded VarInt in the given buffer. If the buffer is not
    /// complete, the length of buffer will be returned.
    ///
    /// This function is more efficient when `buf.len() >= 10`.
    #[inline]
    pub fn get_first_encoded_var_int_len(buf: &[u8]) -> usize {
        unsafe {
            let mut ptr = buf.as_ptr();
            let len = buf.len();
            if likely(len >= MAX_VARINT64_LENGTH) {
                // Fast path
                // Compiler will do loop unrolling for us.
                for i in 1..=9 {
                    if *ptr < 0x80 {
                        return i;
                    }
                    ptr = ptr.add(1);
                }
                10
            } else {
                let ptr_end = buf.as_ptr().add(len);
                // Slow path
                while ptr != ptr_end && *ptr >= 0x80 {
                    ptr = ptr.add(1);
                }
                // When we got here, we are either `ptr == ptr_end`, or `*ptr < 0x80`.
                // For `ptr == ptr_end` case, it means we are expecting a value < 0x80
                //      but meet EOF, so only `len` is returned.
                // For `*ptr < 0x80` case, it means currently it is pointing to the last byte
                //      of the VarInt, so we return `delta + 1` as length.
                if unlikely(ptr == ptr_end) {
                    return len;
                }
                ptr.offset_from(buf.as_ptr()) as usize + 1
            }
        }
    }
}

macro_rules! read {
    ($s:expr, $size:expr, $f:ident) => {{
        let ret = {
            let buf = $s.bytes();
            if unsafe { unlikely(buf.len() < $size) } {
                return Err(Error::eof());
            }
            NumberCodec::$f(buf)
        };
        $s.advance($size);
        Ok(ret)
    }};
}

pub trait NumberDecoder: BufferReader {
    /// Reads an unsigned 8 bit integer,
    /// which is previously wrote via `write_u8`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 1.
    #[inline]
    fn read_u8(&mut self) -> Result<u8> {
        read!(self, 1, decode_u8)
    }

    /// Reads an unsigned 16 bit integer,
    /// which is previously wrote via `write_u16`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 2.
    #[inline]
    fn read_u16(&mut self) -> Result<u16> {
        read!(self, 2, decode_u16)
    }

    /// Reads an unsigned 32 bit integer,
    /// which is previously wrote via `write_u32`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn read_u32(&mut self) -> Result<u32> {
        read!(self, 4, decode_u32)
    }

    /// Reads an unsigned 64 bit integer,
    /// which is previously wrote via `write_u64`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_u64(&mut self) -> Result<u64> {
        read!(self, 8, decode_u64)
    }

    /// Reads an unsigned 64 bit integer,
    /// which is previously wrote via `write_u64_desc`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_u64_desc(&mut self) -> Result<u64> {
        read!(self, 8, decode_u64_desc)
    }

    /// Reads a signed 64 bit integer,
    /// which is previously wrote via `write_i64`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_i64(&mut self) -> Result<i64> {
        read!(self, 8, decode_i64)
    }

    /// Reads a signed 64 bit integer,
    /// which is previously wrote via `write_i64_desc`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_i64_desc(&mut self) -> Result<i64> {
        read!(self, 8, decode_i64_desc)
    }

    /// Reads a 64 bit float number,
    /// which is previously wrote via `write_f64`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_f64(&mut self) -> Result<f64> {
        read!(self, 8, decode_f64)
    }

    /// Reads a 64 bit float number,
    /// which is previously wrote via `write_f64_desc`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_f64_desc(&mut self) -> Result<f64> {
        read!(self, 8, decode_f64_desc)
    }

    /// Reads an unsigned 16 bit integer in little endian,
    /// which is previously wrote via `write_u16_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 2.
    #[inline]
    fn read_u16_le(&mut self) -> Result<u16> {
        read!(self, 2, decode_u16_le)
    }

    /// Reads a signed 16 bit integer in little endian,
    /// which is previously wrote via `write_i16_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 2.
    #[inline]
    fn read_i16_le(&mut self) -> Result<i16> {
        read!(self, 2, decode_i16_le)
    }

    /// Reads an unsigned 32 bit integer in little endian,
    /// which is previously wrote via `write_u32_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn read_u32_le(&mut self) -> Result<u32> {
        read!(self, 4, decode_u32_le)
    }

    /// Reads a signed 32 bit integer in little endian,
    /// which is previously wrote via `write_i32_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn read_i32_le(&mut self) -> Result<i32> {
        read!(self, 4, decode_i32_le)
    }

    /// Reads a 32 bit float  in little endian,
    /// which is previously wrote via `write_f32_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn read_f32_le(&mut self) -> Result<f32> {
        read!(self, 4, decode_f32_le)
    }

    /// Reads an unsigned 64 bit integer in little endian,
    /// which is previously wrote via `write_u64_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_u64_le(&mut self) -> Result<u64> {
        read!(self, 8, decode_u64_le)
    }

    /// Reads a signed 64 bit integer in little endian,
    /// which is previously wrote via `write_i64_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_i64_le(&mut self) -> Result<i64> {
        read!(self, 8, decode_i64_le)
    }

    /// Reads a 64 bit float number in little endian,
    /// which is previously wrote via `write_f64_le`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn read_f64_le(&mut self) -> Result<f64> {
        read!(self, 8, decode_f64_le)
    }

    /// Decodes an unsigned 64 bit integer in VarInt encoding,
    /// which is previously wrote via `write_var_u64`.
    ///
    /// This function is more efficient when remaining buffer size >= 10.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if there is not enough space to decode the whole VarInt.
    #[inline]
    fn read_var_u64(&mut self) -> Result<u64> {
        let (v, decoded_bytes) = {
            let buf = self.bytes();
            NumberCodec::try_decode_var_u64(buf)?
        };
        self.advance(decoded_bytes);
        Ok(v)
    }

    /// Decodes a signed 64 bit integer in VarInt encoding,
    /// which is previously wrote via `write_var_i64`.
    ///
    /// This function is more efficient when remaining buffer size >= 10.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if there is not enough space to decode the whole VarInt.
    #[inline]
    fn read_var_i64(&mut self) -> Result<i64> {
        let (v, decoded_bytes) = {
            let buf = self.bytes();
            NumberCodec::try_decode_var_i64(buf)?
        };
        self.advance(decoded_bytes);
        Ok(v)
    }
}

/// Any types who implemented `BufferReader` also implements `NumberDecoder`.
impl<T: BufferReader> NumberDecoder for T {}

macro_rules! write {
    ($s:expr, $v:expr, $size:expr, $f:ident) => {{
        {
            let buf = unsafe { $s.bytes_mut($size) };
            if unsafe { unlikely(buf.len() < $size) } {
                return Err(Error::eof());
            }
            NumberCodec::$f(buf, $v);
        }
        unsafe { $s.advance_mut($size) };
        Ok(())
    }};
}

pub trait NumberEncoder: BufferWriter {
    /// Writes an unsigned 8 bit integer `v`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 1.
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        write!(self, v, 1, encode_u8)
    }

    /// Writes an unsigned 16 bit integer `v`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 2.
    #[inline]
    fn write_u16(&mut self, v: u16) -> Result<()> {
        write!(self, v, 2, encode_u16)
    }

    /// Writes an unsigned 32 bit integer `v`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn write_u32(&mut self, v: u32) -> Result<()> {
        write!(self, v, 4, encode_u32)
    }

    /// Writes an unsigned 64 bit integer `v`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_u64(&mut self, v: u64) -> Result<()> {
        write!(self, v, 8, encode_u64)
    }

    /// Writes an unsigned 64 bit integer `v`,
    /// which is memory-comparable in descending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_u64_desc(&mut self, v: u64) -> Result<()> {
        write!(self, v, 8, encode_u64_desc)
    }

    /// Writes a signed 64 bit integer `v`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_i64(&mut self, v: i64) -> Result<()> {
        write!(self, v, 8, encode_i64)
    }

    /// Writes a signed 64 bit integer `v`,
    /// which is memory-comparable in descending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_i64_desc(&mut self, v: i64) -> Result<()> {
        write!(self, v, 8, encode_i64_desc)
    }

    /// Writes a 64 bit float number `v`,
    /// which is memory-comparable in ascending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_f64(&mut self, v: f64) -> Result<()> {
        write!(self, v, 8, encode_f64)
    }

    /// Writes a 64 bit float number `v`,
    /// which is memory-comparable in descending order.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_f64_desc(&mut self, v: f64) -> Result<()> {
        write!(self, v, 8, encode_f64_desc)
    }

    /// Writes an unsigned 16 bit integer `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 2.
    #[inline]
    fn write_u16_le(&mut self, v: u16) -> Result<()> {
        write!(self, v, 2, encode_u16_le)
    }

    /// Writes a signed 16 bit integer `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 2.
    #[inline]
    fn write_i16_le(&mut self, v: i16) -> Result<()> {
        write!(self, v, 2, encode_i16_le)
    }

    /// Writes an unsigned 32 bit integer `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn write_u32_le(&mut self, v: u32) -> Result<()> {
        write!(self, v, 4, encode_u32_le)
    }

    /// Writes a signed 32 bit integer `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn write_i32_le(&mut self, v: i32) -> Result<()> {
        write!(self, v, 4, encode_i32_le)
    }

    /// Writes a 32 bit float number `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 4.
    #[inline]
    fn write_f32_le(&mut self, v: f32) -> Result<()> {
        write!(self, v, 4, encode_f32_le)
    }

    /// Writes an unsigned 64 bit integer `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_u64_le(&mut self, v: u64) -> Result<()> {
        write!(self, v, 8, encode_u64_le)
    }

    /// Writes a signed 64 bit integer `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_i64_le(&mut self, v: i64) -> Result<()> {
        write!(self, v, 8, encode_i64_le)
    }

    /// Writes a 64 bit float number `v` in little endian,
    /// which is not memory-comparable.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 8.
    #[inline]
    fn write_f64_le(&mut self, v: f64) -> Result<()> {
        write!(self, v, 8, encode_f64_le)
    }

    /// Writes an unsigned 64 bit integer `v` in VarInt encoding,
    /// which is not memory-comparable. Returns the number of bytes that encoded.
    ///
    /// Note:
    /// - VarInt encoding is slow, try avoid using it.
    /// - The buffer must reserve 10 bytes for writing, although actual written bytes may be less.
    /// - The buffer will be advanced by actual written bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 10.
    #[inline]
    fn write_var_u64(&mut self, v: u64) -> Result<usize> {
        let encoded_bytes = {
            let buf = unsafe { self.bytes_mut(MAX_VARINT64_LENGTH) };
            if unsafe { unlikely(buf.len() < MAX_VARINT64_LENGTH) } {
                return Err(Error::eof());
            }
            NumberCodec::encode_var_u64(buf, v)
        };
        unsafe { self.advance_mut(encoded_bytes) };
        Ok(encoded_bytes)
    }

    /// Writes a signed 64 bit integer `v` in VarInt encoding,
    /// which is not memory-comparable. Returns the number of bytes that encoded.
    ///
    /// Note:
    /// - VarInt encoding is slow, try avoid using it.
    /// - The buffer must reserve 10 bytes for writing, although actual written bytes may be less.
    /// - The buffer will be advanced by actual written bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if buffer remaining size < 10.
    #[inline]
    fn write_var_i64(&mut self, v: i64) -> Result<usize> {
        let encoded_bytes = {
            let buf = unsafe { self.bytes_mut(MAX_VARINT64_LENGTH) };
            if unsafe { unlikely(buf.len() < MAX_VARINT64_LENGTH) } {
                return Err(Error::eof());
            }
            NumberCodec::encode_var_i64(buf, v)
        };
        unsafe { self.advance_mut(encoded_bytes) };
        Ok(encoded_bytes)
    }
}

/// Any types who implemented `BufferWriter` also implements `NumberEncoder`.
impl<T: BufferWriter> NumberEncoder for T {}

#[cfg(test)]
mod tests {
    use protobuf::CodedOutputStream;
    use rand;

    fn get_u8_samples() -> Vec<u8> {
        vec![
            (::std::i8::MIN as u8),
            (::std::i8::MIN as u8).wrapping_add(1),
            (::std::i8::MIN as u8).overflowing_sub(1).0,
            (::std::i8::MAX as u8),
            (::std::i8::MAX as u8).wrapping_add(1),
            (::std::i8::MAX as u8).overflowing_sub(1).0,
            (::std::u8::MIN as u8),
            (::std::u8::MIN as u8).wrapping_add(1),
            (::std::u8::MIN as u8).overflowing_sub(1).0,
            (::std::u8::MAX as u8),
            (::std::u8::MAX as u8).wrapping_add(1),
            (::std::u8::MAX as u8).overflowing_sub(1).0,
            2,
            10,
            20,
            63,
            64,
            65,
            129,
        ]
    }

    fn get_u16_samples() -> Vec<u16> {
        vec![
            (::std::i16::MIN as u16),
            (::std::i16::MIN as u16).wrapping_add(1),
            (::std::i16::MIN as u16).overflowing_sub(1).0,
            (::std::i16::MAX as u16),
            (::std::i16::MAX as u16).wrapping_add(1),
            (::std::i16::MAX as u16).overflowing_sub(1).0,
            (::std::u16::MIN as u16),
            (::std::u16::MIN as u16).wrapping_add(1),
            (::std::u16::MIN as u16).overflowing_sub(1).0,
            (::std::u16::MAX as u16),
            (::std::u16::MAX as u16).wrapping_add(1),
            (::std::u16::MAX as u16).overflowing_sub(1).0,
            0,
            1,
            2,
            10,
            20,
            63,
            64,
            65,
            127,
            128,
            129,
            255,
            256,
            257,
            300,
            1024,
            16383,
            16384,
            16385,
        ]
    }

    fn get_i16_samples() -> Vec<i16> {
        get_u16_samples().into_iter().map(|v| v as i16).collect()
    }

    #[allow(clippy::cast_lossless)]
    fn get_u32_samples() -> Vec<u32> {
        let mut samples = vec![
            (::std::i32::MIN as u32),
            (::std::i32::MIN as u32).wrapping_add(1),
            (::std::i32::MIN as u32).overflowing_sub(1).0,
            (::std::i32::MAX as u32),
            (::std::i32::MAX as u32).wrapping_add(1),
            (::std::i32::MAX as u32).overflowing_sub(1).0,
            (::std::u32::MIN as u32),
            (::std::u32::MIN as u32).wrapping_add(1),
            (::std::u32::MIN as u32).overflowing_sub(1).0,
            (::std::u32::MAX as u32),
            (::std::u32::MAX as u32).wrapping_add(1),
            (::std::u32::MAX as u32).overflowing_sub(1).0,
        ];
        samples.extend(get_u16_samples().into_iter().map(|v| v as u32));
        samples
    }

    fn get_i32_samples() -> Vec<i32> {
        get_u32_samples().into_iter().map(|v| v as i32).collect()
    }

    #[allow(clippy::cast_lossless)]
    fn get_u64_samples() -> Vec<u64> {
        let mut samples = vec![
            (::std::i64::MIN as u64),
            (::std::i64::MIN as u64).wrapping_add(1),
            (::std::i64::MIN as u64).overflowing_sub(1).0,
            (::std::i64::MAX as u64),
            (::std::i64::MAX as u64).wrapping_add(1),
            (::std::i64::MAX as u64).overflowing_sub(1).0,
            (::std::u64::MIN as u64),
            (::std::u64::MIN as u64).wrapping_add(1),
            (::std::u64::MIN as u64).overflowing_sub(1).0,
            (::std::u64::MAX as u64),
            (::std::u64::MAX as u64).wrapping_add(1),
            (::std::u64::MAX as u64).overflowing_sub(1).0,
        ];
        samples.extend(get_u32_samples().into_iter().map(|v| v as u64));
        samples
    }

    fn get_i64_samples() -> Vec<i64> {
        get_u64_samples().into_iter().map(|v| v as i64).collect()
    }

    #[allow(clippy::cast_lossless)]
    fn get_f64_samples() -> Vec<f64> {
        vec![
            -1.0,
            0.0,
            1.0,
            std::f64::MIN,
            std::f64::MIN_POSITIVE,
            std::f64::MAX,
            std::f64::INFINITY,
            std::f64::NEG_INFINITY,
            std::f64::EPSILON,
            std::f64::consts::PI,
            std::f64::consts::E,
            std::f32::MIN as f64,
            std::f32::MIN_POSITIVE as f64,
            std::f32::MAX as f64,
            std::f32::INFINITY as f64,
            std::f32::NEG_INFINITY as f64,
            std::f32::EPSILON as f64,
            std::f32::consts::PI as f64,
            std::f32::consts::E as f64,
            // NAN is intentionally excluded, because NAN != NAN.
        ]
    }

    fn generate_comparer<T>(asc: bool) -> impl for<'r, 's> FnMut(&'r T, &'s T) -> std::cmp::Ordering
    where
        T: PartialOrd,
    {
        move |a: &T, b: &T| {
            let ord = a.partial_cmp(b).unwrap();
            if !asc {
                ord.reverse()
            } else {
                ord
            }
        }
    }

    macro_rules! test_codec {
        ($samples:expr, $enc:ident, $dec:ident, $buf_enc:ident, $buf_dec:ident,) => {
            for sample in $samples {
                let len = std::mem::size_of_val(&sample);

                // Use `encode_fn` and `decode_fn`.
                let mut base_buf = vec![0; len];
                super::NumberCodec::$enc(base_buf.as_mut_slice(), sample);
                assert_eq!(super::NumberCodec::$dec(base_buf.as_slice()), sample);

                // Encode to a `Vec<u8>` without sufficient capacity
                let mut buf: Vec<u8> = vec![];
                super::NumberEncoder::$buf_enc(&mut buf, sample).unwrap();
                assert_eq!(buf.as_slice(), base_buf.as_slice());

                let mut buf: Vec<u8> = Vec::with_capacity(len - 1);
                super::NumberEncoder::$buf_enc(&mut buf, sample).unwrap();
                assert_eq!(buf.as_slice(), base_buf.as_slice());

                // Encode to a `Vec<u8>` with sufficient capacity
                let mut buf: Vec<u8> = Vec::with_capacity(len);
                super::NumberEncoder::$buf_enc(&mut buf, sample).unwrap();
                assert_eq!(buf.as_slice(), base_buf.as_slice());

                let mut buf: Vec<u8> = Vec::with_capacity(len + 10);
                super::NumberEncoder::$buf_enc(&mut buf, sample).unwrap();
                assert_eq!(buf.as_slice(), base_buf.as_slice());

                // Encode to a `Vec<u8>` that has previously written data
                for prefix_len in 1..20 {
                    let mut prefix: Vec<u8> = Vec::with_capacity(prefix_len);
                    for _ in 0..prefix_len {
                        prefix.push(rand::random());
                    }

                    let mut buf = prefix.clone();
                    super::NumberEncoder::$buf_enc(&mut buf, sample).unwrap();
                    assert_eq!(&buf.as_slice()[0..prefix.len()], prefix.as_slice());
                    assert_eq!(&buf.as_slice()[prefix.len()..], base_buf.as_slice());
                }

                // Encode to a `Cursor` (backed by Vec) without sufficient capacity
                let buf: Vec<u8> = vec![];
                let mut cursor = std::io::Cursor::new(buf);
                assert!(super::NumberEncoder::$buf_enc(&mut cursor, sample).is_err());
                assert_eq!(cursor.position(), 0);
                assert_eq!(cursor.get_ref().len(), 0);

                // Note that Vec capacity is not counted in Cursor.
                let buf: Vec<u8> = Vec::with_capacity(len);
                let mut cursor = std::io::Cursor::new(buf);
                assert!(super::NumberEncoder::$buf_enc(&mut cursor, sample).is_err());
                assert_eq!(cursor.position(), 0);
                assert_eq!(cursor.get_ref().len(), 0);

                let buf: Vec<u8> = vec![0; len - 1];
                let mut cursor = std::io::Cursor::new(buf);
                assert!(super::NumberEncoder::$buf_enc(&mut cursor, sample).is_err());
                assert_eq!(cursor.position(), 0);
                assert_eq!(cursor.get_ref().len(), len - 1);

                // Encode to a `Cursor` (backed by Vec) with sufficient capacity
                let buf: Vec<u8> = vec![0; len];
                let mut cursor = std::io::Cursor::new(buf);
                super::NumberEncoder::$buf_enc(&mut cursor, sample).unwrap();
                assert_eq!(cursor.get_ref().as_slice(), base_buf.as_slice());
                assert_eq!(cursor.position(), len as u64);

                let buf: Vec<u8> = vec![0; len + 10];
                let mut cursor = std::io::Cursor::new(buf);
                super::NumberEncoder::$buf_enc(&mut cursor, sample).unwrap();
                assert_eq!(&cursor.get_ref().as_slice()[0..len], base_buf.as_slice());
                assert_eq!(cursor.position(), len as u64);

                // Encode to a `Cursor` (backed by Vec) with existing data at beginning and end
                for buf_len in len..len + 20 {
                    let mut payload: Vec<u8> = Vec::with_capacity(buf_len);
                    for _ in 0..buf_len {
                        payload.push(rand::random::<u8>());
                    }

                    // the cursor leaves sufficient space for encoding
                    for pos in 0usize..=buf_len - len {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        super::NumberEncoder::$buf_enc(&mut cursor, sample).unwrap();
                        assert_eq!(
                            &cursor.get_ref().as_slice()[0..pos],
                            &payload.as_slice()[0..pos]
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos..pos + len],
                            base_buf.as_slice()
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos + len..],
                            &payload.as_slice()[pos + len..]
                        );
                        assert_eq!(cursor.position(), (pos + len) as u64);
                    }

                    // the cursor leaves insufficient space for encoding
                    for pos in buf_len - len + 1..buf_len {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert!(super::NumberEncoder::$buf_enc(&mut cursor, sample).is_err());
                        // underlying buffer and position should be unchanged
                        assert_eq!(&cursor.get_ref().as_slice(), &payload.as_slice());
                        assert_eq!(cursor.position(), pos as u64);
                    }
                }

                // Decode from a `Cursor` without sufficient capacity
                let buf: Vec<u8> = vec![];
                let mut cursor = std::io::Cursor::new(buf);
                assert!(super::NumberDecoder::$buf_dec(&mut cursor).is_err());
                assert_eq!(cursor.position(), 0);
                assert_eq!(cursor.get_ref().len(), 0);

                let buf: Vec<u8> = Vec::with_capacity(len);
                let mut cursor = std::io::Cursor::new(buf);
                assert!(super::NumberDecoder::$buf_dec(&mut cursor).is_err());
                assert_eq!(cursor.position(), 0);
                assert_eq!(cursor.get_ref().len(), 0);

                let buf: Vec<u8> = vec![0; len - 1];
                let mut cursor = std::io::Cursor::new(buf);
                assert!(super::NumberDecoder::$buf_dec(&mut cursor).is_err());
                assert_eq!(cursor.position(), 0);
                assert_eq!(cursor.get_ref().len(), len - 1);

                // Decode from a `Cursor` with sufficient capacity
                let mut buf: Vec<u8> = vec![0; len];
                super::NumberCodec::$enc(buf.as_mut_slice(), sample);
                let mut cursor = std::io::Cursor::new(buf);
                assert_eq!(super::NumberDecoder::$buf_dec(&mut cursor).unwrap(), sample);
                assert_eq!(cursor.position(), len as u64);

                let mut buf: Vec<u8> = vec![0; len + 10];
                super::NumberCodec::$enc(buf.as_mut_slice(), sample);
                let mut cursor = std::io::Cursor::new(buf);
                assert_eq!(super::NumberDecoder::$buf_dec(&mut cursor).unwrap(), sample);
                assert_eq!(cursor.position(), len as u64);

                // Decode from a `Cursor` with existing data at beginning and end
                for buf_len in len..len + 20 {
                    let mut payload: Vec<u8> = Vec::with_capacity(buf_len);
                    for _ in 0..buf_len {
                        payload.push(rand::random::<u8>());
                    }

                    // the cursor leaves sufficient space for decoding
                    for pos in 0usize..=buf_len - len {
                        let mut buf = payload.clone();
                        super::NumberCodec::$enc(&mut buf.as_mut_slice()[pos..], sample);
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert_eq!(super::NumberDecoder::$buf_dec(&mut cursor).unwrap(), sample);
                        assert_eq!(
                            &cursor.get_ref().as_slice()[0..pos],
                            &payload.as_slice()[0..pos]
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos..pos + len],
                            base_buf.as_slice()
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos + len..],
                            &payload.as_slice()[pos + len..]
                        );
                        assert_eq!(cursor.position(), (pos + len) as u64);
                    }

                    // the cursor leaves insufficient space for decoding
                    for pos in buf_len - len + 1..buf_len {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert!(super::NumberDecoder::$buf_dec(&mut cursor).is_err());
                        // underlying buffer and position should be unchanged
                        assert_eq!(&cursor.get_ref().as_slice(), &payload.as_slice());
                        assert_eq!(cursor.position(), pos as u64);
                    }
                }
            }
        };
    }

    /// Test mem compare after encode + decode
    macro_rules! test_mem_compare {
        ($samples:expr, $asc:expr, $enc:ident, $dec:ident, $buf_enc:ident, $buf_dec:ident,) => {
            // 1. given source in order, the encoded should be in order
            let mut source = $samples;
            source.sort_by(generate_comparer($asc));
            let encoded: Vec<_> = source
                .iter()
                .map(|v| {
                    let mut buf = vec![0; std::mem::size_of_val(v)];
                    super::NumberCodec::$enc(buf.as_mut_slice(), *v);
                    buf
                })
                .collect();
            let mut encoded_asc_sorted = encoded.clone();
            encoded_asc_sorted.sort_by(generate_comparer(true));
            assert_eq!(encoded, encoded_asc_sorted);

            // 2. given encoded in order, the decoded should be in order
            let decoded: Vec<_> = encoded_asc_sorted
                .iter()
                .map(|buf| super::NumberCodec::$dec(buf.as_slice()))
                .collect();
            let mut decoded_sorted = decoded.clone();
            decoded_sorted.sort_by(generate_comparer($asc));
            assert_eq!(decoded, decoded_sorted);

            // 3. it must pass encode + decode
            test_codec!($samples, $enc, $dec, $buf_enc, $buf_dec,);
        };
    }

    #[test]
    fn test_u8() {
        test_mem_compare!(
            get_u8_samples(),
            true,
            encode_u8,
            decode_u8,
            write_u8,
            read_u8,
        );
    }

    #[test]
    fn test_u16() {
        test_mem_compare!(
            get_u16_samples(),
            true,
            encode_u16,
            decode_u16,
            write_u16,
            read_u16,
        );
    }

    #[test]
    fn test_u32() {
        test_mem_compare!(
            get_u32_samples(),
            true,
            encode_u32,
            decode_u32,
            write_u32,
            read_u32,
        );
    }

    #[test]
    fn test_u64() {
        test_mem_compare!(
            get_u64_samples(),
            true,
            encode_u64,
            decode_u64,
            write_u64,
            read_u64,
        );
    }

    #[test]
    fn test_u64_desc() {
        test_mem_compare!(
            get_u64_samples(),
            false,
            encode_u64_desc,
            decode_u64_desc,
            write_u64_desc,
            read_u64_desc,
        );
    }

    #[test]
    fn test_i64() {
        test_mem_compare!(
            get_i64_samples(),
            true,
            encode_i64,
            decode_i64,
            write_i64,
            read_i64,
        );
    }

    #[test]
    fn test_i64_desc() {
        test_mem_compare!(
            get_i64_samples(),
            false,
            encode_i64_desc,
            decode_i64_desc,
            write_i64_desc,
            read_i64_desc,
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_f64() {
        test_mem_compare!(
            get_f64_samples(),
            true,
            encode_f64,
            decode_f64,
            write_f64,
            read_f64,
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_f64_desc() {
        test_mem_compare!(
            get_f64_samples(),
            false,
            encode_f64_desc,
            decode_f64_desc,
            write_f64_desc,
            read_f64_desc,
        );
    }

    #[test]
    fn test_u8_codec() {
        test_codec!(get_u8_samples(), encode_u8, decode_u8, write_u8, read_u8,);
    }

    #[test]
    fn test_u16_le() {
        test_codec!(
            get_u16_samples(),
            encode_u16_le,
            decode_u16_le,
            write_u16_le,
            read_u16_le,
        );
    }

    #[test]
    fn test_i16_le() {
        test_codec!(
            get_i16_samples(),
            encode_i16_le,
            decode_i16_le,
            write_i16_le,
            read_i16_le,
        );
    }

    #[test]
    fn test_u32_le() {
        test_codec!(
            get_u32_samples(),
            encode_u32_le,
            decode_u32_le,
            write_u32_le,
            read_u32_le,
        );
    }

    #[test]
    fn test_i32_le() {
        test_codec!(
            get_i32_samples(),
            encode_i32_le,
            decode_i32_le,
            write_i32_le,
            read_i32_le,
        );
    }

    #[test]
    fn test_u64_le() {
        test_codec!(
            get_u64_samples(),
            encode_u64_le,
            decode_u64_le,
            write_u64_le,
            read_u64_le,
        );
    }

    #[test]
    fn test_i64_le() {
        test_codec!(
            get_i64_samples(),
            encode_i64_le,
            decode_i64_le,
            write_i64_le,
            read_i64_le,
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_f64_le() {
        test_codec!(
            get_f64_samples(),
            encode_f64_le,
            decode_f64_le,
            write_f64_le,
            read_f64_le,
        );
    }

    macro_rules! test_varint {
        ($samples:expr, $enc:ident, $dec:ident, $buf_enc:ident, $buf_dec:ident,) => {
            for sample in $samples {
                // NumberCodec encode
                let mut base_buf = vec![0; super::MAX_VARINT64_LENGTH];
                let len = super::NumberCodec::$enc(base_buf.as_mut_slice(), sample);

                // NumberCodec fast path decode
                let result = super::NumberCodec::$dec(base_buf.as_slice()).unwrap();
                assert_eq!(result.0, sample);
                assert_eq!(result.1, len);

                // NumberCodec fast path decode length
                let result = super::NumberCodec::get_first_encoded_var_int_len(base_buf.as_slice());
                assert_eq!(result, len);

                // NumberCodec slow path decode
                let result = super::NumberCodec::$dec(&base_buf[0..len]).unwrap();
                assert_eq!(result.0, sample);
                assert_eq!(result.1, len);

                // NumberCodec slow path decode length
                let result = super::NumberCodec::get_first_encoded_var_int_len(&base_buf[0..len]);
                assert_eq!(result, len);

                // Incomplete buffer, we should got errors in decoding, but get `len` as length
                for l in 0..len {
                    let result = super::NumberCodec::$dec(&base_buf[0..l]);
                    assert!(result.is_err());

                    let result = super::NumberCodec::get_first_encoded_var_int_len(&base_buf[0..l]);
                    assert_eq!(result, l);
                }

                // Buffer encode with insufficient space
                for buf_len in 0..super::MAX_VARINT64_LENGTH {
                    let mut payload: Vec<u8> = Vec::with_capacity(buf_len);
                    for _ in 0..buf_len {
                        payload.push(rand::random::<u8>());
                    }

                    // Starting from any position in the buffer
                    for pos in 0usize..buf_len {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert!(super::NumberEncoder::$buf_enc(&mut cursor, sample).is_err());
                        // underlying buffer and position should be unchanged
                        assert_eq!(&cursor.get_ref().as_slice(), &payload.as_slice());
                        assert_eq!(cursor.position(), pos as u64);
                    }
                }

                // Buffer encode with sufficient space
                for buf_len in super::MAX_VARINT64_LENGTH..super::MAX_VARINT64_LENGTH + 20 {
                    let mut payload: Vec<u8> = Vec::with_capacity(buf_len);
                    for _ in 0..buf_len {
                        payload.push(rand::random::<u8>());
                    }

                    // the cursor leaves sufficient space for encoding
                    for pos in 0usize..=buf_len - super::MAX_VARINT64_LENGTH {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        let encoded_length =
                            super::NumberEncoder::$buf_enc(&mut cursor, sample).unwrap();
                        assert_eq!(
                            &cursor.get_ref().as_slice()[0..pos],
                            &payload.as_slice()[0..pos]
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos..pos + encoded_length],
                            &base_buf.as_slice()[0..encoded_length]
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos + encoded_length..],
                            &payload.as_slice()[pos + encoded_length..]
                        );
                        assert_eq!(cursor.position(), (pos + encoded_length) as u64);
                    }

                    // the cursor leaves insufficient space for encoding
                    for pos in buf_len - len + 1..buf_len {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert!(super::NumberEncoder::$buf_enc(&mut cursor, sample).is_err());
                        // underlying buffer and position should be unchanged
                        assert_eq!(&cursor.get_ref().as_slice(), &payload.as_slice());
                        assert_eq!(cursor.position(), pos as u64);
                    }
                }

                // Buffer decode with insufficient space
                for buf_len in 0..len {
                    let payload: Vec<u8> = base_buf[0..buf_len].to_vec();

                    // Starting from any position in the buffer
                    for pos in 0usize..buf_len {
                        let buf = payload.clone();
                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert!(super::NumberDecoder::$buf_dec(&mut cursor).is_err());
                        // underlying buffer and position should be unchanged
                        assert_eq!(&cursor.get_ref().as_slice(), &payload.as_slice());
                        assert_eq!(cursor.position(), pos as u64);
                    }
                }

                // Buffer decode with sufficient space
                for buf_len in len..len + 20 {
                    let mut payload: Vec<u8> = Vec::with_capacity(buf_len);
                    for _ in 0..buf_len {
                        payload.push(rand::random::<u8>());
                    }
                    for pos in 0usize..=buf_len - len {
                        let mut buf = payload.clone();
                        buf[pos..pos + len].clone_from_slice(&base_buf[0..len]);

                        let mut cursor = std::io::Cursor::new(buf);
                        cursor.set_position(pos as u64);
                        assert_eq!(super::NumberDecoder::$buf_dec(&mut cursor).unwrap(), sample);
                        assert_eq!(
                            &cursor.get_ref().as_slice()[0..pos],
                            &payload.as_slice()[0..pos]
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos..pos + len],
                            &base_buf.as_slice()[0..len]
                        );
                        assert_eq!(
                            &cursor.get_ref().as_slice()[pos + len..],
                            &payload.as_slice()[pos + len..]
                        );
                        assert_eq!(cursor.position(), (pos + len) as u64);
                    }
                }
            }
        };
    }

    #[test]
    fn test_var_u64() {
        test_varint!(
            get_u64_samples(),
            encode_var_u64,
            try_decode_var_u64,
            write_var_u64,
            read_var_u64,
        );
        // Check encoded output with ProtoBuf
        let samples = get_u64_samples();
        for sample in samples {
            let mut buf = vec![0; super::MAX_VARINT64_LENGTH];
            let len = super::NumberCodec::encode_var_u64(buf.as_mut_slice(), sample);

            let mut protobuf_output = vec![];
            {
                let mut writer = CodedOutputStream::new(&mut protobuf_output);
                writer.write_uint64_no_tag(sample).unwrap();
                writer.flush().unwrap();
            }
            assert_eq!(buf[..len], protobuf_output[..]);
        }
    }

    #[test]
    fn test_var_i64() {
        test_varint!(
            get_i64_samples(),
            encode_var_i64,
            try_decode_var_i64,
            write_var_i64,
            read_var_i64,
        );
    }
}

#[cfg(test)]
mod benches {
    use crate::Error;

    use byteorder;
    use protobuf::CodedOutputStream;

    /// Encode u64 little endian using `NumberCodec` and store position in extra variable.
    #[bench]
    fn bench_encode_u64_le_number_codec(b: &mut test::Bencher) {
        let mut buf: [u8; 10] = [0; 10];
        b.iter(|| {
            let mut pos = 0;
            super::NumberCodec::encode_u64_le(
                test::black_box(&mut buf[..]),
                test::black_box(0xDEADBEEF),
            );
            pos += 8;
            test::black_box(buf);
            test::black_box(pos);
        });
    }

    /// Encode u64 little endian using `byteorder::WriteBytesExt` over a `Cursor<&mut [u8]>`.
    #[bench]
    fn bench_encode_u64_le_byteorder(b: &mut test::Bencher) {
        use byteorder::WriteBytesExt;

        let mut buf: [u8; 10] = [0; 10];
        b.iter(|| {
            {
                let mut cursor = std::io::Cursor::new(test::black_box(&mut buf[..]));
                cursor
                    .write_u64::<byteorder::LittleEndian>(test::black_box(0xDEADBEEF))
                    .unwrap();
                test::black_box(cursor.position());
            }
            test::black_box(&buf);
        });
    }

    /// Encode u64 little endian using `NumberEncoder` over a `Cursor<&mut [u8]>`.
    #[bench]
    fn bench_encode_u64_le_buffer_encoder_slice(b: &mut test::Bencher) {
        use super::NumberEncoder;

        let mut buf: Vec<u8> = vec![0; 10];
        b.iter(|| {
            {
                let mut cursor = std::io::Cursor::new(test::black_box(buf.as_mut_slice()));
                cursor.write_u64_le(test::black_box(0xDEADBEEF)).unwrap();
                test::black_box(cursor.position());
            }
            test::black_box(&buf);
        });
    }

    /// Encode u64 little endian using `NumberEncoder` over a `Vec<u8>`.
    #[bench]
    fn bench_encode_u64_le_buffer_encoder_vec(b: &mut test::Bencher) {
        use super::NumberEncoder;

        let mut buf: Vec<u8> = Vec::with_capacity(10);
        b.iter(|| {
            buf.write_u64_le(test::black_box(0xDEADBEEF)).unwrap();
            test::black_box(&buf);
            unsafe { buf.set_len(0) }; // shrink the vector, to avoid growing during benchmark
        });
    }

    /// Decode u64 little endian using `NumberCodec` and store position in extra variable.
    #[bench]
    fn bench_decode_u64_le_number_codec(b: &mut test::Bencher) {
        let buf: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        b.iter(|| {
            let mut pos = 0;
            let v = super::NumberCodec::decode_u64_le(test::black_box(&buf[..]));
            pos += 8;
            test::black_box(v);
            test::black_box(pos);
        });
    }

    /// Decode u64 little endian using `NumberCodec` and store position via slice index.
    #[bench]
    fn bench_decode_u64_le_number_codec_over_slice(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        b.iter(|| {
            let mut slice = buf.as_slice();
            let v = super::NumberCodec::decode_u64_le(test::black_box(slice));
            slice = &slice[8..];
            test::black_box(v);
            test::black_box(&slice);
        });
    }

    /// Decode u64 little endian using `byteorder::ReadBytesExt` over a `Cursor<&[u8]>`.
    #[bench]
    fn bench_decode_u64_le_byteorder(b: &mut test::Bencher) {
        use byteorder::ReadBytesExt;

        let buf: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        b.iter(|| {
            let mut cursor = std::io::Cursor::new(test::black_box(&buf[..]));
            let v = cursor.read_u64::<byteorder::LittleEndian>().unwrap();
            test::black_box(v);
            test::black_box(cursor.position());
        });
    }

    #[inline]
    fn read_num_bytes<T, F>(size: usize, data: &mut &[u8], f: F) -> super::Result<T>
    where
        F: Fn(&[u8]) -> T,
    {
        if data.len() >= size {
            let buf = &data[..size];
            *data = &data[size..];
            return Ok(f(buf));
        }
        Err(Error::eof())
    }

    /// The original implementation in TiKV
    fn original_decode_u64_le(data: &mut &[u8]) -> super::Result<u64> {
        use byteorder::ByteOrder;
        read_num_bytes(
            std::mem::size_of::<u64>(),
            data,
            byteorder::LittleEndian::read_u64,
        )
    }

    /// Decode u64 little endian using `bytes::Buf` over a `Cursor<&[u8]>`.
    #[bench]
    fn bench_decode_u64_le_bytes_buf(b: &mut test::Bencher) {
        use bytes::Buf;

        let buf: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        b.iter(|| {
            let mut cursor = std::io::Cursor::new(test::black_box(buf.as_slice()));
            let v = cursor.get_u64_le();
            test::black_box(v);
            test::black_box(cursor.position());
        });
    }

    /// Decode u64 little endian using `NumberDecoder` over a `Cursor<&[u8]>`.
    #[bench]
    fn bench_decode_u64_le_buffer_decoder(b: &mut test::Bencher) {
        use super::NumberDecoder;

        let buf: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        b.iter(|| {
            let mut cursor = std::io::Cursor::new(test::black_box(buf.as_slice()));
            let v = cursor.read_u64_le().unwrap();
            test::black_box(v);
            test::black_box(cursor.position());
        });
    }

    /// Decode u64 little endian using the original implementation in TiKV.
    #[bench]
    fn bench_decode_u64_le_original(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        b.iter(|| {
            let mut slice = test::black_box(buf.as_slice());
            let v = original_decode_u64_le(&mut slice).unwrap();
            test::black_box(v);
            test::black_box(&slice);
        });
    }

    const VARINT_SAMPLE: u64 = 0xDEADBEEFBAAD;
    const VARINT_ENCODED: [u8; 7] = [173, 245, 190, 247, 219, 213, 55];

    /// Encode u64 in VarInt using `NumberCodec`.
    #[bench]
    fn bench_encode_varint_number_codec(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = vec![0; super::MAX_VARINT64_LENGTH];
        b.iter(|| {
            let mut pos = 0;
            let bytes = super::NumberCodec::encode_var_u64(
                test::black_box(&mut buf[..]),
                test::black_box(VARINT_SAMPLE),
            );
            pos += bytes;
            test::black_box(&buf);
            test::black_box(pos);
        });
    }

    trait OldVarIntEncoder: byteorder::WriteBytesExt {
        fn encode_var_u64(&mut self, mut v: u64) -> super::Result<()> {
            while v >= 0x80 {
                self.write_u8(v as u8 | 0x80)?;
                v >>= 7;
            }
            Ok(self.write_u8(v as u8)?)
        }
    }

    impl<T: byteorder::WriteBytesExt> OldVarIntEncoder for T {}

    /// Encode u64 in VarInt using original TiKV implementation.
    #[bench]
    fn bench_encode_varint_original(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = Vec::with_capacity(super::MAX_VARINT64_LENGTH);
        b.iter(|| {
            OldVarIntEncoder::encode_var_u64(
                test::black_box(&mut buf),
                test::black_box(VARINT_SAMPLE),
            )
            .unwrap();
            test::black_box(&buf);
            unsafe { buf.set_len(0) };
        });
    }

    fn naive_encode_varint(buf: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            buf.push(v as u8 | 0x80);
            v >>= 7;
        }
        buf.push(v as u8);
    }

    /// Encode u64 in VarInt using a naive vector push implementation.
    #[bench]
    fn bench_encode_varint_naive(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = Vec::with_capacity(super::MAX_VARINT64_LENGTH);
        b.iter(|| {
            naive_encode_varint(test::black_box(&mut buf), test::black_box(VARINT_SAMPLE));
            test::black_box(&buf);
            unsafe { buf.set_len(0) };
        });
    }

    /// Encode u64 in VarInt using ProtoBuf implementation.
    #[bench]
    fn bench_encode_varint_protobuf(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = Vec::with_capacity(super::MAX_VARINT64_LENGTH);
        b.iter(|| {
            {
                let mut writer = CodedOutputStream::new(test::black_box(&mut buf));
                writer
                    .write_uint64_no_tag(test::black_box(VARINT_SAMPLE))
                    .unwrap();
                writer.flush().unwrap();
            }
            test::black_box(&buf);
            unsafe { buf.set_len(0) };
        });
    }

    /// Decode u64 < 128 in VarInt using `NumberCodec`.
    /// The buffer size >= 10.
    #[bench]
    fn bench_decode_varint_small_number_codec_large_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![60, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        b.iter(|| {
            let (v, bytes) = super::NumberCodec::try_decode_var_u64(test::black_box(&buf)).unwrap();
            test::black_box(v);
            test::black_box(bytes);
        });
    }

    #[bench]
    fn bench_varint_len_small_number_large_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![60, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        b.iter(|| {
            let bytes = super::NumberCodec::get_first_encoded_var_int_len(test::black_box(&buf));
            test::black_box(bytes);
        });
    }

    /// Decode u64 < 128 in VarInt using `NumberCodec`.
    /// The buffer size < 10.
    #[bench]
    fn bench_decode_varint_small_number_codec_small_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![60, 0, 0];
        b.iter(|| {
            let (v, bytes) = super::NumberCodec::try_decode_var_u64(test::black_box(&buf)).unwrap();
            test::black_box(v);
            test::black_box(bytes);
        });
    }

    #[bench]
    fn bench_varint_len_small_number_small_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![60, 0, 0];
        b.iter(|| {
            let bytes = super::NumberCodec::get_first_encoded_var_int_len(test::black_box(&buf));
            test::black_box(bytes);
        });
    }

    pub fn decode_var_u64_original(data: &mut &[u8]) -> super::Result<u64> {
        if !data.is_empty() {
            // process with value < 127 independently at first
            // since it matches most of the cases.
            if data[0] < 0x80 {
                let res = u64::from(data[0]) & 0x7f;
                *data = unsafe { data.get_unchecked(1..) };
                return Ok(res);
            }

            // process with data's len >=10 or data ends with var u64
            if data.len() >= 10 || *data.last().unwrap() < 0x80 {
                let mut res = 0;
                for i in 0..9 {
                    let b = unsafe { *data.get_unchecked(i) };
                    res |= (u64::from(b) & 0x7f) << (i * 7);
                    if b < 0x80 {
                        *data = unsafe { data.get_unchecked(i + 1..) };
                        return Ok(res);
                    }
                }
                let b = unsafe { *data.get_unchecked(9) };
                if b <= 1 {
                    res |= ((u64::from(b)) & 0x7f) << (9 * 7);
                    *data = unsafe { data.get_unchecked(10..) };
                    return Ok(res);
                }
                return Err(Error::eof());
            }
        }

        // process data's len < 10 && data not end with var u64.
        let mut res = 0;
        for i in 0..data.len() {
            let b = data[i];
            res |= (u64::from(b) & 0x7f) << (i * 7);
            if b < 0x80 {
                *data = unsafe { data.get_unchecked(i + 1..) };
                return Ok(res);
            }
        }
        Err(Error::eof())
    }

    /// Decode u64 < 128 in VarInt using original TiKV implementation.
    #[bench]
    fn bench_decode_varint_small_original_small_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = vec![60, 0, 0];
        b.iter(|| {
            let mut slice = test::black_box(buf.as_slice());
            let v = decode_var_u64_original(&mut slice).unwrap();
            test::black_box(v);
            test::black_box(&slice);
        });
    }

    /// Decode normal u64 in VarInt using `NumberCodec`.
    /// The buffer size >= 10.
    #[bench]
    fn bench_decode_varint_normal_number_codec_large_buffer(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = vec![0; 10];
        buf[0..VARINT_ENCODED.len()].clone_from_slice(&VARINT_ENCODED);
        b.iter(|| {
            let (v, bytes) = super::NumberCodec::try_decode_var_u64(test::black_box(&buf)).unwrap();
            test::black_box(v);
            test::black_box(bytes);
        });
    }

    #[bench]
    fn bench_varint_len_normal_number_large_buffer(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = vec![0; 10];
        buf[0..VARINT_ENCODED.len()].clone_from_slice(&VARINT_ENCODED);
        b.iter(|| {
            let bytes = super::NumberCodec::get_first_encoded_var_int_len(test::black_box(&buf));
            test::black_box(bytes);
        });
    }

    /// Decode normal u64 in VarInt using `NumberCodec`.
    /// The buffer size < 10.
    #[bench]
    fn bench_decode_varint_normal_number_codec_small_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = VARINT_ENCODED.to_vec();
        b.iter(|| {
            let (v, bytes) = super::NumberCodec::try_decode_var_u64(test::black_box(&buf)).unwrap();
            test::black_box(v);
            test::black_box(bytes);
        });
    }

    #[bench]
    fn bench_varint_len_normal_number_small_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = VARINT_ENCODED.to_vec();
        b.iter(|| {
            let bytes = super::NumberCodec::get_first_encoded_var_int_len(test::black_box(&buf));
            test::black_box(bytes);
        });
    }

    /// Decode normal u64 in VarInt using `NumberCodec`.
    /// The buffer size < 10 and has extra data.
    #[bench]
    fn bench_decode_varint_normal_number_codec_small_buffer_with_extra(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = VARINT_ENCODED.to_vec();
        buf.push(0xF0);
        b.iter(|| {
            let (v, bytes) = super::NumberCodec::try_decode_var_u64(test::black_box(&buf)).unwrap();
            test::black_box(v);
            test::black_box(bytes);
        });
    }

    /// Decode normal u64 in VarInt using original TiKV implementation.
    /// The buffer size >= 10.
    #[bench]
    fn bench_decode_varint_normal_original_large_buffer(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = vec![0; 10];
        buf[0..VARINT_ENCODED.len()].clone_from_slice(&VARINT_ENCODED);
        b.iter(|| {
            let mut slice = test::black_box(buf.as_slice());
            let v = decode_var_u64_original(&mut slice).unwrap();
            test::black_box(v);
            test::black_box(&slice);
        });
    }

    /// Decode normal u64 in VarInt using original TiKV implementation.
    /// The buffer size < 10.
    #[bench]
    fn bench_decode_varint_normal_original_small_buffer(b: &mut test::Bencher) {
        let buf: Vec<u8> = VARINT_ENCODED.to_vec();
        b.iter(|| {
            let mut slice = test::black_box(buf.as_slice());
            let v = decode_var_u64_original(&mut slice).unwrap();
            test::black_box(v);
            test::black_box(&slice);
        });
    }

    /// Decode normal u64 in VarInt using original TiKV implementation.
    /// The buffer size < 10 and has extra data.
    #[bench]
    fn bench_decode_varint_normal_original_small_buffer_with_extra(b: &mut test::Bencher) {
        let mut buf: Vec<u8> = VARINT_ENCODED.to_vec();
        buf.push(0xF0);
        b.iter(|| {
            let mut slice = test::black_box(buf.as_slice());
            let v = decode_var_u64_original(&mut slice).unwrap();
            test::black_box(v);
            test::black_box(&slice);
        });
    }
}
