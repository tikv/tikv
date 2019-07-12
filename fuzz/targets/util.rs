// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{self, Result};

use byteorder::{NativeEndian, ReadBytesExt};

pub trait ReadLiteralExt: io::Read {
    #[inline]
    fn read_as_u8(&mut self) -> Result<u8> {
        ReadBytesExt::read_u8(self)
    }

    #[inline]
    fn read_as_i8(&mut self) -> Result<i8> {
        ReadBytesExt::read_i8(self)
    }

    #[inline]
    fn read_as_u16(&mut self) -> Result<u16> {
        ReadBytesExt::read_u16::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_i16(&mut self) -> Result<i16> {
        ReadBytesExt::read_i16::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_u32(&mut self) -> Result<u32> {
        ReadBytesExt::read_u32::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_i32(&mut self) -> Result<i32> {
        ReadBytesExt::read_i32::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_u64(&mut self) -> Result<u64> {
        ReadBytesExt::read_u64::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_i64(&mut self) -> Result<i64> {
        ReadBytesExt::read_i64::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_f64(&mut self) -> Result<f64> {
        ReadBytesExt::read_f64::<NativeEndian>(self)
    }

    #[inline]
    fn read_as_bool(&mut self) -> Result<bool> {
        let v = self.read_as_u8()?;
        Ok(v % 2 == 0)
    }
}

impl<T: io::Read> ReadLiteralExt for T {}
