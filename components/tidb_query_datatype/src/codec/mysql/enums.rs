// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use tipb::FieldType;

use crate::codec::convert::{ConvertTo, ToInt};
use crate::codec::Result;
use crate::expr::EvalContext;
use crate::FieldTypeTp;
use codec::prelude::*;

#[derive(Clone, Debug)]
pub struct Enum {
    name: Vec<u8>,

    // MySQL Enum is 1-based index, value == 0 means this enum is ''
    value: u64,
}

impl Enum {
    pub fn new(name: Vec<u8>, value: u64) -> Self {
        if value == 0 {
            Self {
                name: vec![],
                value,
            }
        } else {
            Self { name, value }
        }
    }
    pub fn value(&self) -> u64 {
        self.value
    }
    pub fn value_ref(&self) -> &u64 {
        &self.value
    }
    pub fn name(&self) -> &[u8] {
        self.name.as_slice()
    }
    pub fn as_ref(&self) -> EnumRef<'_> {
        EnumRef {
            name: &self.name,
            value: self.value,
        }
    }
    fn get_value_name(value: u64, elems: &[String]) -> &[u8] {
        let name = if value == 0 {
            ""
        } else {
            &elems[value as usize - 1]
        };
        name.as_bytes()
    }
}

impl Display for Enum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Eq for Enum {}

impl PartialEq for Enum {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for Enum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for Enum {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl crate::codec::data_type::AsMySQLBool for Enum {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::codec::Result<bool> {
        Ok(self.value != 0)
    }
}

impl ToInt for Enum {
    fn to_int(&self, _ctx: &mut EvalContext, _tp: FieldTypeTp) -> Result<i64> {
        Ok(self.value as i64)
    }

    fn to_uint(&self, _ctx: &mut EvalContext, _tp: FieldTypeTp) -> Result<u64> {
        Ok(self.value)
    }
}

impl ConvertTo<f64> for Enum {
    fn convert(&self, _ctx: &mut EvalContext) -> Result<f64> {
        Ok(self.value as f64)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EnumRef<'a> {
    name: &'a [u8],
    value: u64,
}

impl<'a> EnumRef<'a> {
    pub fn new(name: &'a [u8], value: u64) -> Self {
        if value == 0 {
            Self {
                name: "".as_bytes(),
                value,
            }
        } else {
            Self { name, value }
        }
    }
    pub fn to_owned(self) -> Enum {
        Enum {
            name: self.name.to_owned(),
            value: self.value,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.value == 0
    }
    pub fn value(&self) -> u64 {
        self.value
    }
    pub fn value_ref(&self) -> &u64 {
        &self.value
    }
    pub fn name(&self) -> &'a [u8] {
        self.name
    }
    pub fn as_str(&self) -> Result<&str> {
        Ok(std::str::from_utf8(self.name)?)
    }
    pub fn len(&self) -> usize {
        8 + self.name.len()
    }
}

impl<'a> Display for EnumRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.value == 0 {
            return Ok(());
        }

        write!(f, "{}", String::from_utf8_lossy(self.name))
    }
}

impl<'a> Eq for EnumRef<'a> {}

impl<'a> PartialEq for EnumRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<'a> Ord for EnumRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<'a> PartialOrd for EnumRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl<'a> ToString for EnumRef<'a> {
    fn to_string(&self) -> String {
        String::from_utf8_lossy(self.name).to_string()
    }
}

pub trait EnumEncoder: NumberEncoder {
    #[inline]
    fn write_enum(&mut self, data: EnumRef) -> Result<()> {
        self.write_u64_le(data.value as u64)?;
        self.write_bytes(data.name)?;
        Ok(())
    }

    #[inline]
    fn write_enum_to_chunk(&mut self, value: u64, name: &[u8]) -> Result<()> {
        self.write_u64_le(value)?;
        self.write_bytes(name)?;
        Ok(())
    }
}

impl<T: BufferWriter> EnumEncoder for T {}

pub trait EnumDatumPayloadChunkEncoder: NumberEncoder + EnumEncoder {
    #[inline]
    fn write_enum_to_chunk_by_datum_payload_compact_bytes(
        &mut self,
        mut src_payload: &[u8],
        field_type: &FieldType,
    ) -> Result<()> {
        let vn = src_payload.read_var_i64()? as usize;
        let mut data = src_payload.read_bytes(vn)?;
        let value = data.read_var_u64()?;
        let name = Enum::get_value_name(value, field_type.get_elems());
        self.write_enum_to_chunk(value, name)
    }

    #[inline]
    fn write_enum_to_chunk_by_datum_payload_uint(
        &mut self,
        mut src_payload: &[u8],
        field_type: &FieldType,
    ) -> Result<()> {
        let value = src_payload.read_u64()?;
        let name = Enum::get_value_name(value, field_type.get_elems());
        self.write_enum_to_chunk(value, name)
    }

    #[inline]
    fn write_enum_to_chunk_by_datum_payload_var_uint(
        &mut self,
        mut src_payload: &[u8],
        field_type: &FieldType,
    ) -> Result<()> {
        let value = src_payload.read_var_u64()?;
        let name = Enum::get_value_name(value, field_type.get_elems());
        self.write_enum_to_chunk(value, name)
    }
}

impl<T: BufferWriter> EnumDatumPayloadChunkEncoder for T {}

pub trait EnumDecoder: NumberDecoder {
    #[inline]
    fn read_enum_compact_bytes(&mut self, field_type: &FieldType) -> Result<Enum> {
        let vn = self.read_var_i64()? as usize;
        let mut data = self.read_bytes(vn)?;
        let value = data.read_var_u64()?;
        let name = Enum::get_value_name(value, field_type.get_elems());
        Ok(Enum::new(name.to_vec(), value))
    }

    #[inline]
    fn read_enum_uint(&mut self, field_type: &FieldType) -> Result<Enum> {
        let value = self.read_u64()?;
        let name = Enum::get_value_name(value, field_type.get_elems());
        Ok(Enum::new(name.to_vec(), value))
    }

    #[inline]
    fn read_enum_var_uint(&mut self, field_type: &FieldType) -> Result<Enum> {
        let value = self.read_var_u64()?;
        let name = Enum::get_value_name(value, field_type.get_elems());
        Ok(Enum::new(name.to_vec(), value))
    }

    #[inline]
    fn read_enum_from_chunk(&mut self) -> Result<Enum> {
        let value = self.read_u64()?;
        let name = String::from_utf8_lossy(self.bytes()).to_string();
        Ok(Enum::new(name.into_bytes(), value))
    }
}

impl<T: BufferReader> EnumDecoder for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![("c", 1, "c"), ("b", 2, "b"), ("a", 3, "a")];

        for (name, value, expect) in cases {
            let e = Enum {
                name: name.as_bytes().to_vec(),
                value,
            };

            assert_eq!(e.to_string(), expect.to_string())
        }
    }

    #[test]
    fn test_as_str() {
        let cases = vec![("c", 1, "c"), ("b", 2, "b"), ("a", 3, "a")];

        for (name, value, expect) in cases {
            let e = EnumRef {
                name: name.as_bytes(),
                value,
            };

            assert_eq!(e.as_str().expect("get str correctly"), expect)
        }
    }

    #[test]
    fn test_is_empty() {
        let s = Enum {
            name: "abc".to_owned().into_bytes(),
            value: 1,
        };

        assert!(!s.as_ref().is_empty());

        let s = Enum {
            name: vec![],
            value: 0,
        };

        assert!(s.as_ref().is_empty());
    }
}
