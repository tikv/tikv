// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

use codec::prelude::*;
use tipb::FieldType;

use crate::{
    codec::{convert::ToInt, Result},
    expr::EvalContext,
    FieldTypeTp,
};

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
            value: &self.value,
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

impl std::hash::Hash for Enum {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EnumRef<'a> {
    name: &'a [u8],
    value: &'a u64,
}

impl<'a> EnumRef<'a> {
    pub fn new(name: &'a [u8], value: &'a u64) -> Self {
        if *value == 0 {
            Self { name: b"", value }
        } else {
            Self { name, value }
        }
    }

    pub fn to_owned(self) -> Enum {
        Enum {
            name: self.name.to_owned(),
            value: *self.value,
        }
    }

    pub fn is_empty(&self) -> bool {
        *self.value == 0
    }

    pub fn value(&self) -> u64 {
        *self.value
    }

    pub fn value_ref(&self) -> &'a u64 {
        self.value
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
        if *self.value == 0 {
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
        self.value.cmp(other.value)
    }
}

impl<'a> PartialOrd for EnumRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl<'a> ToInt for EnumRef<'a> {
    fn to_int(&self, _ctx: &mut EvalContext, _tp: FieldTypeTp) -> Result<i64> {
        Ok(*self.value as i64)
    }

    fn to_uint(&self, _ctx: &mut EvalContext, _tp: FieldTypeTp) -> Result<u64> {
        Ok(*self.value)
    }
}

impl<'a> ToString for EnumRef<'a> {
    fn to_string(&self) -> String {
        String::from_utf8_lossy(self.name).to_string()
    }
}

pub trait EnumEncoder: NumberEncoder {
    #[inline]
    fn write_enum_uint(&mut self, data: EnumRef<'_>) -> Result<()> {
        self.write_u64(*data.value)?;
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
        let value = self.read_u64_le()?;
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
        let cases = vec![("c", &1, "c"), ("b", &2, "b"), ("a", &3, "a")];

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

    fn get_enum_field_type() -> FieldType {
        let mut field_type = FieldType::new();
        field_type.set_tp(FieldTypeTp::Enum.to_u8().unwrap() as i32);

        let elems = protobuf::RepeatedField::from_slice(&[
            String::from("c"),
            String::from("b"),
            String::from("a"),
        ]);
        field_type.set_elems(elems);

        field_type
    }

    #[test]
    fn test_read_enum_uint() {
        let field_type = get_enum_field_type();

        let mut data: &[u8] = &[
            0, 0, 0, 0, 0, 0, 0, 3, // 1st
            0, 0, 0, 0, 0, 0, 0, 2, // 2nd
            0, 0, 0, 0, 0, 0, 0, 1, // 3rd
        ];
        let result = [
            Enum::new("a".as_bytes().to_owned(), 3),
            Enum::new("b".as_bytes().to_owned(), 2),
            Enum::new("c".as_bytes().to_owned(), 1),
        ];
        for res in result.iter() {
            let got = data.read_enum_uint(&field_type).expect("read_enum_uint");
            assert_eq!(&got, res);
        }
    }

    #[test]
    fn test_read_enum_var_uint() {
        let field_type = get_enum_field_type();

        let mut data: &[u8] = &[
            3, // 1st
            2, // 2nd
            1, // 3rd
        ];
        let result = [
            Enum::new("a".as_bytes().to_owned(), 3),
            Enum::new("b".as_bytes().to_owned(), 2),
            Enum::new("c".as_bytes().to_owned(), 1),
        ];
        for res in result.iter() {
            let got = data
                .read_enum_var_uint(&field_type)
                .expect("read_enum_var_uint");
            assert_eq!(&got, res);
        }
    }

    #[test]
    fn test_read_enum_compact_bytes() {
        let field_type = get_enum_field_type();

        let mut data: &[u8] = &[
            2, 3, // 1st
            2, 2, // 2nd
            2, 1, // 3rd
        ];
        let result = [
            Enum::new("a".as_bytes().to_owned(), 3),
            Enum::new("b".as_bytes().to_owned(), 2),
            Enum::new("c".as_bytes().to_owned(), 1),
        ];
        for res in result.iter() {
            let got = data
                .read_enum_compact_bytes(&field_type)
                .expect("read_enum_compact_bytes");
            assert_eq!(&got, res);
        }
    }

    #[test]
    fn test_write_enum() {
        let data = [
            EnumRef::new("a".as_bytes(), &3),
            EnumRef::new("b".as_bytes(), &2),
            EnumRef::new("c".as_bytes(), &1),
        ];
        let res: &[u8] = &[
            3, 0, 0, 0, 0, 0, 0, 0, 97, // 1st
            2, 0, 0, 0, 0, 0, 0, 0, 98, // 2nd
            1, 0, 0, 0, 0, 0, 0, 0, 99, // 3rd
        ];

        let mut buf = Vec::new();
        for datum in &data {
            buf.write_enum_to_chunk(*datum.value, datum.name)
                .expect("write_enum");
        }
        assert_eq!(buf.as_slice(), res);
    }

    #[test]
    fn test_write_enum_to_chunk() {
        let data = [
            ("a".as_bytes(), 3),
            ("b".as_bytes(), 2),
            ("c".as_bytes(), 1),
        ];
        let res: &[u8] = &[
            3, 0, 0, 0, 0, 0, 0, 0, 97, // 1st
            2, 0, 0, 0, 0, 0, 0, 0, 98, // 2nd
            1, 0, 0, 0, 0, 0, 0, 0, 99, // 3rd
        ];

        let mut buf = Vec::new();
        for datum in &data {
            buf.write_enum_to_chunk(datum.1, datum.0)
                .expect("write_enum_to_chunk");
        }
        assert_eq!(buf.as_slice(), res);
    }

    #[test]
    fn test_write_enum_to_chunk_by_payload_compact_bytes() {
        let field_type = get_enum_field_type();

        let src: [&[u8]; 3] = [
            &[2, 3], // 1st
            &[2, 2], // 2nd
            &[2, 1], // 3rd
        ];
        let mut dest = Vec::new();

        let res: &[u8] = &[
            3, 0, 0, 0, 0, 0, 0, 0, 97, // 1st
            2, 0, 0, 0, 0, 0, 0, 0, 98, // 2nd
            1, 0, 0, 0, 0, 0, 0, 0, 99, // 3rd
        ];
        for data in &src {
            dest.write_enum_to_chunk_by_datum_payload_compact_bytes(*data, &field_type)
                .expect("write_enum_to_chunk_by_payload_compact_bytes");
        }
        assert_eq!(&dest, res);
    }

    #[test]
    fn test_write_enum_to_chunk_by_payload_uint() {
        let field_type = get_enum_field_type();

        let src: [&[u8]; 3] = [
            &[0, 0, 0, 0, 0, 0, 0, 3], // 1st
            &[0, 0, 0, 0, 0, 0, 0, 2], // 2nd
            &[0, 0, 0, 0, 0, 0, 0, 1], // 3rd
        ];
        let mut dest = Vec::new();

        let res: &[u8] = &[
            3, 0, 0, 0, 0, 0, 0, 0, 97, // 1st
            2, 0, 0, 0, 0, 0, 0, 0, 98, // 2nd
            1, 0, 0, 0, 0, 0, 0, 0, 99, // 3rd
        ];
        for data in &src {
            dest.write_enum_to_chunk_by_datum_payload_uint(*data, &field_type)
                .expect("write_enum_to_chunk_by_payload_uint");
        }
        assert_eq!(&dest, res);
    }

    #[test]
    fn test_write_enum_to_chunk_by_payload_var_uint() {
        let field_type = get_enum_field_type();

        let src: [&[u8]; 3] = [
            &[3], // 1st
            &[2], // 2nd
            &[1], // 3rd
        ];
        let mut dest = Vec::new();

        let res: &[u8] = &[
            3, 0, 0, 0, 0, 0, 0, 0, 97, // 1st
            2, 0, 0, 0, 0, 0, 0, 0, 98, // 2nd
            1, 0, 0, 0, 0, 0, 0, 0, 99, // 3rd
        ];
        for data in &src {
            dest.write_enum_to_chunk_by_datum_payload_var_uint(*data, &field_type)
                .expect("write_enum_to_chunk_by_payload_var_uint");
        }
        assert_eq!(&dest, res);
    }
}
