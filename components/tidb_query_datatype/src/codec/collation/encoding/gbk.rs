// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use encoding_rs::GBK;

use super::*;
use crate::codec::data_type::{BytesGuard, BytesWriter};

#[derive(Debug)]
pub struct EncodingGBK;

impl Encoding for EncodingGBK {
    #[inline]
    fn decode(data: BytesRef<'_>) -> Result<Bytes> {
        match GBK.decode_without_bom_handling_and_without_replacement(data) {
            Some(v) => Ok(Bytes::from(v.as_bytes())),
            None => Err(Error::cannot_convert_string("gbk")),
        }
    }

    #[inline]
    fn encode(data: BytesRef<'_>) -> Result<Bytes> {
        Ok(Bytes::from(GBK.encode(str::from_utf8(data)?).0))
    }

    #[inline]
    // GBK lower and upper follows https://dev.mysql.com/worklog/task/?id=4583.
    fn lower(s: &str, writer: BytesWriter) -> BytesGuard {
        let res = s.chars().flat_map(|ch| {
            let c = ch as u32;
            match c {
                0x216A..=0x216B => char::from_u32(c),
                _ => char::from_u32(c).unwrap().to_lowercase().next(),
            }
        });
        writer.write_from_char_iter(res)
    }

    #[inline]
    fn upper(s: &str, writer: BytesWriter) -> BytesGuard {
        let res = s.chars().flat_map(|ch| {
            let c = ch as u32;
            match c {
                0x00E0..=0x00E1
                | 0x00E8..=0x00EA
                | 0x00EC..=0x00ED
                | 0x00F2..=0x00F3
                | 0x00F9..=0x00FA
                | 0x00FC
                | 0x0101
                | 0x0113
                | 0x011B
                | 0x012B
                | 0x0144
                | 0x0148
                | 0x014D
                | 0x016B
                | 0x01CE
                | 0x01D0
                | 0x01D2
                | 0x01D4
                | 0x01D6
                | 0x01D8
                | 0x01DA
                | 0x01DC => char::from_u32(c),
                _ => char::from_u32(c).unwrap().to_uppercase().next(),
            }
        });
        writer.write_from_char_iter(res)
    }
}
