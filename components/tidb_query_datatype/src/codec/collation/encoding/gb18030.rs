// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use encoding_rs::GB18030;
use lazy_static::*;

use self::gb18030_data::GB18030_TO_UNICODE;
use super::*;
use crate::codec::data_type::{BytesGuard, BytesWriter};

lazy_static! {
    static ref DECODE_MAP: HashMap<u32, char> = GB18030_TO_UNICODE.iter().copied().collect();
    static ref ENCODE_MAP: HashMap<char, Vec<u8>> = GB18030_TO_UNICODE
        .iter()
        .map(|(gb18030, ch)| {
            let mut gb18030_bytes = gb18030.to_be_bytes().to_vec();
            let mut pos = 0;
            while pos < gb18030_bytes.len() && gb18030_bytes[pos] == 0 {
                pos += 1;
            }
            gb18030_bytes = gb18030_bytes[pos..].to_vec();
            (*ch, gb18030_bytes)
        })
        .collect();
}

#[derive(Debug)]
pub struct EncodingGb18030 {}

impl Encoding for EncodingGb18030 {
    #[inline]
    fn decode(data: BytesRef<'_>) -> Result<Bytes> {
        let mut res = Vec::<u8>::new();
        let l = data.len();
        if l == 0 {
            return Ok(res);
        }
        let mut base = 0;
        while base < l {
            // 1. decide the length of next character
            let offset;
            match data[base] {
                ..=0x7f => offset = 1,
                0x81..=0xfe => {
                    if base + 1 >= l {
                        return Err(Error::cannot_convert_string(
                            format_invalid_char(data).as_str(),
                            "gb18030",
                        ));
                    }
                    if 0x40 <= data[base + 1] && data[base + 1] <= 0xfe && data[base + 1] != 0x7f {
                        offset = 2;
                    } else if base + 3 < l
                        && data[base + 1] >= 0x30
                        && data[base + 1] <= 0x39
                        && data[base + 2] >= 0x81
                        && data[base + 2] <= 0xfe
                        && data[base + 3] >= 0x30
                        && data[base + 3] <= 0x39
                    {
                        offset = 4;
                    } else {
                        return Err(Error::cannot_convert_string(
                            format_invalid_char(data).as_str(),
                            "gb18030",
                        ));
                    }
                }
                _ => {
                    return Err(Error::cannot_convert_string(
                        format_invalid_char(data).as_str(),
                        "gb18030",
                    ));
                }
            }

            // 2. decode next character
            let v: u32 = match offset {
                1 => u32::from(data[base]),
                2 => u32::from(data[base]) << 8 | u32::from(data[base + 1]),
                4 => {
                    u32::from(data[base]) << 24
                        | u32::from(data[base + 1]) << 16
                        | u32::from(data[base + 2]) << 8
                        | u32::from(data[base + 3])
                }
                _ => {
                    return Err(Error::cannot_convert_string(
                        format_invalid_char(data).as_str(),
                        "gb18030",
                    ));
                }
            };
            if DECODE_MAP.contains_key(&v) {
                let mut buffer = [0; 4];
                let utf8_bytes = DECODE_MAP
                    .get(&v)
                    .unwrap()
                    .encode_utf8(&mut buffer)
                    .as_bytes();
                res.extend(utf8_bytes.to_vec());
            } else {
                match GB18030
                    .decode_without_bom_handling_and_without_replacement(&data[base..base + offset])
                {
                    Some(v) => {
                        res.extend(v.as_bytes());
                    }
                    None => {
                        return Err(Error::cannot_convert_string(
                            format_invalid_char(data).as_str(),
                            "gb18030",
                        ));
                    }
                }
            }
            base += offset;
        }

        Ok(res)
    }

    #[inline]
    fn encode(data: BytesRef<'_>) -> Result<Bytes> {
        let mut res = Vec::<u8>::new();
        let utf8_str = str::from_utf8(data)?;
        // encode each character one by one
        for ch in utf8_str.chars() {
            if ENCODE_MAP.contains_key(&ch) {
                res.extend(ENCODE_MAP.get(&ch).unwrap().iter().copied());
            } else {
                res.extend(GB18030.encode(&ch.to_string()).0.iter());
            }
        }

        Ok(res)
    }

    #[inline]
    fn lower(s: &str, writer: BytesWriter) -> BytesGuard {
        let res = s.chars().flat_map(|ch| {
            let c = ch as u32;
            match c {
                0xB5 => char::from_u32(c + 775),
                0x3D0 => char::from_u32(c - 30),
                0x3D1 => char::from_u32(c - 25),
                0x3D5 => char::from_u32(c - 15),
                0x3D6 => char::from_u32(c - 22),
                0x3F0 => char::from_u32(c - 54),
                0x3F1 => char::from_u32(c - 48),
                0x3F5 => char::from_u32(c - 64),
                0x1E9B => char::from_u32(c - 58),
                0x1FBE => char::from_u32(c - 7173),
                0x1C5 | 0x1C8 | 0x1CB | 0x1F2 | 0x3C2 => char::from_u32(c + 1),
                0x25C
                | 0x261
                | 0x265..=0x266
                | 0x26A
                | 0x26C
                | 0x282
                | 0x287
                | 0x29D..=0x29E
                | 0x37F
                | 0x3F3
                | 0x526..=0x52F
                | 0x10C7
                | 0x10CD
                | 0x10D0..=0x10FA
                | 0x10FD..=0x10FF
                | 0x13A0..=0x13F5
                | 0x13F8..=0x13FD
                | 0x1C80..=0x1C88
                | 0x1C90..=0x1CBA
                | 0x1CBD..=0x1CBF
                | 0x1D79
                | 0x1D7D
                | 0x1D8E
                | 0x2CF2..=0x2CF3
                | 0x2D27
                | 0x2D2D
                | 0xA660..=0xA661
                | 0xA698..=0xA69B
                | 0xA78D
                | 0xA790..=0xA794
                | 0xA796..=0xA7AE
                | 0xA7B0..=0xA7BF
                | 0xA7C2..=0xA7CA
                | 0xA7F5..=0xA7F6
                | 0xAB53
                | 0xAB70..=0xABBF
                | 0x104B0..=0x104D3
                | 0x104D8..=0x104FB
                | 0x10C80..=0x10CB2
                | 0x10CC0..=0x10CF2
                | 0x118A0..=0x118DF
                | 0x16E40..=0x16E7F
                | 0x1E900..=0x1E943 => char::from_u32(c),
                _ => unicode_to_lower(ch),
            }
        });
        writer.write_from_char_iter(res)
    }

    #[inline]
    fn upper(s: &str, writer: BytesWriter) -> BytesGuard {
        let res = s.chars().flat_map(|ch| {
            let c = ch as u32;
            match c {
                0xB5
                | 0x1C5
                | 0x1C8
                | 0x1CB
                | 0x1F2
                | 0x25C
                | 0x261
                | 0x265..=0x266
                | 0x26A
                | 0x26C
                | 0x282
                | 0x287
                | 0x29D..=0x29E
                | 0x37F
                | 0x3C2
                | 0x3D0
                | 0x3D1
                | 0x3D5
                | 0x3D6
                | 0x3F0
                | 0x3F1
                | 0x3F3
                | 0x3F5
                | 0x526..=0x52F
                | 0x10C7
                | 0x10CD
                | 0x10D0..=0x10FA
                | 0x10FD..=0x10FF
                | 0x13A0..=0x13F5
                | 0x13F8..=0x13FD
                | 0x1C80..=0x1C88
                | 0x1C90..=0x1CBA
                | 0x1CBD..=0x1CBF
                | 0x1D79
                | 0x1D7D
                | 0x1D8E
                | 0x1E9B
                | 0x1FBE
                | 0x2CF2..=0x2CF3
                | 0x2D27
                | 0x2D2D
                | 0xA660..=0xA661
                | 0xA698..=0xA69B
                | 0xA78D
                | 0xA790..=0xA794
                | 0xA796..=0xA7AE
                | 0xA7B0..=0xA7BF
                | 0xA7C2..=0xA7CA
                | 0xA7F5..=0xA7F6
                | 0xAB53
                | 0xAB70..=0xABBF
                | 0x104B0..=0x104D3
                | 0x104D8..=0x104FB
                | 0x10C80..=0x10CB2
                | 0x10CC0..=0x10CF2
                | 0x118A0..=0x118DF
                | 0x16E40..=0x16E7F
                | 0x1E900..=0x1E943 => char::from_u32(c),
                _ => unicode_to_upper(ch),
            }
        });
        writer.write_from_char_iter(res)
    }
}

#[cfg(test)]
mod tests {
    use bstr::ByteSlice;

    use crate::codec::collation::{encoding::EncodingGb18030, Encoding};

    #[test]
    fn test_encode() {
        let cases = vec![
            ("中文", vec![0xD6, 0xD0, 0xCE, 0xC4]),
            ("€", vec![0xA2, 0xE3]),
            ("ḿ", vec![0xA8, 0xBC]),
            ("", vec![0x81, 0x35, 0xF4, 0x37]),
            ("€ḿ", vec![0xA2, 0xE3, 0xA8, 0xBC]),
            ("😃", vec![0x94, 0x39, 0xFC, 0x39]),
            (
                "Foo © bar 𝌆 baz ☃ qux",
                vec![
                    0x46, 0x6F, 0x6F, 0x20, 0x81, 0x30, 0x84, 0x38, 0x20, 0x62, 0x61, 0x72, 0x20,
                    0x94, 0x32, 0xEF, 0x32, 0x20, 0x62, 0x61, 0x7A, 0x20, 0x81, 0x37, 0xA3, 0x30,
                    0x20, 0x71, 0x75, 0x78,
                ],
            ),
            ("ﷻ", vec![0x84, 0x30, 0xFE, 0x35]),
            // GB18030-2005
            (
                "〾⿰⿱⿲⿳⿴⿵⿶⿷⿸⿹⿺⿻",
                vec![
                    0xA9, 0x89, 0xA9, 0x8A, 0xA9, 0x8B, 0xA9, 0x8C, 0xA9, 0x8D, 0xA9, 0x8E, 0xA9,
                    0x8F, 0xA9, 0x90, 0xA9, 0x91, 0xA9, 0x92, 0xA9, 0x93, 0xA9, 0x94, 0xA9, 0x95,
                ],
            ),
            ("ǹ", vec![0xA8, 0xBF]),
            (
                "⺁㧟㩳㧐",
                vec![0xFE, 0x50, 0xFE, 0x63, 0xFE, 0x64, 0xFE, 0x65],
            ),
            ("䦃", vec![0xFE, 0x89]),
            ("︐", vec![0xA6, 0xD9]),
            ("𠂇𠂉", vec![0x95, 0x32, 0x90, 0x31, 0x95, 0x32, 0x90, 0x33]),
            ("\u{e816}\u{e855}", vec![0xFE, 0x51, 0xFE, 0x91]),
            // GB18038-2022
            ("\u{f9f1}", vec![0xFD, 0xA0]),
            (
                "\u{fa0c}\u{fa0d}\u{fa0e}",
                vec![0xFE, 0x40, 0xFE, 0x41, 0xFE, 0x42],
            ),
            (
                "\u{2e81}\u{e816}\u{e817}\u{e818}\u{2e84}",
                vec![0xFE, 0x50, 0xFE, 0x51, 0xFE, 0x52, 0xFE, 0x53, 0xFE, 0x54],
            ),
            (
                "\u{e831}\u{9fb8}\u{2eaa}\u{4056}",
                vec![0xFE, 0x6C, 0xFE, 0x6D, 0xFE, 0x6E, 0xFE, 0x6F],
            ),
            (
                "\u{f92c}\u{f979}\u{f995}\u{f9e7}\u{f9f1}\u{fa0c}\u{fa0d}\u{fa18}\u{fa20}",
                vec![
                    0xFD, 0x9C, 0xFD, 0x9D, 0xFD, 0x9E, 0xFD, 0x9F, 0xFD, 0xA0, 0xFE, 0x40, 0xFE,
                    0x41, 0xFE, 0x47, 0xFE, 0x49,
                ],
            ),
            ("\u{e5e5}\u{e000}", vec![0xA3, 0xA0, 0xAA, 0xA1]),
        ];
        for (case, expected) in cases {
            let res = EncodingGb18030::encode(case.to_string().as_bytes());
            match res {
                Ok(bytes) => {
                    assert_eq!(
                        expected, bytes,
                        "{} expected:{:02X?}, but got:{:02X?}",
                        case, expected, bytes
                    );
                }
                _ => panic!("Should succeed to encode"),
            }
        }
    }

    #[test]
    fn test_decode() {
        let cases: Vec<(Vec<u8>, &str)> = vec![
            (vec![0xD6, 0xD0, 0xCE, 0xC4], "中文"),
            (vec![0xA2, 0xE3], "€"),
            (vec![0xA8, 0xBC], "ḿ"),
            (vec![0x81, 0x35, 0xF4, 0x37], ""),
            (vec![0xA2, 0xE3, 0xA8, 0xBC], "€ḿ"),
            (vec![0x94, 0x39, 0xFC, 0x39], "😃"),
            (
                vec![
                    0x46, 0x6F, 0x6F, 0x20, 0x81, 0x30, 0x84, 0x38, 0x20, 0x62, 0x61, 0x72, 0x20,
                    0x94, 0x32, 0xEF, 0x32, 0x20, 0x62, 0x61, 0x7A, 0x20, 0x81, 0x37, 0xA3, 0x30,
                    0x20, 0x71, 0x75, 0x78,
                ],
                "Foo © bar 𝌆 baz ☃ qux",
            ),
            (vec![0x84, 0x30, 0xFE, 0x35], "ﷻ"),
            // GB18030-2005
            (
                vec![
                    0xA9, 0x89, 0xA9, 0x8A, 0xA9, 0x8B, 0xA9, 0x8C, 0xA9, 0x8D, 0xA9, 0x8E, 0xA9,
                    0x8F, 0xA9, 0x90, 0xA9, 0x91, 0xA9, 0x92, 0xA9, 0x93, 0xA9, 0x94, 0xA9, 0x95,
                ],
                "〾⿰⿱⿲⿳⿴⿵⿶⿷⿸⿹⿺⿻",
            ),
            (vec![0xA8, 0xBF], "ǹ"),
            (
                vec![
                    0xFE, 0x50, 0xFE, 0x54, 0xFE, 0x55, 0xFE, 0x56, 0xFE, 0x57, 0xFE, 0x58, 0xFE,
                    0x5A, 0xFE, 0x5B, 0xFE, 0x5C, 0xFE, 0x5D, 0xFE, 0x5E, 0xFE, 0x5F, 0xFE, 0x60,
                    0xFE, 0x62, 0xFE, 0x63, 0xFE, 0x64, 0xFE, 0x65, 0xFE, 0x68, 0xFE, 0x69, 0xFE,
                    0x6A, 0xFE, 0x6B, 0xFE, 0x6E, 0xFE, 0x6F,
                ],
                "⺁⺄㑳㑇⺈⺋㖞㘚㘎⺌⺗㥮㤘㧏㧟㩳㧐㭎㱮㳠⺧⺪䁖",
            ),
            (vec![0xFE, 0x76], "\u{e83b}"),
            (vec![0xFE, 0x89], "䦃"),
            (vec![0xA6, 0xD9], "︐"),
            (vec![0x95, 0x32, 0x90, 0x31, 0x95, 0x32, 0x90, 0x33], "𠂇𠂉"),
            (vec![0xFE, 0x51, 0xFE, 0x91], "\u{e816}\u{e855}"),
            // GB18030-2022
            (vec![0xFD, 0xA0], "\u{f9f1}"),
            (
                vec![0xFE, 0x40, 0xFE, 0x41, 0xFE, 0x42],
                "\u{fa0c}\u{fa0d}\u{fa0e}",
            ),
            (
                vec![0xFE, 0x50, 0xFE, 0x51, 0xFE, 0x52, 0xFE, 0x53, 0xFE, 0x54],
                "\u{2e81}\u{e816}\u{e817}\u{e818}\u{2e84}",
            ),
            (
                vec![0xFE, 0x6C, 0xFE, 0x6D, 0xFE, 0x6E, 0xFE, 0x6F],
                "\u{e831}\u{9fb8}\u{2eaa}\u{4056}",
            ),
            (
                vec![
                    0xFD, 0x9C, 0xFD, 0x9D, 0xFD, 0x9E, 0xFD, 0x9F, 0xFD, 0xA0, 0xFE, 0x40, 0xFE,
                    0x41, 0xFE, 0x47, 0xFE, 0x49,
                ],
                "\u{f92c}\u{f979}\u{f995}\u{f9e7}\u{f9f1}\u{fa0c}\u{fa0d}\u{fa18}\u{fa20}",
            ),
            (vec![0xA3, 0xA0, 0xAA, 0xA1], "\u{e5e5}\u{e000}"),
        ];
        for (case, expected) in cases {
            let res = EncodingGb18030::decode(case.as_bytes());
            match res {
                Ok(bytes) => {
                    let s = bytes.to_str().unwrap();
                    assert_eq!(
                        expected, s,
                        "{:02X?} expected:{}, but got:{}",
                        case, expected, s
                    )
                }
                Err(e) => {
                    panic!("Should succeed to decode;\n{}", e);
                }
            }
        }
    }
}
