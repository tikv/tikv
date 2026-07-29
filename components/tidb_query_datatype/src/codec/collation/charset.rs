// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::str;

use super::*;

pub struct CharsetBinary;

impl Charset for CharsetBinary {
    type Char = u8;

    #[inline]
    fn validate(_: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)> {
        if data.is_empty() {
            None
        } else {
            Some((data[0], 1))
        }
    }

    fn charset() -> crate::Charset {
        crate::Charset::Binary
    }
}

pub struct CharsetUtf8mb4;

impl Charset for CharsetUtf8mb4 {
    type Char = char;

    #[inline]
    fn validate(bstr: &[u8]) -> Result<()> {
        str::from_utf8(bstr)?;
        Ok(())
    }

    #[inline]
    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)> {
        // Match Go's utf8.DecodeRune behavior used by TiDB: malformed UTF-8
        // produces the replacement character and consumes one byte.
        let first = *data.first()?;
        if first < 0x80 {
            return Some((first as char, 1));
        }
        let width = match first {
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => return Some((char::REPLACEMENT_CHARACTER, 1)),
        };
        match data
            .get(..width)
            .and_then(|bytes| str::from_utf8(bytes).ok())
            .and_then(|s| s.chars().next())
        {
            Some(ch) => Some((ch, width)),
            None => Some((char::REPLACEMENT_CHARACTER, 1)),
        }
    }

    fn charset() -> crate::Charset {
        crate::Charset::Utf8Mb4
    }
}

// gbk character data actually stored with utf8mb4 character encoding.
pub type CharsetGbk = CharsetUtf8mb4;

// gb18030 character data actually stored with utf8mb4 character encoding.
pub type CharsetGb18030 = CharsetUtf8mb4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8mb4_decode_one() {
        for (input, expected) in [
            ("a", ('a', 1)),
            ("é", ('é', 2)),
            ("你", ('你', 3)),
            ("🐶", ('🐶', 4)),
        ] {
            assert_eq!(CharsetUtf8mb4::decode_one(input.as_bytes()), Some(expected));
        }
        assert_eq!(CharsetUtf8mb4::decode_one(b""), None);

        // Like Go's utf8.DecodeRune, malformed encodings consume one byte and
        // produce the replacement character.
        for input in [
            &[0xE4][..],
            &[0xAA][..],
            &[0xC3, 0x28][..],
            &[0xED, 0xA0, 0x80][..],
            &[0xF5, 0x80, 0x80, 0x80][..],
        ] {
            assert_eq!(
                CharsetUtf8mb4::decode_one(input),
                Some((char::REPLACEMENT_CHARACTER, 1))
            );
        }
    }
}
