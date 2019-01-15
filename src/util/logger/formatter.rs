// Copyright 2019 PingCAP, Inc.
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

// Copyright 2018 The Serde Json Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;

#[inline]
fn is_escape_key(byte: u8) -> bool {
    byte <= 0x20 || byte == 0x22 || byte == 0x3D || byte == 0x5B || byte == 0x5D
}

#[inline]
fn need_escape(bytes: &[u8]) -> bool {
    for &byte in bytes {
        if is_escape_key(byte) {
            return true;
        }
    }
    false
}

pub fn write_escaped_str<W: ?Sized>(writer: &mut W, value: &str) -> io::Result<()>
where
    W: io::Write,
{
    if !need_escape(value.as_bytes()) {
        writer.write_all(value.as_bytes())?;
    } else {
        writer.write_all(b"\"")?;
        format_escaped_str_contents(writer, value)?;
        writer.write_all(b"\"")?;
    }
    Ok(())
}

fn format_escaped_str_contents<W: ?Sized>(writer: &mut W, value: &str) -> io::Result<()>
where
    W: io::Write,
{
    let bytes = value.as_bytes();

    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = ESCAPE[byte as usize];
        if escape == 0 {
            continue;
        }

        if start < i {
            write_string_fragment(writer, &value[start..i])?;
        }

        let char_escape = CharEscape::from_escape_table(escape, byte);
        write_char_escape(writer, char_escape)?;

        start = i + 1;
    }

    if start != bytes.len() {
        write_string_fragment(writer, &value[start..])?;
    }

    Ok(())
}

const BB: u8 = b'b'; // \x08
const TT: u8 = b't'; // \x09
const NN: u8 = b'n'; // \x0A
const FF: u8 = b'f'; // \x0C
const RR: u8 = b'r'; // \x0D
const QU: u8 = b'"'; // \x22
const BS: u8 = b'\\'; // \x5C
const UU: u8 = b'u'; // \x00...\x1F except the ones above
const __: u8 = 0;

// Lookup table of escape sequences. A value of b'x' at index i means that byte
// i is escaped as "\x" in JSON. A value of 0 means that byte i is not escaped.
static ESCAPE: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    UU, UU, UU, UU, UU, UU, UU,
    UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 3
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
    __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __, // 5
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

/// Writes a string fragment that doesn't need any escaping to the
/// specified writer.
#[inline]
fn write_string_fragment<W: ?Sized>(writer: &mut W, fragment: &str) -> io::Result<()>
where
    W: io::Write,
{
    writer.write_all(fragment.as_bytes())
}

/// Represents a character escape code in a type-safe manner.
pub enum CharEscape {
    /// An escaped quote `"`
    Quote,
    /// An escaped reverse solidus `\`
    ReverseSolidus,
    /// An escaped backspace character (usually escaped as `\b`)
    Backspace,
    /// An escaped form feed character (usually escaped as `\f`)
    FormFeed,
    /// An escaped line feed character (usually escaped as `\n`)
    LineFeed,
    /// An escaped carriage return character (usually escaped as `\r`)
    CarriageReturn,
    /// An escaped tab character (usually escaped as `\t`)
    Tab,
    /// An escaped ASCII plane control character (usually escaped as
    /// `\u00XX` where `XX` are two hex characters)
    AsciiControl(u8),
}

impl CharEscape {
    #[inline]
    fn from_escape_table(escape: u8, byte: u8) -> CharEscape {
        match escape {
            self::BB => CharEscape::Backspace,
            self::TT => CharEscape::Tab,
            self::NN => CharEscape::LineFeed,
            self::FF => CharEscape::FormFeed,
            self::RR => CharEscape::CarriageReturn,
            self::QU => CharEscape::Quote,
            self::BS => CharEscape::ReverseSolidus,
            self::UU => CharEscape::AsciiControl(byte),
            _ => unreachable!(),
        }
    }
}

/// Writes a character escape code to the specified writer.
#[inline]
fn write_char_escape<W: ?Sized>(writer: &mut W, char_escape: CharEscape) -> io::Result<()>
where
    W: io::Write,
{
    let s = match char_escape {
        CharEscape::Quote => b"\\\"",
        CharEscape::ReverseSolidus => b"\\\\",
        CharEscape::Backspace => b"\\b",
        CharEscape::FormFeed => b"\\f",
        CharEscape::LineFeed => b"\\n",
        CharEscape::CarriageReturn => b"\\r",
        CharEscape::Tab => b"\\t",
        CharEscape::AsciiControl(byte) => {
            static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
            let bytes = &[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX_DIGITS[(byte >> 4) as usize],
                HEX_DIGITS[(byte & 0xF) as usize],
            ];
            return writer.write_all(bytes);
        }
    };

    writer.write_all(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_escaped_key() {
        let cases = [
            (b' ', true),
            (b'"', true),
            (b'=', true),
            (b'[', true),
            (b']', true),
            (0x00, true),
            (0x20, true),
            (0x21, false),
            (b'a', false),
        ];

        for (byte, expect) in &cases {
            println!("{},{}", byte, expect);
            assert_eq!(is_escape_key(*byte), *expect);
        }
    }

    #[test]
    fn test_need_escape() {
        let cases = [
            ("abc", false),
            ("a b", true),
            ("a=b", true),
            ("a[b", true),
            ("a]b", true),
            ("\u{000f}", true),
            ("💖", false),
            ("�", false),
            ("\u{7fff}\u{ffff}", false),
            ("欢迎", false),
            ("欢迎 TiKV", true),
        ];
        for (input, expect) in &cases {
            assert_eq!(
                need_escape(input.as_bytes()),
                *expect,
                "{} | {}",
                input,
                expect
            );
        }
    }
}
