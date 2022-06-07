// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{char, str, u32};

use super::{super::Result, JsonRef, JsonType};

const ESCAPED_UNICODE_BYTES_SIZE: usize = 4;

const CHAR_BACKSPACE: char = '\x08';
const CHAR_HORIZONTAL_TAB: char = '\x09';
const CHAR_LINEFEED: char = '\x0A';
const CHAR_FORMFEED: char = '\x0C';
const CHAR_CARRIAGE_RETURN: char = '\x0D';

impl<'a> JsonRef<'a> {
    /// `unquote` recognizes the escape sequences shown in:
    /// <https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html>
    /// json-unquote-character-escape-sequences
    ///
    /// See `Unquote()` in TiDB `json/binary_function.go`
    pub fn unquote(&self) -> Result<String> {
        match self.get_type() {
            JsonType::String => {
                let s = self.get_str()?;
                unquote_string(s)
            }
            _ => Ok(self.to_string()),
        }
    }
}

pub fn unquote_string(s: &str) -> Result<String> {
    let mut ret = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let c = match chars.next() {
                Some(c) => c,
                None => return Err(box_err!("Incomplete escaped sequence")),
            };
            match c {
                '"' => ret.push('"'),
                'b' => ret.push(CHAR_BACKSPACE),
                'f' => ret.push(CHAR_FORMFEED),
                'n' => ret.push(CHAR_LINEFEED),
                'r' => ret.push(CHAR_CARRIAGE_RETURN),
                't' => ret.push(CHAR_HORIZONTAL_TAB),
                '\\' => ret.push('\\'),
                'u' => {
                    let b = chars.as_str().as_bytes();
                    if b.len() < ESCAPED_UNICODE_BYTES_SIZE {
                        return Err(box_err!("Invalid unicode, byte len too short: {:?}", b));
                    }
                    let unicode = str::from_utf8(&b[0..ESCAPED_UNICODE_BYTES_SIZE])?;
                    if unicode.len() != ESCAPED_UNICODE_BYTES_SIZE {
                        return Err(box_err!("Invalid unicode, char len too short: {}", unicode));
                    }
                    let utf8 = decode_escaped_unicode(unicode)?;
                    ret.push(utf8);
                    for _ in 0..ESCAPED_UNICODE_BYTES_SIZE {
                        chars.next();
                    }
                }
                _ => {
                    // For all other escape sequences, backslash is ignored.
                    ret.push(c);
                }
            }
        } else {
            ret.push(ch);
        }
    }
    Ok(ret)
}

fn decode_escaped_unicode(s: &str) -> Result<char> {
    let u = box_try!(u32::from_str_radix(s, 16));
    char::from_u32(u).ok_or(box_err!("invalid char from: {}", s))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{super::Json, *};

    #[test]
    fn test_decode_escaped_unicode() {
        let mut test_cases = vec![
            ("5e8a", '床'),
            ("524d", '前'),
            ("660e", '明'),
            ("6708", '月'),
            ("5149", '光'),
        ];
        for (i, (escaped, expected)) in test_cases.drain(..).enumerate() {
            let d = decode_escaped_unicode(escaped);
            assert!(d.is_ok(), "#{} expect ok but got err {:?}", i, d);
            let got = d.unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?} but got {:?}",
                i, expected, got
            );
        }
    }

    #[test]
    fn test_json_unquote() {
        // test unquote json string
        let mut test_cases = vec![
            ("\\b", true, Some("\x08")),
            ("\\f", true, Some("\x0C")),
            ("\\n", true, Some("\x0A")),
            ("\\r", true, Some("\x0D")),
            ("\\t", true, Some("\x09")),
            ("\\\\", true, Some("\x5c")),
            ("\\u597d", true, Some("好")),
            ("0\\u597d0", true, Some("0好0")),
            ("\\a", true, Some("a")),
            ("[", true, Some("[")),
            // invalid input
            ("\\", false, None),
            ("\\u59", false, None),
        ];
        for (i, (input, no_error, expected)) in test_cases.drain(..).enumerate() {
            let j = Json::from_string(String::from(input)).unwrap();
            let r = j.as_ref().unquote();
            if no_error {
                assert!(r.is_ok(), "#{} expect unquote ok but got err {:?}", i, r);
                let got = r.unwrap();
                let expected = String::from(expected.unwrap());
                assert_eq!(
                    got, expected,
                    "#{} expect {:?} but got {:?}",
                    i, expected, got
                );
            } else {
                assert!(r.is_err(), "#{} expected error but got {:?}", i, r);
            }
        }

        // test unquote other json types
        let mut test_cases = vec![
            Json::from_object(BTreeMap::new()).unwrap(),
            Json::from_array(vec![]).unwrap(),
            Json::from_i64(2017).unwrap(),
            Json::from_f64(19.28).unwrap(),
            Json::from_bool(true).unwrap(),
            Json::none().unwrap(),
        ];
        for (i, j) in test_cases.drain(..).enumerate() {
            let expected = j.to_string();
            let r = j.as_ref().unquote();
            assert!(r.is_ok(), "#{} expect unquote ok but got err {:?}", i, r);
            let got = r.unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?} but got {:?}",
                i, expected, got
            );
        }
    }
}
