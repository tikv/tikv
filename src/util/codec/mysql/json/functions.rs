// Copyright 2017 PingCAP, Inc.
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

// FIXME: remove following later
#![allow(dead_code)]

use std::{u32, char};
use super::super::Result;
use super::json::Json;

const ESCAPED_UNICODE_BYTES_SIZE: usize = 4;

impl Json {
    pub fn unquote(&self) -> Result<String> {
        match *self {
            Json::String(ref s) => unquote_string(s),
            _ => Ok(format!("{:?}", self)),
        }
    }
}

// unquote_string recognizes the escape sequences shown in:
// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#
// json-unquote-character-escape-sequences
pub fn unquote_string(s: &str) -> Result<String> {
    let mut ret = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let c = match chars.next() {
                Some(c) => c,
                None => return Err(box_err!("Missing a closing quotation mark in string")),
            };
            match c {
                '"' => ret.push('"'),
                'b' => ret.push('\x08'),
                'f' => ret.push('\x0C'),
                'n' => ret.push('\x0A'),
                'r' => ret.push('\x0D'),
                't' => ret.push('\x0B'),
                '\\' => ret.push('\\'),
                'u' => {
                    let mut unicode = String::with_capacity(ESCAPED_UNICODE_BYTES_SIZE);
                    for _ in 0..ESCAPED_UNICODE_BYTES_SIZE {
                        match chars.next() {
                            Some(c) => unicode.push(c),
                            None => return Err(box_err!("Invalid unicode: {}", unicode)),
                        }
                    }
                    let utf8 = try!(decode_escaped_unicode(&unicode));
                    ret.push(utf8);
                }
                _ => ret.push(c),
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
mod test {
    use std::collections::BTreeMap;
    use super::*;

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
            assert_eq!(got,
                       expected,
                       "#{} expect {:?} but got {:?}",
                       i,
                       expected,
                       got);
        }
    }

    #[test]
    fn test_json_unquote() {
        // test unquote json string
        let mut test_cases = vec![("\\b", true, Some("\x08")),
                                  ("\\f", true, Some("\x0C")),
                                  ("\\n", true, Some("\x0A")),
                                  ("\\r", true, Some("\x0D")),
                                  ("\\t", true, Some("\x0B")),
                                  ("\\\\", true, Some("\x5c")),
                                  ("\\u597d", true, Some("好")),
                                  ("0\\u597d0", true, Some("0好0")),
                                  ("[", true, Some("[")),
                                  // invalid input
                                  ("\\", false, None),
                                  ("\\u59", false, None)];
        for (i, (input, no_error, expected)) in test_cases.drain(..).enumerate() {
            let j = Json::String(String::from(input));
            let r = j.unquote();
            if no_error {
                assert!(r.is_ok(), "#{} expect unquote ok but got err {:?}", i, r);
                let got = r.unwrap();
                let expected = String::from(expected.unwrap());
                assert_eq!(got,
                           expected,
                           "#{} expect {:?} but got {:?}",
                           i,
                           expected,
                           got);
            } else {
                assert!(r.is_err(), "#{} expected error but got {:?}", i, r);
            }
        }

        // test unquote other json types
        let mut test_cases = vec![Json::Object(BTreeMap::new()),
                                  Json::Array(vec![]),
                                  Json::I64(2017),
                                  Json::Double(19.28),
                                  Json::Boolean(true),
                                  Json::None];
        for (i, j) in test_cases.drain(..).enumerate() {
            let expected = format!("{:?}", j);
            let r = j.unquote();
            assert!(r.is_ok(), "#{} expect unquote ok but got err {:?}", i, r);
            let got = r.unwrap();
            assert_eq!(got,
                       expected,
                       "#{} expect {:?} but got {:?}",
                       i,
                       expected,
                       got);
        }
    }
}
