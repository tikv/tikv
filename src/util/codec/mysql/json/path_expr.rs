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


// From MySQL 5.7, JSON path expression grammar:
//     pathExpression ::= scope (pathLeg)*
//     scope ::= [ columnReference ] '$'
//     columnReference ::= // omit...
//     pathLeg ::= member | arrayLocation | '**'
//     member ::= '.' (keyName | '*')
//     arrayLocation ::= '[' (non-negative-integer | '*') ']'
//     keyName ::= ECMAScript-identifier | ECMAScript-string-literal
//
// And some implementation limits in MySQL 5.7:
//     1) columnReference in scope must be empty now;
//     2) double asterisk(**) could not be last leg;
//
// Examples:
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]

// FIXME: remove following later
#![allow(dead_code)]

use std::ops::Index;
use std::ascii::AsciiExt;
use regex::Regex;
use super::super::Result;
use super::functions::unquote_string;

pub const PATH_EXPR_ASTERISK: &'static str = "*";

// [a-zA-Z_][a-zA-Z0-9_]* matches any identifier;
// "[^"\\]*(\\.[^"\\]*)*" matches any string literal which can carry escaped quotes.
const PATH_EXPR_LEG_RE_STR: &'static str =
    r#"(\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)"#;

#[derive(Clone, Debug, PartialEq)]
pub enum PathLeg {
    /// `Key` indicates the path leg  with '.key'.
    Key(String),
    /// `Index` indicates the path leg with form '[number]'.
    Index(i32),
    /// `DoubleAsterisk` indicates the path leg with form '**'.
    DoubleAsterisk,
}

// ArrayIndexAsterisk is for parsing '*' into a number.
// we need this number represent "all".
pub const PATH_EXPR_ARRAY_INDEX_ASTERISK: i32 = -1;

pub type PathExpressionFlag = u8;

pub const PATH_EXPRESSION_CONTAINS_ASTERISK: PathExpressionFlag = 0x01;
pub const PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK: PathExpressionFlag = 0x02;

pub fn contains_any_asterisk(flags: PathExpressionFlag) -> bool {
    (flags & (PATH_EXPRESSION_CONTAINS_ASTERISK | PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK)) != 0
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PathExpression {
    pub legs: Vec<PathLeg>,
    pub flags: PathExpressionFlag,
}

/// Parses a JSON path expression. Returns a `PathExpression`
/// object which can be used in `JSON_EXTRACT`, `JSON_SET` and so on.
pub fn parse_json_path_expr(path_expr: &str) -> Result<PathExpression> {
    // Find the position of first '$'. If any no-blank characters in
    // path_expr[0: dollarIndex], return an error.
    let dollar_index = match path_expr.find('$') {
        Some(i) => i,
        None => return Err(box_err!("Invalid JSON path: {}", path_expr)),
    };
    if path_expr.index(0..dollar_index).char_indices().any(|(_, c)| !c.is_ascii_whitespace()) {
        return Err(box_err!("Invalid JSON path: {}", path_expr));
    }

    let expr = path_expr.index(dollar_index + 1..).trim_left();

    let re = Regex::new(PATH_EXPR_LEG_RE_STR).unwrap();
    let mut legs = vec![];
    let mut flags = PathExpressionFlag::default();
    let mut last_end = 0;
    for (start, end) in re.find_iter(expr) {
        // Check all characters between two legs are blank.
        if expr.index(last_end..start).char_indices().any(|(_, c)| !c.is_ascii_whitespace()) {
            return Err(box_err!("Invalid JSON path: {}", path_expr));
        }
        last_end = end;

        let next_char = expr.index(start..).chars().next().unwrap();
        if next_char == '[' {
            // The leg is an index of a JSON array.
            let leg = expr[start + 1..end].trim();
            let index_str = leg[0..leg.len() - 1].trim();
            let index = if index_str == PATH_EXPR_ASTERISK {
                flags |= PATH_EXPRESSION_CONTAINS_ASTERISK;
                PATH_EXPR_ARRAY_INDEX_ASTERISK
            } else {
                box_try!(index_str.parse::<i32>())
            };
            legs.push(PathLeg::Index(index))
        } else if next_char == '.' {
            // The leg is a key of a JSON object.
            let mut key = expr[start + 1..end].trim().to_owned();
            if key == PATH_EXPR_ASTERISK {
                flags |= PATH_EXPRESSION_CONTAINS_ASTERISK;
            } else if key.chars().next().unwrap() == '"' {
                // We need unquote the origin string.
                key = try!(unquote_string(&key[1..key.len() - 1]));
            }
            legs.push(PathLeg::Key(key))
        } else {
            // The leg is '**'.
            flags |= PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
            legs.push(PathLeg::DoubleAsterisk);
        }
    }
    // Check `!expr.is_empty()` here because "$" is a valid path to specify the current JSON.
    if (last_end == 0) && (!expr.is_empty()) {
        return Err(box_err!("Invalid JSON path: {}", path_expr));
    }
    if !legs.is_empty() {
        if let PathLeg::DoubleAsterisk = *legs.last().unwrap() {
            // The last leg of a path expression cannot be '**'.
            return Err(box_err!("Invalid JSON path: {}", path_expr));
        }
    }
    Ok(PathExpression {
        legs: legs,
        flags: flags,
    })
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_path_expression_flag() {
        let mut flag = PathExpressionFlag::default();
        assert!(!contains_any_asterisk(flag));
        flag |= PATH_EXPRESSION_CONTAINS_ASTERISK;
        assert!(contains_any_asterisk(flag));
        let mut flag = PathExpressionFlag::default();
        flag |= PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
        assert!(contains_any_asterisk(flag));
    }

    #[test]
    fn test_parse_json_path_expr() {
        let mut test_cases = vec![("$",
                                   true,
                                   Some(PathExpression {
                                      legs: vec![],
                                      flags: PathExpressionFlag::default(),
                                  })),
                                  ("$.a",
                                   true,
                                   Some(PathExpression {
                                      legs: vec![PathLeg::Key(String::from("a"))],
                                      flags: PathExpressionFlag::default(),
                                  })),
                                  ("$[0]",
                                   true,
                                   Some(PathExpression {
                                      legs: vec![PathLeg::Index(0)],
                                      flags: PathExpressionFlag::default(),
                                  })),
                                  ("$**.a",
                                   true,
                                   Some(PathExpression {
                                      legs: vec![PathLeg::DoubleAsterisk,
                                                 PathLeg::Key(String::from("a"))],
                                      flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                                  })),
                                  // invalid path expressions
                                  (".a", false, None),
                                  ("xx$[1]", false, None),
                                  ("$.a xx .b", false, None),
                                  ("$[a]", false, None),
                                  ("$.\"\\u33\"", false, None),
                                  ("$**", false, None)];
        for (i, (path_expr, no_error, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_path_expr(path_expr);
            if no_error {
                assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
                let got = r.unwrap();
                let expected = expected.unwrap();
                assert_eq!(got,
                           expected,
                           "#{} expect {:?} but got {:?}",
                           i,
                           expected,
                           got);
            } else {
                assert!(r.is_err(), "#{} expect error but got {:?}", i, r);
            }
        }
    }

    #[test]
    fn test_parse_json_path_expr_contains_any_asterisk() {
        let mut test_cases = vec![
            ("$.a[b]", false),
            ("$.a[*]", true),
            ("$.*[b]", true),
            ("$**.a[b]", true),
        ];
        for (i, (path_expr, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_path_expr(path_expr);
            assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
            let e = r.unwrap();
            let b = contains_any_asterisk(e.flags);
            assert_eq!(b, expected, "#{} expect {:?} but got {:?}", i, expected, b);
        }
    }
}
