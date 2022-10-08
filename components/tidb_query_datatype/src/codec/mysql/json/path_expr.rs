// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// Refer to https://dev.mysql.com/doc/refman/5.7/en/json-path-syntax.html
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
// ```
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]
// ```

use std::{iter::Peekable, str::CharIndices};

use super::json_unquote::unquote_string;
use crate::codec::{Error, Result};

pub const PATH_EXPR_ASTERISK: &str = "*";

#[derive(Clone, Debug, PartialEq)]
pub enum PathLeg {
    /// `Key` indicates the path leg  with '.key'.
    Key(String),
    /// `Index` indicates the path leg with form 'number'.
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

#[derive(Clone, Default, Debug, PartialEq)]
pub struct PathExpression {
    pub legs: Vec<PathLeg>,
    pub flags: PathExpressionFlag,
}

impl PathExpression {
    pub fn contains_any_asterisk(&self) -> bool {
        (self.flags
            & (PATH_EXPRESSION_CONTAINS_ASTERISK | PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK))
            != 0
    }
}

/// `box_json_path_err` creates an error from the slice position
/// The position is added with 1, to count from 1 as start
macro_rules! box_json_path_err {
    ($e:expr) => {{
        box_err!(
            "Invalid JSON path expression. The error is around character position {}.",
            ($e) + 1
        )
    }};
}

struct PathExpressionTokenizer<'a> {
    input: &'a str,

    char_iterator: Peekable<CharIndices<'a>>,
}

struct Position {
    start: usize,
    end: usize,
}

/// PathExpressionToken represents a section in path expression and its position
enum PathExpressionToken {
    Leg((PathLeg, Position)),
    /// Represents the beginning "$" in the expression
    Start(Position),
}

impl<'a> Iterator for PathExpressionTokenizer<'a> {
    type Item = Result<PathExpressionToken>;

    /// Next will try to parse the next path leg and return
    /// If it returns None, it means the input is over.
    /// If it returns Some(Err(..)), it means the format is error.
    /// If it returns Some(Ok(..)), it represents the next token.
    fn next(&mut self) -> Option<Result<PathExpressionToken>> {
        self.trim_white_spaces();
        // Trim all spaces at first
        if self.reached_end() {
            return None;
        };

        let (start, ch) = *self.char_iterator.peek().unwrap();
        match ch {
            '$' => {
                self.char_iterator.next();
                Some(Ok(PathExpressionToken::Start(Position {
                    start,
                    end: self.current_index(),
                })))
            }
            '.' => Some(self.next_key()),
            '[' => Some(self.next_index()),
            '*' => Some(self.next_double_asterisk()),
            _ => Some(Err(box_json_path_err!(self.current_index()))),
        }
    }
}

impl<'a> PathExpressionTokenizer<'a> {
    fn new(input: &'a str) -> PathExpressionTokenizer<'a> {
        PathExpressionTokenizer {
            input,
            char_iterator: input.char_indices().peekable(),
        }
    }

    /// Returns the current index on the slice
    fn current_index(&mut self) -> usize {
        match self.char_iterator.peek() {
            Some((start, _)) => *start,
            None => self.input.len(),
        }
    }

    /// `trim_while_spaces` removes following spaces
    fn trim_white_spaces(&mut self) {
        while self
            .char_iterator
            .next_if(|(_, ch)| ch.is_whitespace())
            .is_some()
        {}
    }

    /// Returns whether the input has reached the end
    fn reached_end(&mut self) -> bool {
        return self.char_iterator.peek().is_none();
    }

    fn next_key(&mut self) -> Result<PathExpressionToken> {
        let (start, _) = self.char_iterator.next().unwrap();

        self.trim_white_spaces();
        if self.reached_end() {
            return Err(box_json_path_err!(self.current_index()));
        }

        match *self.char_iterator.peek().unwrap() {
            (_, '*') => {
                self.char_iterator.next().unwrap();

                Ok(PathExpressionToken::Leg((
                    PathLeg::Key(PATH_EXPR_ASTERISK.to_string()),
                    Position {
                        start,
                        end: self.current_index(),
                    },
                )))
            }
            (mut key_start, '"') => {
                // Skip this '"' character
                key_start += 1;
                self.char_iterator.next().unwrap();

                // Next until the next '"' character
                while self.char_iterator.next_if(|(_, ch)| *ch != '"').is_some() {}

                // Now, it's a '"' or the end
                if self.char_iterator.peek().is_none() {
                    return Err(box_json_path_err!(self.current_index()));
                }

                // `key_end` is the index of '"'
                let key_end = self.current_index();
                self.char_iterator.next().unwrap();

                let key = unquote_string(unsafe { self.input.get_unchecked(key_start..key_end) })?;
                for ch in key.chars() {
                    // According to JSON standard, a string cannot
                    // contain any ASCII control characters
                    if ch.is_control() {
                        // TODO: add the concrete error location
                        // after unquote, we lost the map between
                        // the character and input position.
                        return Err(box_json_path_err!(key_start));
                    }
                }

                Ok(PathExpressionToken::Leg((
                    PathLeg::Key(key),
                    Position {
                        start,
                        end: self.current_index(),
                    },
                )))
            }
            (key_start, _) => {
                // We have to also check the current value
                while self
                    .char_iterator
                    .next_if(|(_, ch)| {
                        !(ch.is_whitespace() || *ch == '.' || *ch == '[' || *ch == '*')
                    })
                    .is_some()
                {}

                // Now it reaches the end or a whitespace/./[/*
                let key_end = self.current_index();

                // The start character is not available
                if key_end == key_start {
                    return Err(box_json_path_err!(key_start));
                }

                let key = unsafe { self.input.get_unchecked(key_start..key_end) }.to_string();

                // It's not quoted, we'll have to validate whether it's an available ECMEScript
                // identifier
                for (i, c) in key.char_indices() {
                    if i == 0 && c.is_ascii_digit() {
                        return Err(box_json_path_err!(key_start + i));
                    }
                    if !c.is_ascii_alphanumeric() && c != '_' && c != '$' && c.is_ascii() {
                        return Err(box_json_path_err!(key_start + i));
                    }
                }

                Ok(PathExpressionToken::Leg((
                    PathLeg::Key(key),
                    Position {
                        start,
                        end: key_end,
                    },
                )))
            }
        }
    }

    fn next_index(&mut self) -> Result<PathExpressionToken> {
        let (start, _) = self.char_iterator.next().unwrap();

        self.trim_white_spaces();
        if self.reached_end() {
            return Err(box_json_path_err!(self.current_index()));
        }

        return match self.char_iterator.next().unwrap() {
            (_, '*') => {
                // Then it's a glob array index
                self.trim_white_spaces();
                if self.reached_end() {
                    return Err(box_json_path_err!(self.current_index()));
                }

                if self.char_iterator.next_if(|(_, ch)| *ch == ']').is_none() {
                    return Err(box_json_path_err!(self.current_index()));
                }

                Ok(PathExpressionToken::Leg((
                    PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK),
                    Position {
                        start,
                        end: self.current_index(),
                    },
                )))
            }
            (number_start, '0'..='9') => {
                // Then it's a number array index
                while self
                    .char_iterator
                    .next_if(|(_, ch)| ch.is_ascii_digit())
                    .is_some()
                {}
                let number_end = self.current_index();

                self.trim_white_spaces();
                // now, it reaches the end of input, or reaches a non-digit character
                match self.char_iterator.peek() {
                    Some((_, ']')) => {}
                    Some((pos, _)) => {
                        return Err(box_json_path_err!(pos));
                    }
                    None => {
                        return Err(box_json_path_err!(self.current_index()));
                    }
                }
                self.char_iterator.next().unwrap();

                let index = self.input[number_start..number_end]
                    .parse::<i32>()
                    .map_err(|_| -> Error { box_json_path_err!(number_end) })?;
                Ok(PathExpressionToken::Leg((
                    PathLeg::Index(index),
                    Position {
                        start,
                        end: self.current_index(),
                    },
                )))
            }
            (pos, _) => Err(box_json_path_err!(pos)),
        };
    }

    fn next_double_asterisk(&mut self) -> Result<PathExpressionToken> {
        let (start, _) = self.char_iterator.next().unwrap();

        match self.char_iterator.next() {
            Some((end, '*')) => {
                // Three or more asterisks are not allowed
                if let Some((pos, '*')) = self.char_iterator.peek() {
                    return Err(box_json_path_err!(pos));
                }

                Ok(PathExpressionToken::Leg((
                    PathLeg::DoubleAsterisk,
                    Position { start, end },
                )))
            }
            Some((pos, _)) => Err(box_json_path_err!(pos)),
            None => Err(box_json_path_err!(self.current_index())),
        }
    }
}

/// Parses a JSON path expression. Returns a `PathExpression`
/// object which can be used in `JSON_EXTRACT`, `JSON_SET` and so on.
pub fn parse_json_path_expr(path_expr: &str) -> Result<PathExpression> {
    let mut legs = Vec::new();
    let tokenizer = PathExpressionTokenizer::new(path_expr);
    let mut flags = PathExpressionFlag::default();

    let mut started = false;
    let mut last_position = Position { start: 0, end: 0 };
    for (index, token) in tokenizer.enumerate() {
        let token = token?;

        match token {
            PathExpressionToken::Leg((leg, position)) => {
                if !started {
                    return Err(box_json_path_err!(position.start));
                }

                match &leg {
                    PathLeg::Key(key) => {
                        if key == PATH_EXPR_ASTERISK {
                            flags |= PATH_EXPRESSION_CONTAINS_ASTERISK
                        }
                    }
                    PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK) => {
                        flags |= PATH_EXPRESSION_CONTAINS_ASTERISK
                    }
                    PathLeg::DoubleAsterisk => flags |= PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                    _ => {}
                }

                legs.push(leg.clone());
                last_position = position;
            }
            PathExpressionToken::Start(position) => {
                started = true;

                if index != 0 {
                    return Err(box_json_path_err!(position.start));
                }
            }
        }
    }

    // There is no available token
    if !started {
        return Err(box_json_path_err!(path_expr.len()));
    }
    // The last one cannot be the double asterisk
    if !legs.is_empty() && legs.last().unwrap() == &PathLeg::DoubleAsterisk {
        return Err(box_json_path_err!(last_position.end));
    }

    Ok(PathExpression { legs, flags })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_expression_flag() {
        let mut e = PathExpression {
            legs: vec![],
            flags: PathExpressionFlag::default(),
        };
        assert!(!e.contains_any_asterisk());
        e.flags |= PATH_EXPRESSION_CONTAINS_ASTERISK;
        assert!(e.contains_any_asterisk());
        e.flags = PathExpressionFlag::default();
        e.flags |= PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
        assert!(e.contains_any_asterisk());
    }

    #[test]
    fn test_parse_json_path_expr() {
        let mut test_cases = vec![
            (
                "$",
                None,
                Some(PathExpression {
                    legs: vec![],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.a",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(String::from("a"))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$ .a. $",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::Key(String::from("a")),
                        PathLeg::Key(String::from("$")),
                    ],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.\"hello world\"",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(String::from("hello world"))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.  \"你好 世界\"  ",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(String::from("你好 世界"))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.   ❤️  ",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(String::from("❤️"))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.   你好  ",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(String::from("你好"))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$[    0    ]",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$**.a",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::DoubleAsterisk, PathLeg::Key(String::from("a"))],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            (
                "   $  **  . a",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::DoubleAsterisk, PathLeg::Key(String::from("a"))],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            (
                "   $  **  . $",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::DoubleAsterisk, PathLeg::Key(String::from("$"))],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            // invalid path expressions
            (
                "   $  **  . 5",
                Some("Invalid JSON path expression. The error is around character position 13."),
                None,
            ),
            (
                ".a",
                Some("Invalid JSON path expression. The error is around character position 1."),
                None,
            ),
            (
                "xx$[1]",
                Some("Invalid JSON path expression. The error is around character position 1."),
                None,
            ),
            (
                "$.a xx .b",
                Some("Invalid JSON path expression. The error is around character position 5."),
                None,
            ),
            (
                "$[a]",
                Some("Invalid JSON path expression. The error is around character position 3."),
                None,
            ),
            (
                "$.\"\\u33\"",
                // TODO: pass the position in the unquote unicode error
                Some("Invalid unicode, byte len too short"),
                None,
            ),
            (
                "$**",
                Some("Invalid JSON path expression. The error is around character position 3."),
                None,
            ),
            (
                "$.\"a\\t\"",
                Some("Invalid JSON path expression. The error is around character position 4."),
                None,
            ),
            (
                "$ .a $",
                Some("Invalid JSON path expression. The error is around character position 6."),
                None,
            ),
            (
                "$ [ 2147483648 ]",
                Some("Invalid JSON path expression. The error is around character position 15."),
                None,
            ),
        ];
        for (i, (path_expr, error_message, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_path_expr(path_expr);

            match error_message {
                Some(error_message) => {
                    assert!(r.is_err(), "#{} expect error but got {:?}", i, r);

                    let got = r.err().unwrap().to_string();
                    assert!(
                        got.contains(error_message),
                        "#{} error message {} should contain {}",
                        i,
                        got,
                        error_message
                    )
                }
                None => {
                    assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
                    let got = r.unwrap();
                    let expected = expected.unwrap();
                    assert_eq!(
                        got, expected,
                        "#{} expect {:?} but got {:?}",
                        i, expected, got
                    );
                }
            }
        }
    }

    #[test]
    fn test_parse_json_path_expr_contains_any_asterisk() {
        let mut test_cases = vec![
            ("$.a[0]", false),
            ("$.a[*]", true),
            ("$.*[0]", true),
            ("$**.a[0]", true),
        ];
        for (i, (path_expr, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_path_expr(path_expr);
            assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
            let e = r.unwrap();
            let b = e.contains_any_asterisk();
            assert_eq!(b, expected, "#{} expect {:?} but got {:?}", i, expected, b);
        }
    }
}
