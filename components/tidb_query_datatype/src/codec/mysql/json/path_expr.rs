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

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::{
        complete,
        complete::{char, none_of, satisfy, space0, space1},
    },
    combinator::{map, map_opt},
    multi::{many0, many1},
    sequence::{delimited, pair, tuple},
    IResult,
};

use super::json_unquote::unquote_string;
use crate::codec::Result;

fn lift_error_to_failure<E>(err: nom::Err<E>) -> nom::Err<E> {
    if let nom::Err::Error(err) = err {
        nom::Err::Failure(err)
    } else {
        err
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ArrayIndex {
    Left(u32),  // `Left` represents an array index start from left
    Right(u32), // `Right` represents an array index start from right
}

fn array_index_left(input: &str) -> IResult<&str, ArrayIndex> {
    let (input, index) = complete::u32(input)?;
    Ok((input, ArrayIndex::Left(index)))
}

fn array_index_last(input: &str) -> IResult<&str, ArrayIndex> {
    let (input, _) = tag("last")(input)?;

    Ok((input, ArrayIndex::Right(0)))
}

fn array_index_right(input: &str) -> IResult<&str, ArrayIndex> {
    let (input, _) = tag("last")(input)?;
    let (input, _) = space0(input)?;
    let (input, _) = char('-')(input)?;
    let (input, _) = space0(input)?;

    let (input, index) = complete::u32(input)?;
    Ok((input, ArrayIndex::Right(index)))
}

fn array_index(input: &str) -> IResult<&str, ArraySelection> {
    map(
        alt((array_index_left, array_index_right, array_index_last)),
        |index| ArraySelection::Index(index),
    )(input)
}

fn array_asterisk(input: &str) -> IResult<&str, ArraySelection> {
    map(char('*'), |_| ArraySelection::Asterisk)(input)
}

fn array_range(input: &str) -> IResult<&str, ArraySelection> {
    let (input, start) = array_index(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("to")(input)?;
    let (before_last_index, _) = space1(input)?;
    let (input, end) = array_index(before_last_index)?;

    match (start, end) {
        (ArraySelection::Index(start), ArraySelection::Index(end)) => {
            // specially check the position
            let allowed = match (start, end) {
                (ArrayIndex::Left(start), ArrayIndex::Left(end)) => start <= end,
                (ArrayIndex::Right(start), ArrayIndex::Right(end)) => start >= end,
                (..) => true,
            };
            if !allowed {
                // TODO: use a customized error kind, as the ErrorKind::Verify is designed
                // to be used in `verify` combinator
                return Err(nom::Err::Failure(nom::error::make_error(
                    before_last_index,
                    nom::error::ErrorKind::Verify,
                )));
            }
            Ok((input, ArraySelection::Range(start, end)))
        }
        _ => unreachable!(),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ArraySelection {
    Asterisk,                      // `Asterisk` select all element from array.
    Index(ArrayIndex),             // `Index` select one element from array.
    Range(ArrayIndex, ArrayIndex), // `Range` selects a closed-interval from array.
}

fn path_leg_array_selection(input: &str) -> IResult<&str, PathLeg> {
    let (input, _) = char('[')(input)?;
    let (input, _) = space0(input)?;
    let (input, leg) = map(
        alt((array_asterisk, array_range, array_index)),
        |array_selection| PathLeg::ArraySelection(array_selection),
    )(input)
    .map_err(lift_error_to_failure)?;
    let (input, _) = space0(input)?;
    let (input, _) = char(']')(input).map_err(lift_error_to_failure)?;

    Ok((input, leg))
}

#[derive(Clone, Debug, PartialEq)]
pub enum KeySelection {
    Asterisk,
    Key(String),
}

fn key_selection_asterisk(input: &str) -> IResult<&str, KeySelection> {
    map(char('*'), |_| KeySelection::Asterisk)(input)
}

fn key_selection_key(input: &str) -> IResult<&str, KeySelection> {
    let key_with_quote = map_opt(
        delimited(char('"'), many1(none_of("\"")), char('"')),
        |key: Vec<_>| {
            let key: String = key.into_iter().collect();
            let key = unquote_string(&key).ok()?;
            for ch in key.chars() {
                if ch.is_control() {
                    return None;
                }
            }
            Some(KeySelection::Key(key))
        },
    );

    let take_key_until_end = many1(satisfy(|ch| {
        !(ch.is_whitespace() || ch == '.' || ch == '[' || ch == '*')
    }));
    let key_without_quote = map_opt(take_key_until_end, |key: Vec<_>| {
        for (i, c) in key.iter().enumerate() {
            if i == 0 && c.is_ascii_digit() {
                return None;
            }
            if !c.is_ascii_alphanumeric() && *c != '_' && *c != '$' && c.is_ascii() {
                return None;
            }
        }

        Some(KeySelection::Key(key.into_iter().collect()))
    });

    alt((key_with_quote, key_without_quote))(input)
}

fn path_leg_key(input: &str) -> IResult<&str, PathLeg> {
    let (input, _) = char('.')(input)?;
    let (input, _) = space0(input)?;

    map(
        alt((key_selection_key, key_selection_asterisk)),
        |key_selection| PathLeg::Key(key_selection),
    )(input)
    .map_err(lift_error_to_failure)
}

fn path_leg_double_asterisk(input: &str) -> IResult<&str, PathLeg> {
    map(pair(char('*'), char('*')), |_| PathLeg::DoubleAsterisk)(input)
}

#[derive(Clone, Debug, PartialEq)]
pub enum PathLeg {
    /// `Key` indicates the path leg  with '.key'.
    Key(KeySelection),
    /// `ArraySelection` indicates the path leg with form '[...]'.
    ArraySelection(ArraySelection),
    /// `DoubleAsterisk` indicates the path leg with form '**'.
    DoubleAsterisk,
}

pub type PathExpressionFlag = u8;

pub const PATH_EXPRESSION_CONTAINS_ASTERISK: PathExpressionFlag = 0x01;
pub const PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK: PathExpressionFlag = 0x02;
pub const PATH_EXPRESSION_CONTAINS_RANGE: PathExpressionFlag = 0x04;

fn path_expression(input: &str) -> IResult<&str, PathExpression> {
    let mut flags = PathExpressionFlag::default();
    let (input, (_, _, legs)) = tuple((
        space0,
        char('$'),
        many0(delimited(
            space0,
            alt((
                path_leg_key,
                path_leg_array_selection,
                path_leg_double_asterisk,
            )),
            space0,
        )),
    ))(input)?;

    for leg in legs.iter() {
        match leg {
            PathLeg::DoubleAsterisk => flags |= PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
            PathLeg::Key(KeySelection::Asterisk) => flags |= PATH_EXPRESSION_CONTAINS_ASTERISK,
            PathLeg::ArraySelection(ArraySelection::Asterisk) => {
                flags |= PATH_EXPRESSION_CONTAINS_ASTERISK
            }
            PathLeg::ArraySelection(ArraySelection::Range(..)) => {
                flags |= PATH_EXPRESSION_CONTAINS_RANGE
            }
            _ => {}
        }
    }

    Ok((input, PathExpression { legs, flags }))
}

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

    pub fn contains_any_range(&self) -> bool {
        (self.flags & PATH_EXPRESSION_CONTAINS_RANGE) != 0
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

/// Parses a JSON path expression. Returns a `PathExpression`
/// object which can be used in `JSON_EXTRACT`, `JSON_SET` and so on.
///
/// See `parseJSONPathExpr` in TiDB `types/json_path_expr.go`.
pub fn parse_json_path_expr(path_expr_input: &str) -> Result<PathExpression> {
    let (left_input, path_expr) = match path_expression(path_expr_input) {
        Ok(ret) => ret,
        Err(err) => {
            let input = match err {
                nom::Err::Error(err) => err.input,
                nom::Err::Failure(err) => err.input,
                _ => {
                    unreachable!()
                }
            };

            return Err(box_json_path_err!(path_expr_input.len() - input.len()));
        }
    };

    // Some extra input is left
    if !left_input.is_empty() {
        return Err(box_json_path_err!(path_expr_input.len() - left_input.len()));
    }

    // The last one cannot be the double asterisk
    if !path_expr.legs.is_empty() && path_expr.legs.last().unwrap() == &PathLeg::DoubleAsterisk {
        return Err(box_json_path_err!(path_expr_input.len() - 1));
    }

    Ok(path_expr)
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
                    legs: vec![PathLeg::Key(KeySelection::Key(String::from("a")))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$ .a. $",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::Key(KeySelection::Key(String::from("a"))),
                        PathLeg::Key(KeySelection::Key(String::from("$"))),
                    ],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.\"hello world\"",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(KeySelection::Key(String::from("hello world")))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.  \"你好 世界\"  ",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(KeySelection::Key(String::from("你好 世界")))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.   ❤️  ",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(KeySelection::Key(String::from("❤️")))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$.   你好  ",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::Key(KeySelection::Key(String::from("你好")))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$[    0    ]",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::ArraySelection(ArraySelection::Index(
                        ArrayIndex::Left(0),
                    ))],
                    flags: PathExpressionFlag::default(),
                }),
            ),
            (
                "$**.a",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::DoubleAsterisk,
                        PathLeg::Key(KeySelection::Key(String::from("a"))),
                    ],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            (
                "   $  **  . a",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::DoubleAsterisk,
                        PathLeg::Key(KeySelection::Key(String::from("a"))),
                    ],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            (
                "   $  **  . $",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::DoubleAsterisk,
                        PathLeg::Key(KeySelection::Key(String::from("$"))),
                    ],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            (
                "   $  [ 1 to 10 ]",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::ArraySelection(ArraySelection::Range(
                        ArrayIndex::Left(1),
                        ArrayIndex::Left(10),
                    ))],
                    flags: PATH_EXPRESSION_CONTAINS_RANGE,
                }),
            ),
            (
                "   $  [ 1 to last - 10 ]",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::ArraySelection(ArraySelection::Range(
                        ArrayIndex::Left(1),
                        ArrayIndex::Right(10),
                    ))],
                    flags: PATH_EXPRESSION_CONTAINS_RANGE,
                }),
            ),
            (
                "   $  [ 1 to last-10 ]",
                None,
                Some(PathExpression {
                    legs: vec![PathLeg::ArraySelection(ArraySelection::Range(
                        ArrayIndex::Left(1),
                        ArrayIndex::Right(10),
                    ))],
                    flags: PATH_EXPRESSION_CONTAINS_RANGE,
                }),
            ),
            (
                "   $  **  [ 1 to last ]",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::DoubleAsterisk,
                        PathLeg::ArraySelection(ArraySelection::Range(
                            ArrayIndex::Left(1),
                            ArrayIndex::Right(0),
                        )),
                    ],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK
                        | PATH_EXPRESSION_CONTAINS_RANGE,
                }),
            ),
            (
                "   $  **  [ last ]",
                None,
                Some(PathExpression {
                    legs: vec![
                        PathLeg::DoubleAsterisk,
                        PathLeg::ArraySelection(ArraySelection::Index(ArrayIndex::Right(0))),
                    ],
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
                Some("Invalid JSON path expression. The error is around character position 3."),
                None,
            ),
            (
                "$**",
                Some("Invalid JSON path expression. The error is around character position 3."),
                None,
            ),
            (
                "$.\"a\\t\"",
                Some("Invalid JSON path expression. The error is around character position 3."),
                None,
            ),
            (
                "$ .a $",
                Some("Invalid JSON path expression. The error is around character position 6."),
                None,
            ),
            (
                "$ [ 4294967296 ]",
                Some("Invalid JSON path expression. The error is around character position 5."),
                None,
            ),
            (
                "$ [ 1to2 ]",
                Some("Invalid JSON path expression. The error is around character position 6."),
                None,
            ),
            (
                "$ [ 2 to 1 ]",
                Some("Invalid JSON path expression. The error is around character position 10."),
                None,
            ),
            (
                "$ [ last - 10 to last - 20 ]",
                Some("Invalid JSON path expression. The error is around character position 18."),
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

    #[test]
    fn test_parse_json_path_expr_contains_any_range() {
        let mut test_cases = vec![
            ("$.a[0]", false),
            ("$.a[*]", false),
            ("$**.a[0]", false),
            ("$.a[1 to 2]", true),
            ("$.a[1 to last - 2]", true),
        ];
        for (i, (path_expr, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_path_expr(path_expr);
            assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
            let e = r.unwrap();
            let b = e.contains_any_range();
            assert_eq!(b, expected, "#{} expect {:?} but got {:?}", i, expected, b);
        }
    }
}
