// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ord, Ordering},
    f64,
};

use super::{super::Result, ERR_CONVERT_FAILED, Json, JsonRef, JsonType, constants::*};
use crate::codec::convert::ToStringValue;

fn compare<T: Ord>(x: T, y: T) -> Ordering {
    x.cmp(&y)
}

fn compare_i64_u64(x: i64, y: u64) -> Ordering {
    if x < 0 {
        Ordering::Less
    } else {
        compare::<u64>(x as u64, y)
    }
}

/// Compares two JSON doubles exactly.
///
/// This mirrors `compareFloat64` in TiDB `pkg/types/json_binary_functions.go`,
/// which uses plain `<` / `==` with no tolerance. The coprocessor must agree
/// with TiDB root evaluation bit for bit, otherwise pushing a `<` / `>`
/// predicate down silently changes the result set: an absolute tolerance makes
/// adjacent distinct doubles below magnitude 1 (e.g. 0.25 and
/// 0.25000000000000006, one ulp apart at 5.55e-17) compare Equal, which drops
/// rows that TiDB root returns.
fn compare_f64(x: f64, y: f64) -> Option<Ordering> {
    x.partial_cmp(&y)
}

/// The acceptable error quantity when comparing a double against an integer
/// that was widened to `f64`.
///
/// This is `floatEpsilon` in TiDB `pkg/types/json_binary_functions.go`:
///
/// ```go
/// // floatEpsilon is the acceptable error quantity when comparing two float numbers.
/// const floatEpsilon = 1.e-8
/// ```
///
/// It must stay equal to TiDB's value. The coprocessor has to reproduce TiDB
/// root evaluation exactly, otherwise pushing a predicate down changes the
/// result set. A *stricter* tolerance is just as wrong as a looser one: with
/// `f64::EPSILON` (2.22e-16) the JSON double `3.000000001` compared `Greater`
/// than the integer `3` here while TiDB root called them `Equal`, so
/// `WHERE j = 3` returned the row at the root and silently dropped it once
/// pushed down.
const FLOAT_EPSILON: f64 = 1.0e-8;

/// Compares a double against an integer that was widened to `f64`, allowing for
/// the precision loss of that widening.
///
/// This is the counterpart of `compareFloat64PrecisionLoss` in TiDB
/// `pkg/types/json_binary_functions.go`, which is used only on the
/// integer-vs-double paths:
///
/// ```go
/// func compareFloat64PrecisionLoss(x, y float64) int {
///     if x-y < floatEpsilon && y-x < floatEpsilon {
///         return 0
///     } else if x-y < 0 {
///         return -1
///     }
///     return 1
/// }
/// ```
///
/// The two one-sided comparisons are reproduced literally rather than folded
/// into `(x - y).abs() < FLOAT_EPSILON`. The two are equivalent for every
/// non-NaN input, but they differ on NaN: the `abs` form falls through to
/// `partial_cmp`, which yields `None`, and `Ord for Json` unwraps that and
/// panics, whereas TiDB reports `Greater`. Matching TiDB's shape keeps the
/// coprocessor in step with the root here too.
///
/// Known limitation, shared with TiDB: a tolerance makes this comparator
/// intransitive across the integer/double boundary (`0` compares equal to both
/// `0.0` and `1e-9`, yet `1e-9 > 0.0`), so ordering a mixed column can depend
/// on input order. TiDB's `CompareBinaryJSON` has the identical property;
/// diverging from it here would be worse than matching it, so it is left
/// alone deliberately.
fn compare_f64_precision_loss(x: f64, y: f64) -> Option<Ordering> {
    if x - y < FLOAT_EPSILON && y - x < FLOAT_EPSILON {
        Some(Ordering::Equal)
    } else if x - y < 0.0 {
        Some(Ordering::Less)
    } else {
        Some(Ordering::Greater)
    }
}

impl JsonRef<'_> {
    fn get_precedence(&self) -> i32 {
        match self.get_type() {
            JsonType::Object => PRECEDENCE_OBJECT,
            JsonType::Array => PRECEDENCE_ARRAY,
            JsonType::Literal => self
                .get_literal()
                .map_or(PRECEDENCE_NULL, |_| PRECEDENCE_BOOLEAN),
            JsonType::I64 | JsonType::U64 | JsonType::Double => PRECEDENCE_NUMBER,
            JsonType::String => PRECEDENCE_STRING,
            JsonType::Opaque => PRECEDENCE_OPAQUE,
            JsonType::Date => PRECEDENCE_DATE,
            JsonType::Datetime => PRECEDENCE_DATETIME,
            JsonType::Timestamp => PRECEDENCE_DATETIME,
            JsonType::Time => PRECEDENCE_TIME,
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match self.get_type() {
            JsonType::I64 => Ok(self.get_i64() as f64),
            JsonType::U64 => Ok(self.get_u64() as f64),
            JsonType::Double => Ok(self.get_double()),
            JsonType::Literal => {
                let v = self.as_literal().unwrap();
                Ok(v.into())
            }
            _ => Err(invalid_type!(
                "{} from {} to f64",
                ERR_CONVERT_FAILED,
                self.to_string_value()
            )),
        }
    }
}

impl Eq for JsonRef<'_> {}

impl Ord for JsonRef<'_> {
    fn cmp(&self, right: &JsonRef<'_>) -> Ordering {
        self.partial_cmp(right).unwrap()
    }
}

impl PartialEq for JsonRef<'_> {
    fn eq(&self, right: &JsonRef<'_>) -> bool {
        self.partial_cmp(right)
            .is_some_and(|r| r == Ordering::Equal)
    }
}
impl PartialOrd for JsonRef<'_> {
    // See `CompareBinary` in TiDB `types/json/binary_functions.go`
    fn partial_cmp(&self, right: &JsonRef<'_>) -> Option<Ordering> {
        let precedence_diff = self.get_precedence() - right.get_precedence();
        if precedence_diff == 0 {
            if self.get_precedence() == PRECEDENCE_NULL {
                // for JSON null.
                return Some(Ordering::Equal);
            }

            return match self.get_type() {
                JsonType::I64 => match right.get_type() {
                    JsonType::I64 => Some(compare(self.get_i64(), right.get_i64())),
                    JsonType::U64 => Some(compare_i64_u64(self.get_i64(), right.get_u64())),
                    JsonType::Double => {
                        compare_f64_precision_loss(self.get_i64() as f64, right.as_f64().unwrap())
                    }
                    _ => unreachable!(),
                },
                JsonType::U64 => match right.get_type() {
                    JsonType::I64 => {
                        Some(compare_i64_u64(right.get_i64(), self.get_u64()).reverse())
                    }
                    JsonType::U64 => Some(compare(self.get_u64(), right.get_u64())),
                    JsonType::Double => {
                        compare_f64_precision_loss(self.get_u64() as f64, right.as_f64().unwrap())
                    }
                    _ => unreachable!(),
                },
                JsonType::Double => match right.get_type() {
                    // Double vs Double is compared exactly, matching TiDB's
                    // `compareFloat64`.
                    JsonType::Double => compare_f64(self.get_double(), right.get_double()),
                    // Double vs integer widens the integer to `f64`, so it keeps
                    // the precision-loss tolerance, matching TiDB's
                    // `compareFloat64Int64` / `compareFloat64Uint64`.
                    JsonType::I64 | JsonType::U64 => {
                        compare_f64_precision_loss(self.get_double(), right.as_f64().unwrap())
                    }
                    _ => unreachable!(),
                },
                JsonType::Literal => {
                    // false is less than true.
                    self.get_literal().partial_cmp(&right.get_literal())
                }
                JsonType::Object => {
                    // only equal is defined on two json objects.
                    // larger and smaller are not defined.
                    self.value().partial_cmp(right.value())
                }
                JsonType::String => {
                    if let (Ok(left), Ok(right)) = (self.get_str_bytes(), right.get_str_bytes()) {
                        left.partial_cmp(right)
                    } else {
                        return None;
                    }
                }
                JsonType::Array => {
                    let left_count = self.get_elem_count();
                    let right_count = right.get_elem_count();
                    let mut i = 0;
                    while i < left_count && i < right_count {
                        if let (Ok(left_ele), Ok(right_ele)) =
                            (self.array_get_elem(i), right.array_get_elem(i))
                        {
                            match left_ele.partial_cmp(&right_ele) {
                                order @ None
                                | order @ Some(Ordering::Greater)
                                | order @ Some(Ordering::Less) => return order,
                                Some(Ordering::Equal) => i += 1,
                            }
                        } else {
                            return None;
                        }
                    }
                    Some(left_count.cmp(&right_count))
                }
                JsonType::Opaque => {
                    if let (Ok(left), Ok(right)) =
                        (self.get_opaque_bytes(), right.get_opaque_bytes())
                    {
                        left.partial_cmp(right)
                    } else {
                        return None;
                    }
                }
                JsonType::Date | JsonType::Datetime | JsonType::Timestamp => {
                    // The jsonTypePrecedences guarantees that the DATE is only comparable with the
                    // DATE, and the DATETIME and TIMESTAMP will compare with
                    // each other
                    if let (Ok(left), Ok(right)) = (self.get_time(), right.get_time()) {
                        left.partial_cmp(&right)
                    } else {
                        return None;
                    }
                }
                JsonType::Time => {
                    if let (Ok(left), Ok(right)) = (self.get_duration(), right.get_duration()) {
                        left.partial_cmp(&right)
                    } else {
                        return None;
                    }
                }
            };
        }

        if precedence_diff > 0 {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    }
}

impl Eq for Json {}
impl Ord for Json {
    fn cmp(&self, right: &Json) -> Ordering {
        self.as_ref().partial_cmp(&right.as_ref()).unwrap()
    }
}

impl PartialEq for Json {
    fn eq(&self, right: &Json) -> bool {
        self.as_ref().partial_cmp(&right.as_ref()).unwrap() == Ordering::Equal
    }
}

impl PartialOrd for Json {
    fn partial_cmp(&self, right: &Json) -> Option<Ordering> {
        self.as_ref().partial_cmp(&right.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec::{
            data_type::Duration,
            mysql::{Time, TimeType},
        },
        expr::EvalContext,
    };

    #[test]
    fn test_cmp_json_numberic_type() {
        let cases = vec![
            (
                Json::from_i64(922337203685477581),
                Json::from_i64(922337203685477580),
                Ordering::Greater,
            ),
            (
                Json::from_i64(-1),
                Json::from_u64(18446744073709551615),
                Ordering::Less,
            ),
            (
                Json::from_i64(922337203685477580),
                Json::from_u64(922337203685477581),
                Ordering::Less,
            ),
            (Json::from_i64(2), Json::from_u64(1), Ordering::Greater),
            (
                Json::from_i64(i64::MAX),
                Json::from_u64(i64::MAX as u64),
                Ordering::Equal,
            ),
            (
                Json::from_u64(18446744073709551615),
                Json::from_i64(-1),
                Ordering::Greater,
            ),
            (
                Json::from_u64(922337203685477581),
                Json::from_i64(922337203685477580),
                Ordering::Greater,
            ),
            (Json::from_u64(1), Json::from_i64(2), Ordering::Less),
            (
                Json::from_u64(i64::MAX as u64),
                Json::from_i64(i64::MAX),
                Ordering::Equal,
            ),
            (Json::from_f64(9.0), Json::from_i64(9), Ordering::Equal),
            (Json::from_f64(8.9), Json::from_i64(9), Ordering::Less),
            (Json::from_f64(9.1), Json::from_i64(9), Ordering::Greater),
            (Json::from_f64(9.0), Json::from_u64(9), Ordering::Equal),
            (Json::from_f64(8.9), Json::from_u64(9), Ordering::Less),
            (Json::from_f64(9.1), Json::from_u64(9), Ordering::Greater),
            (Json::from_i64(9), Json::from_f64(9.0), Ordering::Equal),
            (Json::from_i64(9), Json::from_f64(8.9), Ordering::Greater),
            (Json::from_i64(9), Json::from_f64(9.1), Ordering::Less),
            (Json::from_u64(9), Json::from_f64(9.0), Ordering::Equal),
            (Json::from_u64(9), Json::from_f64(8.9), Ordering::Greater),
            (Json::from_u64(9), Json::from_f64(9.1), Ordering::Less),
        ];

        for (left, right, expected) in cases {
            let left = left.unwrap();
            let right = right.unwrap();
            assert_eq!(expected, left.partial_cmp(&right).unwrap());
        }
    }

    /// Regression test for tikv/tikv#20101.
    ///
    /// Double vs double used to go through a single helper that treated
    /// `|x - y| < f64::EPSILON` (2.22e-16) as `Ordering::Equal`. `f64::EPSILON`
    /// is the gap between 1.0 and the next double, not a universal tolerance:
    /// one ulp at 0.25 is 5.55e-17, so adjacent distinct doubles compared Equal
    /// and pushed-down `<` / `>` predicates silently dropped rows that TiDB
    /// root returned. That arm now uses [`compare_f64`], which is exact.
    #[test]
    fn test_cmp_json_adjacent_doubles() {
        // Sanity check that these really are two distinct, adjacent doubles
        // whose gap is below the old threshold.
        assert_ne!(0.25f64, 0.25000000000000006f64);
        assert_eq!(0.25f64.next_up(), 0.25000000000000006f64);
        assert!((0.25000000000000006f64 - 0.25f64).abs() < f64::EPSILON);

        let cases = vec![
            // The exact values from the issue: 1-ulp neighbours below magnitude
            // 1, which the old absolute epsilon masked.
            (
                Json::from_f64(0.25),
                Json::from_f64(0.25000000000000006),
                Ordering::Less,
            ),
            (
                Json::from_f64(0.25000000000000006),
                Json::from_f64(0.25),
                Ordering::Greater,
            ),
            (Json::from_f64(0.25), Json::from_f64(0.25), Ordering::Equal),
            // Another sub-1 neighbour pair, for the `j < x` / `j > x` shape of
            // the reported query.
            (
                Json::from_f64(0.1),
                Json::from_f64(0.1f64.next_up()),
                Ordering::Less,
            ),
            (
                Json::from_f64(0.1f64.next_up()),
                Json::from_f64(0.1),
                Ordering::Greater,
            ),
            // IEEE-754 signed zeros: `-0.0 == 0.0`, so an exact comparison
            // still reports Equal and neither ordering is preferred.
            (Json::from_f64(-0.0), Json::from_f64(0.0), Ordering::Equal),
            (Json::from_f64(0.0), Json::from_f64(-0.0), Ordering::Equal),
            // Neighbours at a magnitude where the gap already exceeded the old
            // epsilon, so these compared correctly before the fix too: proof
            // that existing behaviour is preserved rather than changed.
            (
                Json::from_f64(4.0),
                Json::from_f64(4.0f64.next_up()),
                Ordering::Less,
            ),
            (
                Json::from_f64(4.0f64.next_up()),
                Json::from_f64(4.0),
                Ordering::Greater,
            ),
            // Coarse, clearly-separated doubles: unchanged by the fix.
            (Json::from_f64(0.25), Json::from_f64(0.5), Ordering::Less),
            (Json::from_f64(0.5), Json::from_f64(0.25), Ordering::Greater),
        ];

        for (left, right, expected) in cases {
            let left = left.unwrap();
            let right = right.unwrap();
            assert_eq!(
                expected,
                left.partial_cmp(&right).unwrap(),
                "cmp({:?}, {:?})",
                left,
                right
            );
        }
    }

    /// The integer-vs-double paths keep TiDB's precision-loss tolerance
    /// (`compareFloat64PrecisionLoss` with `floatEpsilon = 1.e-8`), because an
    /// i64/u64 widened to `f64` genuinely loses precision there.
    ///
    /// The cases below are chosen to straddle `1e-8` specifically: a difference
    /// of `1e-9` is inside TiDB's tolerance but roughly seven orders of
    /// magnitude outside `f64::EPSILON` (2.22e-16), so they fail if this ever
    /// drifts back to the stricter constant. Differences far below both
    /// constants would pass either way and would pin nothing.
    #[test]
    fn test_cmp_json_int_vs_double_keeps_precision_loss() {
        // The property that makes the cases below meaningful.
        assert!(1e-9 < FLOAT_EPSILON);
        assert!(1e-9 > f64::EPSILON);

        let cases = vec![
            // Inside TiDB's tolerance, outside `f64::EPSILON`: Equal, in every
            // orientation and for both integer types.
            (
                Json::from_f64(3.000000001),
                Json::from_i64(3),
                Ordering::Equal,
            ),
            (
                Json::from_i64(3),
                Json::from_f64(3.000000001),
                Ordering::Equal,
            ),
            (
                Json::from_f64(2.999999999),
                Json::from_i64(3),
                Ordering::Equal,
            ),
            (
                Json::from_f64(3.000000001),
                Json::from_u64(3),
                Ordering::Equal,
            ),
            (
                Json::from_u64(3),
                Json::from_f64(3.000000001),
                Ordering::Equal,
            ),
            // Just outside TiDB's tolerance (2e-8): ordered, not Equal. This
            // pins the constant from the other side, so widening it further
            // would fail here.
            (
                Json::from_f64(3.00000002),
                Json::from_i64(3),
                Ordering::Greater,
            ),
            (
                Json::from_i64(3),
                Json::from_f64(3.00000002),
                Ordering::Less,
            ),
            (
                Json::from_f64(3.00000002),
                Json::from_u64(3),
                Ordering::Greater,
            ),
            // The precision loss the tolerance exists for: 2^53 + 1 is not
            // representable as an `f64`, so widening it yields 2^53 exactly.
            (
                Json::from_u64(9007199254740993),
                Json::from_f64(9007199254740992.0),
                Ordering::Equal,
            ),
            (
                Json::from_i64(9007199254740993),
                Json::from_f64(9007199254740992.0),
                Ordering::Equal,
            ),
        ];

        for (left, right, expected) in cases {
            let left = left.unwrap();
            let right = right.unwrap();
            assert_eq!(
                expected,
                left.partial_cmp(&right).unwrap(),
                "cmp({:?}, {:?})",
                left,
                right
            );
        }
    }

    /// `compare_f64_precision_loss` reproduces TiDB's two one-sided
    /// comparisons rather than `(x - y).abs() < FLOAT_EPSILON`. The forms agree
    /// for every non-NaN input but differ on NaN, where the `abs` form would
    /// yield `None` and make `Ord for Json` panic on its `unwrap`.
    #[test]
    fn test_cmp_json_precision_loss_matches_tidb_shape() {
        for &(x, y) in &[
            (0.0f64, 0.0f64),
            (1e-9, 0.0),
            (0.0, 1e-9),
            (3.00000002, 3.0),
            (3.0, 3.00000002),
            (-1e300, 1e300),
            (f64::INFINITY, 1.0),
        ] {
            let abs_form = if (x - y).abs() < FLOAT_EPSILON {
                Some(Ordering::Equal)
            } else {
                x.partial_cmp(&y)
            };
            assert_eq!(abs_form, compare_f64_precision_loss(x, y), "({x}, {y})");
        }

        // NaN is where the two shapes part ways. TiDB returns 1 (Greater)
        // because both `x-y < floatEpsilon` and `x-y < 0` are false for NaN.
        assert_eq!(
            Some(Ordering::Greater),
            compare_f64_precision_loss(f64::NAN, 1.0)
        );
        assert_eq!(
            Some(Ordering::Greater),
            compare_f64_precision_loss(1.0, f64::NAN)
        );
    }

    #[test]
    fn test_cmp_json_between_same_type() {
        let test_cases = vec![
            ("false", "true"),
            ("-3", "3"),
            ("3", "5"),
            ("3.0", "4.9"),
            (r#""hello""#, r#""hello, world""#),
            (r#"["a", "b"]"#, r#"["a", "c"]"#),
            (r#"{"a": "b"}"#, r#"{"a": "c"}"#),
        ];
        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
            assert_eq!(left, left.clone());
        }
        assert_eq!(Json::none().unwrap(), Json::none().unwrap());
    }

    #[test]
    fn test_cmp_json_between_diff_type() {
        let test_cases = vec![
            ("1.5", "2"),
            ("1.5", "false"),
            ("1.5", "true"),
            ("2", "true"),
            ("null", r#"{"a": "b"}"#),
            ("2", r#""hello, world""#),
            (r#""hello, world""#, r#"{"a": "b"}"#),
            (r#"{"a": "b"}"#, r#"["a", "b"]"#),
            (r#"["a", "b"]"#, "false"),
        ];

        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
        }
    }

    #[test]
    fn test_cmp_json_between_json_type() {
        let mut ctx = EvalContext::default();

        let cmp = [
            (
                Json::from_time(
                    Time::parse(
                        &mut ctx,
                        "1998-06-13 12:13:14",
                        TimeType::DateTime,
                        0,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap(),
                Json::from_time(
                    Time::parse(
                        &mut ctx,
                        "1998-06-14 13:14:15",
                        TimeType::DateTime,
                        0,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap(),
                Ordering::Less,
            ),
            (
                Json::from_time(
                    Time::parse(
                        &mut ctx,
                        "1998-06-13 12:13:14",
                        TimeType::DateTime,
                        0,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap(),
                Json::from_time(
                    Time::parse(
                        &mut ctx,
                        "1998-06-12 13:14:15",
                        TimeType::DateTime,
                        0,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap(),
                Ordering::Greater,
            ),
            (
                // DateTime is always greater than Date
                Json::from_time(
                    Time::parse(
                        &mut ctx,
                        "1998-06-13 12:13:14",
                        TimeType::DateTime,
                        0,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap(),
                Json::from_time(
                    Time::parse(&mut ctx, "1998-06-14", TimeType::Date, 0, false).unwrap(),
                )
                .unwrap(),
                Ordering::Greater,
            ),
            (
                Json::from_duration(Duration::parse(&mut ctx, "12:13:14", 0).unwrap()).unwrap(),
                Json::from_duration(Duration::parse(&mut ctx, "12:13:16", 0).unwrap()).unwrap(),
                Ordering::Less,
            ),
            (
                Json::from_duration(Duration::parse(&mut ctx, "12:13:16", 0).unwrap()).unwrap(),
                Json::from_duration(Duration::parse(&mut ctx, "12:13:14", 0).unwrap()).unwrap(),
                Ordering::Greater,
            ),
            (
                // Time is always greater than Date
                Json::from_duration(Duration::parse(&mut ctx, "12:13:16", 0).unwrap()).unwrap(),
                Json::from_time(
                    Time::parse(&mut ctx, "1998-06-12", TimeType::Date, 0, false).unwrap(),
                )
                .unwrap(),
                Ordering::Greater,
            ),
            (
                // Time is always less than DateTime
                Json::from_duration(Duration::parse(&mut ctx, "12:13:16", 0).unwrap()).unwrap(),
                Json::from_time(
                    Time::parse(
                        &mut ctx,
                        "1998-06-12 11:11:11",
                        TimeType::DateTime,
                        0,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap(),
                Ordering::Less,
            ),
        ];

        for (l, r, result) in cmp {
            assert_eq!(l.cmp(&r), result)
        }
    }
}
