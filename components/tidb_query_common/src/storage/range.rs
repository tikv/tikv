// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::KeyRange;

// TODO: Remove this module after switching to DAG v2.

#[derive(PartialEq, Eq, Clone)]
pub enum Range {
    Point(PointRange),
    Interval(IntervalRange),
}

impl Range {
    pub fn from_pb_range(mut range: KeyRange, accept_point_range: bool) -> Self {
        if accept_point_range && crate::util::is_point(&range) {
            Range::Point(PointRange(range.take_start()))
        } else {
            Range::Interval(IntervalRange::from((range.take_start(), range.take_end())))
        }
    }
}

impl std::fmt::Debug for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Range::Point(r) => std::fmt::Debug::fmt(r, f),
            Range::Interval(r) => std::fmt::Debug::fmt(r, f),
        }
    }
}

impl From<IntervalRange> for Range {
    fn from(r: IntervalRange) -> Self {
        Range::Interval(r)
    }
}

impl From<PointRange> for Range {
    fn from(r: PointRange) -> Self {
        Range::Point(r)
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct IntervalRange {
    pub lower_inclusive: Vec<u8>,
    pub upper_exclusive: Vec<u8>,
}

impl std::fmt::Debug for IntervalRange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        write!(f, "{}", hex::encode_upper(self.lower_inclusive.as_slice()))?;
        write!(f, ", ")?;
        write!(f, "{}", hex::encode_upper(self.upper_exclusive.as_slice()))?;
        write!(f, ")")
    }
}

impl From<(Vec<u8>, Vec<u8>)> for IntervalRange {
    fn from((lower, upper): (Vec<u8>, Vec<u8>)) -> Self {
        IntervalRange {
            lower_inclusive: lower,
            upper_exclusive: upper,
        }
    }
}

impl From<(String, String)> for IntervalRange {
    fn from((lower, upper): (String, String)) -> Self {
        IntervalRange::from((lower.into_bytes(), upper.into_bytes()))
    }
}

// FIXME: Maybe abuse.
impl<'a, 'b> From<(&'a str, &'b str)> for IntervalRange {
    fn from((lower, upper): (&'a str, &'b str)) -> Self {
        IntervalRange::from((lower.to_owned(), upper.to_owned()))
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct PointRange(pub Vec<u8>);

impl std::fmt::Debug for PointRange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode_upper(self.0.as_slice()))
    }
}

impl From<Vec<u8>> for PointRange {
    fn from(v: Vec<u8>) -> Self {
        PointRange(v)
    }
}

impl From<String> for PointRange {
    fn from(v: String) -> Self {
        PointRange::from(v.into_bytes())
    }
}

// FIXME: Maybe abuse.
impl<'a> From<&'a str> for PointRange {
    fn from(v: &'a str) -> Self {
        PointRange::from(v.to_owned())
    }
}
