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
        if accept_point_range && crate::coprocessor::util::is_point(&range) {
            Range::Point(PointRange(range.take_start()))
        } else {
            Range::Interval(IntervalRange {
                lower_inclusive: range.take_start(),
                upper_exclusive: range.take_end(),
            })
        }
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct IntervalRange {
    pub lower_inclusive: Vec<u8>,
    pub upper_exclusive: Vec<u8>,
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct PointRange(pub Vec<u8>);

impl std::fmt::Debug for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use hex::ToHex;
        match self {
            Range::Point(PointRange(v)) => v.as_slice().write_hex_upper(f),
            Range::Interval(IntervalRange {
                lower_inclusive,
                upper_exclusive,
            }) => {
                write!(f, "[")?;
                lower_inclusive.as_slice().write_hex_upper(f)?;
                write!(f, ", ")?;
                upper_exclusive.as_slice().write_hex_upper(f)?;
                write!(f, ")")
            }
        }
    }
}
