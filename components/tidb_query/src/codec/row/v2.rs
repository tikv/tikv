// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{i16, i32, i8, u16, u32, u8};

pub const CODEC_VERSION: u8 = 128;
const BIG_ID_FLAG: u8 = 1;

mod row_slice;

#[cfg(test)]
mod encoder;

trait BoundedU64 {
    const MIN_U64: u64;
    const MAX_U64: u64;
}

trait BoundedI64 {
    const MIN_I64: i64;
    const MAX_I64: i64;
}

macro_rules! bounded_u64_impl {
    ($t:ty, $min:expr, $max:expr) => {
        impl BoundedU64 for $t {
            const MIN_U64: u64 = $min as u64;
            const MAX_U64: u64 = $max as u64;
        }
    };
}

macro_rules! bounded_i64_impl {
    ($t:ty, $min:expr, $max:expr) => {
        impl BoundedI64 for $t {
            const MIN_I64: i64 = $min as i64;
            const MAX_I64: i64 = $max as i64;
        }
    };
}

bounded_u64_impl!(u8, u8::MIN, u8::MAX);
bounded_u64_impl!(u16, u16::MIN, u16::MAX);
bounded_u64_impl!(u32, u32::MIN, u32::MAX);

bounded_i64_impl!(i8, i8::MIN, i8::MAX);
bounded_i64_impl!(i16, i16::MIN, i16::MAX);
bounded_i64_impl!(i32, i32::MIN, i32::MAX);

#[cfg(test)]
mod tests {
    use super::{BoundedI64, BoundedU64};
    use std::{i16, u8};

    #[test]
    fn test_bound() {
        assert_eq!(u8::MAX_U64, u64::from(u8::MAX));
        assert_eq!(u8::MIN_U64, u64::from(u8::MIN));
        assert_eq!(i16::MAX_I64, i64::from(i16::MAX));
        assert_eq!(i16::MIN_I64, i64::from(i16::MIN));
    }
}
