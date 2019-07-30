// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

macro_rules! match_template_evaluable {
    ($t:tt, $($tail:tt)*) => {
        match_template! {
            $t = [Int, Real, Decimal, Bytes, DateTime, Duration, Json],
            $($tail)*
        }
    };
}

macro_rules! unknown_err {
    ($($arg:tt)*) => { crate::error::EvaluateError::Unknown(format!($($arg)*)) }
}
