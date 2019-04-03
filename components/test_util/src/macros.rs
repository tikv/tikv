// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_export]
macro_rules! retry {
    ($expr:expr) => {
        retry!($expr, 10)
    };
    ($expr:expr, $count:expr) => {
        retry!($expr, $count, 100)
    };
    ($expr:expr, $count:expr, $interval:expr) => {{
        use std::thread;
        let mut res = $expr;
        if !res.is_ok() {
            for _ in 0..$count {
                thread::sleep(Duration::from_millis($interval));
                res = $expr;
                if res.is_ok() {
                    break;
                }
            }
        }
        res
    }};
}
