// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_export]
macro_rules! other_err {
    ($msg:tt) => ({
        tidb_query_common::error::Error::from(tidb_query_common::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $msg), file!(), line!())
        ))
    });
    ($f:tt, $($arg:expr),+) => ({
        tidb_query_common::error::Error::from(tidb_query_common::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $f), file!(), line!(), $($arg),+)
        ))
    });
}
