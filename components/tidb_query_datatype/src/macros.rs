// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_export]
macro_rules! other_err {
    ($msg:tt) => ({
        tidb_query_datatype::Error::from(tidb_query_datatype::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $msg), file!(), line!())
        ))
    });
    ($f:tt, $($arg:expr),+) => ({
        tidb_query_datatype::Error::from(tidb_query_datatype::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $f), file!(), line!(), $($arg),+)
        ))
    });
}

macro_rules! _other_err {
    ($msg:tt) => ({
        crate::Error::from(crate::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $msg), file!(), line!())
        ))
    });
    ($f:tt, $($arg:expr),+) => ({
        crate::Error::from(crate::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $f), file!(), line!(), $($arg),+)
        ))
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_other_err() {
        use crate::error::{ErrorInner, EvaluateError};

        let e = _other_err!("foo");
        match *e.0 {
            ErrorInner::Evaluate(EvaluateError::Other(s)) => assert!(s.ends_with("foo")),
            _ => panic!(),
        }

        let e = _other_err!("foo {} bar", "abc");
        match *e.0 {
            ErrorInner::Evaluate(EvaluateError::Other(s)) => assert!(s.ends_with("foo abc bar")),
            _ => panic!(),
        }
    }
}
