// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

/// Logs a critical level message using the slog global logger.
#[macro_export]
macro_rules! crit( ($($args:tt)+) => {
    ::slog_global::crit!($($args)+)
};);

/// Logs a error level message using the slog global logger. /// Use '?' to
/// output error in debug format or '%' to output error in display format. As
/// the third and forth rules shown, the last log field should follow a ',' to
/// separate the 'err' field. eg. `error!(?e, "msg"; "foo" => foo,);`
/// If you don't want to output error code, just use the common form like other
/// macros. Require `slog_global` dependency and
/// `#![feature(min_speacilization)]` in all crates.
#[macro_export]
macro_rules! error {
    (?$e:expr; $l:literal) => {
        ::slog_global::error!($l; "err" => ?$e,"err_code" => %error_code::ErrorCodeExt::error_code(&$e))
    };

    (%$e:expr; $l:literal) => {
        ::slog_global::error!($l; "err" => ?$e,"err_code" => %error_code::ErrorCodeExt::error_code(&$e))
    };

    (?$e:expr; $($args:tt)+) => {
        ::slog_global::error!($($args)+ "err" => ?$e,"err_code" => %error_code::ErrorCodeExt::error_code(&$e))
    };

    (%$e:expr; $($args:tt)+) => {
        ::slog_global::error!($($args)+ "err" => %$e,"err_code" => %error_code::ErrorCodeExt::error_code(&$e))
    };

    ($($args:tt)+) => {
        ::slog_global::error!($($args)+)
    };
}

// error_unknown is used the same as the above error macro
// However, it will always use an error code of UNKNOWN
// This is for errors that do not implement ErrorCodeExt
// It is recommended to implement ErrorCodeExt instead of using this macro
#[macro_export]
macro_rules! error_unknown {
    (?$e:expr; $l:literal) => {
        ::slog_global::error!($l; "err" => ?$e,"err_code" => %error_code::UNKNOWN)
    };

    (%$e:expr; $l:literal) => {
        ::slog_global::error!($l; "err" => ?$e,"err_code" => %error_code::UNKNOWN)
    };

    (?$e:expr; $($args:tt)+) => {
        ::slog_global::error!($($args)+ "err" => ?$e,"err_code" => %error_code::UNKNOWN)
    };

    (%$e:expr; $($args:tt)+) => {
        ::slog_global::error!($($args)+ "err" => %$e,"err_code" => %error_code::UNKNOWN)
    };
}

/// Logs a warning level message using the slog global logger.
#[macro_export]
macro_rules! warn(($($args:tt)+) => {
    ::slog_global::warn!($($args)+)
};);

/// Logs a info level message using the slog global logger.
#[macro_export]
macro_rules! info(($($args:tt)+) => {
    ::slog_global::info!($($args)+)
};);

/// Logs a debug level message using the slog global logger.
#[macro_export]
macro_rules! debug(($($args:tt)+) => {
    ::slog_global::debug!($($args)+)
};);

/// Logs a trace level message using the slog global logger.
#[macro_export]
macro_rules! trace(($($args:tt)+) => {
    ::slog_global::trace!($($args)+)
};);

use std::fmt::{self, Display, Write};

use slog::{BorrowedKV, OwnedKVList, Record, KV};

struct FormatKeyValueList<'a, W> {
    buffer: &'a mut W,
    written: bool,
}

impl<'a, W: Write> slog::Serializer for FormatKeyValueList<'a, W> {
    fn emit_arguments(&mut self, key: slog::Key, val: &fmt::Arguments<'_>) -> slog::Result {
        if !self.written {
            write!(&mut self.buffer, "[{}={}]", key, val).unwrap();
            self.written = true;
        } else {
            write!(&mut self.buffer, " [{}={}]", key, val).unwrap()
        }
        Ok(())
    }
}

/// A helper struct to format the key-value list of a slog logger. It's not
/// exact the same format as `TiKVFormat` and etc. It's just a simple
/// implementation for panic, return errors that doesn't show in normal logs
/// processing.
pub struct SlogFormat<'a>(pub &'a slog::Logger);

impl<'a> Display for SlogFormat<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = FormatKeyValueList {
            buffer: f,
            written: false,
        };
        let record = slog::record_static!(slog::Level::Trace, "");
        self.0
            .list()
            .serialize(
                &Record::new(&record, &format_args!(""), slog::b!()),
                &mut formatter,
            )
            .unwrap();
        Ok(())
    }
}

#[doc(hidden)]
pub fn format_kv_list(buffer: &mut String, kv_list: &OwnedKVList, borrow_list: BorrowedKV<'_>) {
    let mut formatter = FormatKeyValueList {
        buffer,
        written: false,
    };
    let record = slog::record_static!(slog::Level::Trace, "");
    let args = format_args!("");
    let record = Record::new(&record, &args, slog::b!());
    // Serialize borrow list first to make region_id, peer_id at the end.
    borrow_list.serialize(&record, &mut formatter).unwrap();
    kv_list.serialize(&record, &mut formatter).unwrap();
}

/// A helper macro to panic with the key-value list of a slog logger.
///
/// Similar to `SlogFormat`, but just panic.
#[macro_export]
macro_rules! slog_panic {
    ($logger:expr, $msg:expr, $borrowed_kv:expr) => {{
        let owned_kv = ($logger).list();
        let mut s = String::new();
        $crate::log::format_kv_list(&mut s, &owned_kv, $borrowed_kv);
        if s.is_empty() {
            panic!("{}", $msg)
        } else {
            panic!("{} {}", $msg, s)
        }
    }};
    ($logger:expr, $msg:expr) => {{
        $crate::slog_panic!($logger, $msg, slog::b!())
    }};
    ($logger:expr, $msg:expr; $($arg:tt)+) => {{
        $crate::slog_panic!($logger, $msg, slog::b!($($arg)+))
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_format_kv() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let s = format!("{}", super::SlogFormat(&logger));
        assert_eq!(s, String::new());

        let logger = logger.new(slog::o!("a" => 1));
        let s = format!("{}", super::SlogFormat(&logger));
        assert_eq!(s, "[a=1]");

        let logger = logger.new(slog::o!("b" => 2));
        let s = format!("{}", super::SlogFormat(&logger));
        assert_eq!(s, "[b=2] [a=1]");
    }

    #[test]
    fn test_slog_panic() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let err = panic_hook::recover_safe(|| {
            crate::slog_panic!(logger, "test");
        })
        .unwrap_err();
        assert_eq!(err.downcast::<String>().unwrap().as_str(), "test");

        let err = panic_hook::recover_safe(|| {
            crate::slog_panic!(logger, "test"; "k" => "v");
        })
        .unwrap_err();
        assert_eq!(err.downcast::<String>().unwrap().as_str(), "test [k=v]");

        let logger = logger.new(slog::o!("a" => 1));
        let err = panic_hook::recover_safe(|| {
            crate::slog_panic!(logger, "test");
        })
        .unwrap_err();
        assert_eq!(err.downcast::<String>().unwrap().as_str(), "test [a=1]");

        let logger = logger.new(slog::o!("b" => 2));
        let err = panic_hook::recover_safe(|| {
            crate::slog_panic!(logger, "test");
        })
        .unwrap_err();
        assert_eq!(
            err.downcast::<String>().unwrap().as_str(),
            "test [b=2] [a=1]"
        );

        let err = panic_hook::recover_safe(|| {
            crate::slog_panic!(logger, "test"; "k" => "v");
        })
        .unwrap_err();
        assert_eq!(
            err.downcast::<String>().unwrap().as_str(),
            "test [k=v] [b=2] [a=1]"
        );
    }
}
