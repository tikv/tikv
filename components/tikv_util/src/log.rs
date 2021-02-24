// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

/// Logs a critical level message using the slog global logger.
#[macro_export]
macro_rules! crit( ($($args:tt)+) => {
    ::slog_global::crit!($($args)+)
};);

/// Logs a error level message using the slog global logger. /// Use '?' to output error in debug format or '%' to ouput error in display format.
/// As the third and forth rules shown, the last log field should follow a ',' to seperate the 'err' field. eg. `error!(?e, "msg"; "foo" => foo,);`
/// If you don't want to output error code, just use the common form like other macros.
/// Require `slog_global` dependency and `#![feature(min_speacilization)]` in all crates.
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
