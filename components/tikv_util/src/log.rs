/// Logs a critical level message using the slog global logger.
#[macro_export]
macro_rules! crit( ($($args:tt)+) => {
    ::slog_global::crit!($($args)+)
};);
/// Logs a error level message using the slog global logger.
#[macro_export]
macro_rules! error {
    (?$e:expr; $l:literal) => {
        ::slog_global::error!($l; "err"=>?$e,"err_code"=>%$e.error_code())
    };

    (%$e:expr; $l:literal) => {
        ::slog_global::error!($l; "err"=>?$e,"err_code"=>%$e.error_code())
    };

    (?$e:expr; $($args:tt)+) => {
        ::slog_global::error!($($args)+ "err"=>?$e,"err_code"=>%$e.error_code())
    };

    (%$e:expr; $($args:tt)+) => {
        ::slog_global::error!($($args)+ "err" => %$e,"err_code" => %$e.error_code())
    };

    ($($args:tt)+) => {
        ::slog_global::error!($($args)+)
    };
}

/// Logs a warning level message using the slog global logger.
#[macro_export]
macro_rules! warn( ($($args:tt)+) => {
    ::slog_global::warn!($($args)+)
};);
/// Logs a info level message using the slog global logger.
#[macro_export]
macro_rules! info( ($($args:tt)+) => {
    ::slog_global::info!($($args)+)
};);
/// Logs a debug level message using the slog global logger.
#[macro_export]
macro_rules! debug( ($($args:tt)+) => {
    ::slog_global::debug!($($args)+)
};);
/// Logs a trace level message using the slog global logger.
#[macro_export]
macro_rules! trace( ($($args:tt)+) => {
    ::slog_global::trace!($($args)+)
};);
