// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides wrappers for types that comes from 3rd-party and does not implement slog::Value.

#[macro_use]
extern crate slog;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod test_util;

/// Wraps any `Display` type, use `Display` as `slog::Value`.
///
/// Usually this wrapper is useful in containers, e.g. `Option<DisplayValue<T>>`.
///
/// If your type `val: T` is directly used as a field value, you may use `"key" => %value` syntax
/// instead.
pub struct DisplayValue<T: std::fmt::Display>(pub T);

impl<T: std::fmt::Display> slog::Value for DisplayValue<T> {
    #[inline]
    fn serialize(
        &self,
        _record: &::slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self.0))
    }
}

/// Wraps any `Debug` type, use `Debug` as `slog::Value`.
///
/// Usually this wrapper is useful in containers, e.g. `Option<DebugValue<T>>`.
///
/// If your type `val: T` is directly used as a field value, you may use `"key" => ?value` syntax
/// instead.
pub struct DebugValue<T: std::fmt::Debug>(pub T);

impl<T: std::fmt::Debug> slog::Value for DebugValue<T> {
    #[inline]
    fn serialize(
        &self,
        _record: &::slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{:?}", self.0))
    }
}

#[cfg(test)]
#[test]
fn test_debug() {
    let buffer = crate::test_util::SyncLoggerBuffer::new();
    let logger = buffer.build_logger();

    slog_info!(logger, "foo"; "bar" => DebugValue(&::std::time::Duration::from_millis(2500)));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: 2.5s\n");

    buffer.clear();
    slog_info!(logger, "foo"; "bar" => DebugValue(::std::time::Duration::from_millis(23)));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: 23ms\n");

    buffer.clear();
    slog_info!(logger, "foo"; "bar" => DebugValue(&::std::time::Duration::from_secs(1000)));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: 1000s\n");

    buffer.clear();
    slog_info!(logger, "foo"; "bar" => Some(DebugValue(&::std::time::Duration::from_secs(1))));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: 1s\n");

    buffer.clear();
    let v: Option<DebugValue<::std::time::Duration>> = None;
    slog_info!(logger, "foo"; "bar" => v);
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: None\n");
}

pub struct Key<'a>(pub &'a [u8]);

impl<'a> slog::Value for Key<'a> {
    #[inline]
    fn serialize(
        &self,
        _record: &::slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", hex::encode_upper(self.0)))
    }
}

#[cfg(test)]
#[test]
fn test_log_key() {
    let buffer = crate::test_util::SyncLoggerBuffer::new();
    let logger = buffer.build_logger();
    slog_info!(logger, "foo"; "bar" => Key(b"\xAB \xCD"));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: AB20CD\n");
}
