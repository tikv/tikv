// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides wrappers for types that comes from 3rd-party and does not implement slog::Value.

#[macro_use]
extern crate slog;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod hex;
use std::{
    fmt,
    sync::atomic::{AtomicBool, Ordering},
};

use protobuf::atomic_flags::set_redact_bytes as proto_set_redact_bytes;

pub use crate::hex::*;

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

// Log user data to info log only when this flag is set to false.
static REDACT_INFO_LOG: AtomicBool = AtomicBool::new(false);

/// Set whether we should avoid user data to slog.
pub fn set_redact_info_log(v: bool) {
    REDACT_INFO_LOG.store(v, Ordering::Relaxed);
    proto_set_redact_bytes(v);
}

pub struct Value<'a>(pub &'a [u8]);

impl<'a> Value<'a> {
    pub fn key(key: &'a [u8]) -> Self {
        Value(key)
    }

    pub fn value(v: &'a [u8]) -> Self {
        Value(v)
    }
}

impl<'a> slog::Value for Value<'a> {
    #[inline]
    fn serialize(
        &self,
        _record: &::slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        if REDACT_INFO_LOG.load(Ordering::Relaxed) {
            serializer.emit_arguments(key, &format_args!("?"))
        } else {
            serializer.emit_arguments(key, &format_args!("{}", crate::hex_encode_upper(self.0)))
        }
    }
}

impl<'a> fmt::Display for Value<'a> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::Relaxed) {
            // Print placeholder instead of the value itself.
            write!(f, "?")
        } else {
            write!(f, "{}", crate::hex_encode_upper(self.0))
        }
    }
}

impl<'a> fmt::Debug for Value<'a> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
#[test]
fn test_log_key() {
    let buffer = crate::test_util::SyncLoggerBuffer::new();
    let logger = buffer.build_logger();
    slog_info!(logger, "foo"; "bar" => Value::key(b"\xAB \xCD"));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: AB20CD\n");
}

#[cfg(test)]
#[test]
#[ignore]
fn test_redact_info_log() {
    let buffer = crate::test_util::SyncLoggerBuffer::new();
    let logger = buffer.build_logger();
    set_redact_info_log(true);
    slog_info!(logger, "foo"; "bar" => Value::key(b"\xAB \xCD"));
    assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: ?\n");
    set_redact_info_log(false);
}
