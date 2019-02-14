// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! Provides wrappers for types that comes from 3rd-party and does not implement slog::Value.

extern crate hex;
extern crate kvproto as lib_kvproto;
#[macro_use]
extern crate slog;
extern crate slog_term;

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
        _record: &::slog::Record,
        key: slog::Key,
        serializer: &mut slog::Serializer,
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
        _record: &::slog::Record,
        key: slog::Key,
        serializer: &mut slog::Serializer,
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
        _record: &::slog::Record,
        key: slog::Key,
        serializer: &mut slog::Serializer,
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

pub mod kvproto {
    pub mod kvrpcpb {
        pub struct KeyRange<'a>(pub &'a crate::lib_kvproto::kvrpcpb::KeyRange);

        impl<'a> slog::Value for KeyRange<'a> {
            #[inline]
            fn serialize(
                &self,
                _record: &::slog::Record,
                key: slog::Key,
                serializer: &mut slog::Serializer,
            ) -> slog::Result {
                serializer.emit_arguments(
                    key,
                    &format_args!(
                        "[{}, {})",
                        hex::encode_upper(self.0.get_start_key()),
                        hex::encode_upper(self.0.get_end_key())
                    ),
                )
            }
        }

        #[cfg(test)]
        #[test]
        fn test_key_range() {
            let buffer = crate::test_util::SyncLoggerBuffer::new();
            let logger = buffer.build_logger();

            let mut range = crate::lib_kvproto::kvrpcpb::KeyRange::new();
            range.set_start_key(b"\x20\x00".to_vec());
            range.set_end_key(b"\x31\xFF\x12a".to_vec());
            slog_info!(logger, "foo"; "bar" => KeyRange(&range));
            assert_eq!(
                &buffer.as_string(),
                "TIME INFO foo, bar: [2000, 31FF1261)\n"
            );

            buffer.clear();
            let mut range = crate::lib_kvproto::kvrpcpb::KeyRange::new();
            range.set_end_key(b"\x31".to_vec());
            slog_info!(logger, "foo"; "bar" => KeyRange(&range));
            assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: [, 31)\n");

            buffer.clear();
            let mut range = crate::lib_kvproto::kvrpcpb::KeyRange::new();
            range.set_start_key(b"\xC0".to_vec());
            slog_info!(logger, "foo"; "bar" => KeyRange(&range));
            assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: [C0, )\n");
        }
    }

    pub mod coprocessor {
        pub struct KeyRange<'a>(pub &'a crate::lib_kvproto::coprocessor::KeyRange);

        // Similar to `kvrpcpb::KeyRange`. Tests are ignored.
        impl<'a> slog::Value for KeyRange<'a> {
            #[inline]
            fn serialize(
                &self,
                _record: &::slog::Record,
                key: slog::Key,
                serializer: &mut slog::Serializer,
            ) -> slog::Result {
                serializer.emit_arguments(
                    key,
                    &format_args!(
                        "[{}, {})",
                        hex::encode_upper(self.0.get_start()),
                        hex::encode_upper(self.0.get_end())
                    ),
                )
            }
        }
    }
}
