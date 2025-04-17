// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides wrappers for types that comes from 3rd-party and does not implement
//! slog::Value.

#[macro_use]
extern crate slog;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod hex;
use std::{fmt, str::FromStr, sync::atomic::Ordering};

use atomic::Atomic;
use online_config::ConfigValue;
use protobuf::atomic_flags::{
    DEFAULT_REDACT_MARKER_HEAD, DEFAULT_REDACT_MARKER_TAIL, RedactLevel,
    set_redact_level as proto_set_redact_level,
};
use serde::{Deserialize, Serialize, Serializer, de};

pub use crate::hex::*;

pub mod test_util;

/// Wraps any `Display` type, use `Display` as `slog::Value`.
///
/// Usually this wrapper is useful in containers, e.g.
/// `Option<DisplayValue<T>>`.
///
/// If your type `val: T` is directly used as a field value, you may use `"key"
/// => %value` syntax instead.
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
/// If your type `val: T` is directly used as a field value, you may use `"key"
/// => ?value` syntax instead.
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

/// RedactOption is exposed to user to manually control the redaction of log
/// data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedactOption {
    Off,
    On,
    Marker,
}

impl Default for RedactOption {
    fn default() -> Self {
        Self::Off
    }
}

impl fmt::Display for RedactOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::On => "on",
            Self::Off => "off",
            Self::Marker => "marker",
        }
        .fmt(f)
    }
}

impl FromStr for RedactOption {
    type Err = String;
    fn from_str(s: &str) -> Result<RedactOption, String> {
        match s {
            "" => Ok(RedactOption::default()),
            "on" | "ON" | "true" | "TRUE" | "True" => Ok(RedactOption::On),
            "off" | "OFF" | "false" | "FALSE" | "False" => Ok(RedactOption::Off),
            "marker" | "MARKER" => Ok(RedactOption::Marker),
            s => Err(format!(
                "expect: marker | on | off | true | false, got: {s:?}"
            )),
        }
    }
}

impl TryFrom<ConfigValue> for RedactOption {
    type Error = String;
    fn try_from(value: ConfigValue) -> Result<Self, Self::Error> {
        match value {
            ConfigValue::Bool(true) => Ok(RedactOption::On),
            ConfigValue::Bool(false) => Ok(RedactOption::Off),
            ConfigValue::String(s) => RedactOption::from_str(&s),
            _ => Err(format!(
                "expect: marker | on | off | true | false, got: {value:?}"
            )),
        }
    }
}

impl From<RedactOption> for ConfigValue {
    fn from(option: RedactOption) -> Self {
        ConfigValue::String(option.to_string())
    }
}

impl Serialize for RedactOption {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::On => true.serialize(serializer),
            Self::Off => false.serialize(serializer),
            Self::Marker => "marker".serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RedactOption {
    fn deserialize<D>(deseralizer: D) -> Result<RedactOption, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct RedactOptionVisitor;

        impl de::Visitor<'_> for RedactOptionVisitor {
            type Value = RedactOption;

            fn expecting(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt.write_str("string or bool")
            }

            fn visit_str<E>(self, value: &str) -> Result<RedactOption, E>
            where
                E: de::Error,
            {
                FromStr::from_str(value).map_err(E::custom)
            }

            fn visit_bool<E>(self, flag: bool) -> Result<RedactOption, E>
            where
                E: de::Error,
            {
                Ok(if flag {
                    RedactOption::On
                } else {
                    RedactOption::Off
                })
            }
        }

        deseralizer.deserialize_any(RedactOptionVisitor)
    }
}

impl RedactOption {
    fn convert(&self) -> RedactLevel {
        match self {
            Self::On => RedactLevel::On,
            Self::Off => RedactLevel::Off,
            Self::Marker => RedactLevel::Marker,
        }
    }
}

// Log user data to info log only when this flag is set to false.
static REDACT_INFO_LOG: Atomic<RedactLevel> = Atomic::new(RedactLevel::Off);

/// Set whether we should avoid user data to slog.
pub fn set_redact_info_log(config: RedactOption) {
    let level = config.convert();
    REDACT_INFO_LOG.store(level, Ordering::Relaxed);
    // Also set the redact level in protobuf.
    proto_set_redact_level(level);
}

#[derive(Clone, Copy)]
pub struct Value<'a>(pub &'a [u8]);

impl<'a> Value<'a> {
    pub fn key(key: &'a [u8]) -> Self {
        Value(key)
    }

    pub fn value(v: &'a [u8]) -> Self {
        Value(v)
    }
}

impl slog::Value for Value<'_> {
    #[inline]
    fn serialize(
        &self,
        _record: &::slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match REDACT_INFO_LOG.load(Ordering::Relaxed) {
            RedactLevel::Marker => serializer.emit_arguments(
                key,
                &format_args!(
                    "{}{}{}",
                    DEFAULT_REDACT_MARKER_HEAD,
                    crate::hex_encode_upper(self.0),
                    DEFAULT_REDACT_MARKER_TAIL,
                ),
            ),
            RedactLevel::On => serializer.emit_arguments(key, &format_args!("?")),
            _ => {
                serializer.emit_arguments(key, &format_args!("{}", crate::hex_encode_upper(self.0)))
            }
        }
    }
}

impl fmt::Display for Value<'_> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match REDACT_INFO_LOG.load(Ordering::Relaxed) {
            RedactLevel::Marker => {
                write!(
                    f,
                    "{}{}{}",
                    DEFAULT_REDACT_MARKER_HEAD,
                    crate::hex_encode_upper(self.0),
                    DEFAULT_REDACT_MARKER_TAIL
                )
            }
            RedactLevel::On => {
                // Print placeholder instead of the value itself.
                write!(f, "?")
            }
            _ => {
                write!(f, "{}", crate::hex_encode_upper(self.0))
            }
        }
    }
}

impl fmt::Debug for Value<'_> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_log_key() {
        let buffer = crate::test_util::SyncLoggerBuffer::new();
        let logger = buffer.build_logger();
        slog_info!(logger, "foo"; "bar" => Value::key(b"\xAB \xCD"));
        assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: AB20CD\n");
    }

    #[test]
    fn test_redact_options() {
        #[derive(Default, Serialize, Deserialize)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        struct TestRedactInfo {
            redact_info_log: RedactOption,
        }

        assert_eq!(RedactOption::from_str("").unwrap(), RedactOption::Off);
        assert_eq!(RedactOption::from_str("on").unwrap(), RedactOption::On);
        assert_eq!(RedactOption::from_str("off").unwrap(), RedactOption::Off);
        assert_eq!(
            RedactOption::from_str("marker").unwrap(),
            RedactOption::Marker
        );
        assert_eq!(
            RedactOption::from_str("MARKER").unwrap(),
            RedactOption::Marker
        );
        RedactOption::from_str("Marker").unwrap_err();

        let mut template = r#""#;
        let mut test_config: TestRedactInfo = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::default());
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::Off);

        template = r#"
            redact-info-log = true
        "#;
        test_config = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::On);
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::On);

        template = r#"
            redact-info-log = false
        "#;
        test_config = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::Off);
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::Off);

        template = r#"
            redact-info-log = "on"
        "#;
        test_config = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::On);
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::On);

        template = r#"
            redact-info-log = "off"
        "#;
        test_config = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::Off);
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::Off);

        template = r#"
            redact-info-log = "marker"
        "#;
        test_config = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::Marker);
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::Marker);

        template = r#"
            redact-info-log = "MARKER"
        "#;
        test_config = toml::from_str(template).unwrap();
        assert_eq!(test_config.redact_info_log, RedactOption::Marker);
        assert_eq!(test_config.redact_info_log.convert(), RedactLevel::Marker);

        template = r#"
            redact-info-log = "Maker"
        "#;
        toml::from_str::<RedactOption>(template).unwrap_err();
    }

    #[test]
    fn test_redact_info_log() {
        let buffer = crate::test_util::SyncLoggerBuffer::new();
        let logger = buffer.build_logger();
        set_redact_info_log(RedactOption::On);
        slog_info!(logger, "foo"; "bar" => Value::key(b"\xAB \xCD"));
        assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: ?\n");

        buffer.clear();
        set_redact_info_log(RedactOption::default());
        slog_info!(logger, "foo"; "bar" => Value::key(b"\xAB \xCD"));
        assert_eq!(&buffer.as_string(), "TIME INFO foo, bar: AB20CD\n");

        buffer.clear();
        set_redact_info_log(RedactOption::Marker);
        slog_info!(logger, "foo"; "bar" => Value::key(b"\xAB \xCD"));
        assert_eq!(
            buffer.as_string(),
            format!(
                "TIME INFO foo, bar: {}AB20CD{}\n",
                DEFAULT_REDACT_MARKER_HEAD, DEFAULT_REDACT_MARKER_TAIL
            )
        );
    }
}
