// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::REDACT_INFO_LOG;

use protobuf::PbPrint;
use std::{fmt, sync::atomic::Ordering};

pub struct ProtobufValue<'a, T>(pub &'a T);

impl<'a, T> slog::Value for ProtobufValue<'a, T>
where
    ProtobufValue<'a, T>: fmt::Debug,
{
    #[inline]
    fn serialize(
        &self,
        _record: &::slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{:?}", self))
    }
}

impl<'a, T> PbPrint for ProtobufValue<'a, T>
where
    ProtobufValue<'a, T>: fmt::Debug,
{
    #[inline]
    fn fmt(&self, name: &str, buf: &mut String) {
        protobuf::push_message_start(name, buf);
        buf.push_str(&format!(" {:?} }}", self));
    }
}

// TODO: replace Debug with user-data-suppressed outputs
impl<'a, T> fmt::Debug for ProtobufValue<'a, T>
where
    T: fmt::Debug,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::Relaxed) {
            write!(f, "redacted: {:?}", self.0)
        } else {
            self.0.fmt(f)
        }
    }
}
