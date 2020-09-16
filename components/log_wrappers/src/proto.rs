// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::REDACT_INFO_LOG;

use kvproto::{
    import_sstpb::{Range, RewriteRule, SstMeta},
    metapb::Region,
    pdpb::Merge,
    raft_cmdpb::{
        AdminRequest, BatchSplitRequest, CommitMergeRequest, DeleteRangeRequest, DeleteRequest,
        GetRequest, IngestSstRequest, PrepareMergeRequest, PrewriteRequest, PutRequest,
        RaftCmdRequest, Request, SplitRequest,
    },
};

use protobuf::PbPrint;
use std::{fmt, sync::atomic::Ordering};

pub struct ProtobufValue<'a, T: ?Sized>(pub &'a T);

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

#[cfg(feature = "protobuf-codec")]
macro_rules! impl_print {
    (+, $f:ident, $s:expr, $base:expr, $field:ident) => {
        PbPrint::fmt(&$base.$field, std::stringify!($field), $s);
    };
    (-, $f:ident, $s:expr, $base:expr, $field:ident) => {
        $s.push_str(" ");
        $s.push_str(std::stringify!($field));
        $s.push_str(": <");
        $s.push_str(std::stringify!($field));
        $s.push_str(">");
    };
    (*, $f:ident, $s:expr, $base:expr, $field:ident) => {
        paste::paste! {
            let $field = format!("{:?}", ProtobufValue($base.[<get_ $field>]()));
            PbPrint::fmt(&$field, std::stringify!($field), $s);
        }
    };
    (%, $f:ident, $s:expr, $base:expr, $field:ident) => {
        paste::paste! {
            let $field = $base
                .[<get_ $field>]()
                .iter()
                .map(ProtobufValue)
                .collect::<Vec<_>>();
            PbPrint::fmt(&$field, std::stringify!($field), $s);
        }
    };
}

/// `impl_fmt` generates redacted Debug trait implementation
/// for protobuf objects.
///
/// ## Usage
///
/// ```ignore
/// impl_fmt! { ProtobufType, +visible_field, -redacted_field, *protobuf_field, %protobuf_list }
/// ```
#[cfg(feature = "protobuf-codec")]
macro_rules! impl_fmt {
    ($ty:ty, $($oper:tt $field:ident),+) => {
        impl<'a> fmt::Debug for ProtobufValue<'a, $ty> {
            #[inline]
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                if REDACT_INFO_LOG.load(Ordering::Relaxed) {
                    let mut s = String::new();
                    $( impl_print! { $oper, f, &mut s, self.0, $field } )*
                    write!(f, "{}", s)
                } else {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };
}

/// `impl_fmt` generates redacted Debug trait implementation
/// for protobuf objects.
///
/// TODO: For PROST codec, `impl_fmt` will redact all information.
/// This should be fixed.
#[cfg(not(feature = "protobuf-codec"))]
macro_rules! impl_fmt {
    ($ty:ty, $($oper:tt $field:ident),+) => {
        impl<'a> fmt::Debug for ProtobufValue<'a, $ty> {
            #[inline]
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                if REDACT_INFO_LOG.load(Ordering::Relaxed) {
                    write!(f, "<redacted>")
                } else {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };
}

impl_fmt! { Merge, *target }
impl_fmt! { Region, +id, -start_key, -end_key, +region_epoch, +peers }
impl_fmt! { Range, -start, -end }
impl_fmt! { RewriteRule, -old_key_prefix, -new_key_prefix, +new_timestamp }
impl_fmt! { SstMeta, +uuid, *range, +crc32, +length, +cf_name, +region_id, +region_epoch }
impl_fmt! { GetRequest, +cf, -key }
impl_fmt! { PutRequest, +cf, -key, -value }
impl_fmt! { DeleteRequest, +cf, -key }
impl_fmt! { DeleteRangeRequest, +cf, -start_key, -end_key, +notify_only }
impl_fmt! { PrewriteRequest, -key, -value, +lock }
impl_fmt! { IngestSstRequest, *sst }
impl_fmt! { Request, +cmd_type, *get, *put, *delete, +snap, *prewrite, *delete_range, *ingest_sst, +read_index }
impl_fmt! { SplitRequest, -split_key, +new_region_id, +new_peer_ids, +right_derive }
impl_fmt! { BatchSplitRequest, %requests, +right_derive }
impl_fmt! { PrepareMergeRequest, +min_index, *target }
impl_fmt! { CommitMergeRequest, *source, +commit, +entries }
impl_fmt! { AdminRequest, +cmd_type, +change_peer, *split, +compact_log, +transfer_leader, +verify_hash, *prepare_merge, *commit_merge, +rollback_merge, *splits }
impl_fmt! { RaftCmdRequest, %requests, *admin_request, +status_request }
