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

impl<'a> fmt::Debug for ProtobufValue<'a, Region> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            PbPrint::fmt(&self.0.id, "id", &mut s);
            s.push_str(" start_key: <start_key>");
            s.push_str(" end_key: <end_key>");
            PbPrint::fmt(&self.0.region_epoch, "region_epoch", &mut s);
            PbPrint::fmt(&self.0.peers, "peers", &mut s);
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, Merge> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        let target = format!("{:?}", ProtobufValue(self.0.get_target()));
        PbPrint::fmt(&target, "target", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, Range> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            s.push_str(" start: <start>");
            s.push_str(" end: <end>");
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, RewriteRule> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            s.push_str(" old_key_prefix: <old_key_prefix>");
            s.push_str(" new_key_prefix: <new_key_prefix>");
            PbPrint::fmt(&self.0.new_timestamp, "new_timestamp", &mut s);
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, SstMeta> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        PbPrint::fmt(&self.0.uuid, "uuid", &mut s);
        let range = ProtobufValue(self.0.get_range());
        PbPrint::fmt(&range, "range", &mut s);
        PbPrint::fmt(&self.0.crc32, "crc32", &mut s);
        PbPrint::fmt(&self.0.length, "length", &mut s);
        PbPrint::fmt(&self.0.cf_name, "cf_name", &mut s);
        PbPrint::fmt(&self.0.region_id, "region_id", &mut s);
        PbPrint::fmt(&self.0.region_epoch, "region_epoch", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, GetRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            PbPrint::fmt(&self.0.cf, "cf", &mut s);
            s.push_str(" key: <key>");
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, PutRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            PbPrint::fmt(&self.0.cf, "cf", &mut s);
            s.push_str(" key: <key>");
            s.push_str(" value: <value>");
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, DeleteRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            PbPrint::fmt(&self.0.cf, "cf", &mut s);
            s.push_str(" key: <key>");
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, DeleteRangeRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            PbPrint::fmt(&self.0.cf, "cf", &mut s);
            s.push_str(" start_key: <start_key>");
            s.push_str(" end_key: <end_key>");
            PbPrint::fmt(&self.0.notify_only, "notify_only", &mut s);
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, PrewriteRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            s.push_str(" key: <key>");
            s.push_str(" value: <value>");
            PbPrint::fmt(&self.0.lock, "lock", &mut s);
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, IngestSstRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        let sst = ProtobufValue(self.0.get_sst());
        PbPrint::fmt(&sst, "sst", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, Request> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        PbPrint::fmt(&self.0.cmd_type, "cmd_type", &mut s);
        let get = ProtobufValue(self.0.get_get());
        PbPrint::fmt(&get, "get", &mut s);
        let put = ProtobufValue(self.0.get_put());
        PbPrint::fmt(&put, "put", &mut s);
        let delete = ProtobufValue(self.0.get_delete());
        PbPrint::fmt(&delete, "delete", &mut s);
        PbPrint::fmt(&self.0.snap, "snap", &mut s);
        let prewrite = ProtobufValue(self.0.get_prewrite());
        PbPrint::fmt(&prewrite, "prewrite", &mut s);
        let delete_range = ProtobufValue(self.0.get_delete_range());
        PbPrint::fmt(&delete_range, "delete_range", &mut s);
        let ingest_sst = ProtobufValue(self.0.get_ingest_sst());
        PbPrint::fmt(&ingest_sst, "ingest_sst", &mut s);
        PbPrint::fmt(&self.0.read_index, "read_index", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, SplitRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if REDACT_INFO_LOG.load(Ordering::SeqCst) {
            let mut s = String::new();
            s.push_str(" split_key: <split_key>");
            PbPrint::fmt(&self.0.new_region_id, "new_region_id", &mut s);
            PbPrint::fmt(&self.0.new_peer_ids, "new_peer_ids", &mut s);
            PbPrint::fmt(&self.0.right_derive, "right_derive", &mut s);
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, BatchSplitRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        let requests = self
            .0
            .get_requests()
            .iter()
            .map(ProtobufValue)
            .collect::<Vec<_>>();
        PbPrint::fmt(&requests, "requests", &mut s);
        PbPrint::fmt(&self.0.right_derive, "right_derive", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, PrepareMergeRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        PbPrint::fmt(&self.0.min_index, "min_index", &mut s);
        let target = ProtobufValue(self.0.get_target());
        PbPrint::fmt(&target, "target", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, CommitMergeRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        let source = ProtobufValue(self.0.get_source());
        PbPrint::fmt(&source, "source", &mut s);
        PbPrint::fmt(&self.0.commit, "commit", &mut s);
        PbPrint::fmt(&self.0.entries, "entries", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, AdminRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        PbPrint::fmt(&self.0.cmd_type, "cmd_type", &mut s);
        PbPrint::fmt(&self.0.change_peer, "change_peer", &mut s);
        let split = ProtobufValue(self.0.get_split());
        PbPrint::fmt(&split, "split", &mut s);
        PbPrint::fmt(&self.0.compact_log, "compact_log", &mut s);
        PbPrint::fmt(&self.0.transfer_leader, "transfer_leader", &mut s);
        PbPrint::fmt(&self.0.verify_hash, "verify_hash", &mut s);
        let prepare_merge = ProtobufValue(self.0.get_prepare_merge());
        PbPrint::fmt(&prepare_merge, "prepare_merge", &mut s);
        let commit_merge = ProtobufValue(self.0.get_commit_merge());
        PbPrint::fmt(&commit_merge, "commit_merge", &mut s);
        PbPrint::fmt(&self.0.rollback_merge, "rollback_merge", &mut s);
        let splits = ProtobufValue(self.0.get_splits());
        PbPrint::fmt(&splits, "splits", &mut s);
        write!(f, "{}", s)
    }
}

impl<'a> fmt::Debug for ProtobufValue<'a, RaftCmdRequest> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        PbPrint::fmt(&self.0.header, "header", &mut s);
        let requests = self
            .0
            .get_requests()
            .iter()
            .map(ProtobufValue)
            .collect::<Vec<_>>();
        PbPrint::fmt(&requests, "requests", &mut s);
        let admin_request = ProtobufValue(self.0.get_admin_request());
        PbPrint::fmt(&admin_request, "admin_request", &mut s);
        PbPrint::fmt(&self.0.status_request, "status_request", &mut s);
        write!(f, "{}", s)
    }
}
