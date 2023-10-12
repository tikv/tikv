// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod bucket;
mod command;
mod life;
mod misc;
mod pd;
mod query;
mod ready;
mod txn_ext;

pub use command::{
    AdminCmdResult, ApplyFlowControl, CatchUpLogs, CommittedEntries, CompactLogContext,
    MergeContext, ProposalControl, RequestHalfSplit, RequestSplit, SimpleWriteBinary,
    SimpleWriteEncoder, SimpleWriteReqDecoder, SimpleWriteReqEncoder, SplitFlowControl,
    MERGE_IN_PROGRESS_PREFIX, MERGE_SOURCE_PREFIX, SPLIT_PREFIX,
};
pub use life::{AbnormalPeerContext, DestroyProgress, GcPeerContext};
pub use ready::{
    write_initial_states, ApplyTrace, AsyncWriter, DataTrace, GenSnapTask, SnapState, StateStorage,
};

pub(crate) use self::{
    bucket::BucketStatsInfo,
    command::SplitInit,
    query::{LocalReader, ReadDelegatePair, SharedReadTablet},
    txn_ext::TxnContext,
};

#[cfg(test)]
pub mod test_util {
    use std::sync::Arc;

    use kvproto::kvrpcpb::ApiVersion;
    use sst_importer::SstImporter;
    use tempfile::TempDir;

    pub fn create_tmp_importer() -> (TempDir, Arc<SstImporter>) {
        let dir = TempDir::new().unwrap();
        let importer = Arc::new(
            SstImporter::new(&Default::default(), dir.path(), None, ApiVersion::V1).unwrap(),
        );
        (dir, importer)
    }
<<<<<<< HEAD
=======

    pub struct MockReporter {
        sender: Sender<ApplyRes>,
    }

    impl MockReporter {
        pub fn new() -> (Self, Receiver<ApplyRes>) {
            let (tx, rx) = channel();
            (MockReporter { sender: tx }, rx)
        }
    }

    impl ApplyResReporter for MockReporter {
        fn report(&self, apply_res: ApplyRes) {
            let _ = self.sender.send(apply_res);
        }

        fn redirect_catch_up_logs(&self, _c: CatchUpLogs) {}
    }

    pub fn new_put_entry(
        region_id: u64,
        region_epoch: RegionEpoch,
        k: &[u8],
        v: &[u8],
        term: u64,
        index: u64,
    ) -> Entry {
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, k, v);
        let mut header = Box::<RaftRequestHeader>::default();
        header.set_region_id(region_id);
        header.set_region_epoch(region_epoch);
        let req_encoder = SimpleWriteReqEncoder::new(header, encoder.encode(), 512);
        let (bin, _) = req_encoder.encode();
        let mut e = Entry::default();
        e.set_entry_type(EntryType::EntryNormal);
        e.set_term(term);
        e.set_index(index);
        e.set_data(bin.into());
        e
    }

    pub fn new_delete_range_entry(
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        index: u64,
        cf: CfName,
        start_key: &[u8],
        end_key: &[u8],
        notify_only: bool,
    ) -> Entry {
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.delete_range(cf, start_key, end_key, notify_only);
        let mut header = Box::<RaftRequestHeader>::default();
        header.set_region_id(region_id);
        header.set_region_epoch(region_epoch);
        let req_encoder = SimpleWriteReqEncoder::new(header, encoder.encode(), 512);
        let (bin, _) = req_encoder.encode();
        let mut e = Entry::default();
        e.set_entry_type(EntryType::EntryNormal);
        e.set_term(term);
        e.set_index(index);
        e.set_data(bin.into());
        e
    }
>>>>>>> 272fcd04f6 (raftstore-v2: avoid follower forwarding propose msg (#15704))
}
