// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod command;
mod life;
mod pd;
mod query;
mod ready;

pub use command::{
    AdminCmdResult, CommittedEntries, ProposalControl, SimpleWriteDecoder, SimpleWriteEncoder,
};
pub use life::DestroyProgress;
pub use ready::{AsyncWriter, GenSnapTask, SnapState};

<<<<<<< HEAD
pub(crate) use self::{command::SplitInit, query::LocalReader};
=======
pub(crate) use self::{
    command::SplitInit,
    query::{LocalReader, ReadDelegatePair, SharedReadTablet},
    txn_ext::TxnContext,
};

#[cfg(test)]
pub mod test_util {
    use std::sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    };

    use engine_traits::{CfName, CF_DEFAULT};
    use kvproto::{kvrpcpb::ApiVersion, metapb::RegionEpoch, raft_cmdpb::RaftRequestHeader};
    use raft::prelude::{Entry, EntryType};
    use raftstore::store::simple_write::SimpleWriteEncoder;
    use sst_importer::SstImporter;
    use tempfile::TempDir;

    use super::{CatchUpLogs, SimpleWriteReqEncoder};
    use crate::{fsm::ApplyResReporter, router::ApplyRes};

    pub fn create_tmp_importer() -> (TempDir, Arc<SstImporter>) {
        let dir = TempDir::new().unwrap();
        let importer = Arc::new(
            SstImporter::new(&Default::default(), dir.path(), None, ApiVersion::V1, true).unwrap(),
        );
        (dir, importer)
    }

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
}
>>>>>>> 12c2cf1098 (raftstore: improve the bucket split strategy  (#15798))
