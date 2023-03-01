// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod bucket;
mod command;
mod life;
mod pd;
mod query;
mod ready;
mod txn_ext;

pub use command::{
    AdminCmdResult, ApplyFlowControl, CommittedEntries, CompactLogContext, MergeContext,
    ProposalControl, RequestHalfSplit, RequestSplit, SimpleWriteBinary, SimpleWriteEncoder,
    SimpleWriteReqDecoder, SimpleWriteReqEncoder, SplitFlowControl, SPLIT_PREFIX,
};
pub use life::{DestroyProgress, GcPeerContext};
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
}
