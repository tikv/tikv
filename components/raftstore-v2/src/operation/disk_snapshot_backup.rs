// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use futures::channel::mpsc::UnboundedSender;
use kvproto::brpb::CheckAdminResponse;
use raftstore::store::snapshot_backup::{SnapshotBrHandle, SnapshotBrWaitApplyRequest};
use tikv_util::box_err;

const REASON: &str = "Raftstore V2 doesn't support snapshot backup yet.";

#[derive(Clone, Copy)]
pub struct UnimplementedHandle;

impl SnapshotBrHandle for UnimplementedHandle {
    fn send_wait_apply(&self, _region: u64, _req: SnapshotBrWaitApplyRequest) -> crate::Result<()> {
        Err(crate::Error::Other(box_err!(
            "send_wait_apply not implemented; note: {}",
            REASON
        )))
    }

    fn broadcast_wait_apply(&self, _req: SnapshotBrWaitApplyRequest) -> crate::Result<()> {
        Err(crate::Error::Other(box_err!(
            "broadcast_wait_apply not implemented; note: {}",
            REASON
        )))
    }

    fn broadcast_check_pending_admin(
        &self,
        _tx: UnboundedSender<CheckAdminResponse>,
    ) -> crate::Result<()> {
        Err(crate::Error::Other(box_err!(
            "broadcast_check_pending_admin not implemented; note: {}",
            REASON
        )))
    }
}
