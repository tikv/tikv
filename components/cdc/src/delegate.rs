// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use futures::sync::mpsc::*;
use kvproto::cdcpb::*;
use kvproto::metapb::{Region, RegionEpoch};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use tikv::raftstore::Error as RaftStoreError;
use tikv::storage::mvcc::{Lock, LockType, WriteRef, WriteType};
use tikv::storage::txn::TxnEntry;
use tikv_util::collections::HashMap;
use txn_types::{Key, TimeStamp};

use crate::Error;

static DOWNSTREAM_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Downstream.
#[derive(Clone, Copy, Debug)]
pub struct DownstreamID(usize);

impl DownstreamID {
    fn new() -> DownstreamID {
        DownstreamID(DOWNSTREAM_ID_ALLOC.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Clone)]
pub struct Downstream {
    // TODO: include cdc request.
    /// A unique identifier of the Downstream.
    pub id: DownstreamID,
    // The IP address of downstream.
    peer: String,
    region_epoch: RegionEpoch,
    sink: UnboundedSender<ChangeDataEvent>,
}

impl Downstream {
    /// Create a Downsteam.
    ///
    /// peer is the address of the downstream.
    /// sink sends data to the downstream.
    pub fn new(
        peer: String,
        region_epoch: RegionEpoch,
        sink: UnboundedSender<ChangeDataEvent>,
    ) -> Downstream {
        Downstream {
            id: DownstreamID::new(),
            peer,
            sink,
            region_epoch,
        }
    }

    fn sink(&self, change_data: ChangeDataEvent) {
        if self.sink.unbounded_send(change_data).is_err() {
            error!("send event failed"; "downstream" => %self.peer);
        }
    }
}

// TODO fill the logic, for now it's incomplete, see more https://github.com/overvenus/tikv/tree/cdc
pub struct Delegate {
    pub region_id: u64,
}

impl Delegate {
    pub fn new(region_id: u64) -> Delegate {
        Delegate { region_id }
    }
}
