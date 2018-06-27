// Copyright 2018 PingCAP, Inc.
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

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Instant;

use kvproto::errorpb;
use kvproto::metapb;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse};
use mio;
use prometheus::local::LocalHistogram;
use rocksdb::DB;

use raftstore::errors::RAFTSTORE_IS_BUSY;
use raftstore::store::msg::Callback;
use raftstore::store::store::Store;
use raftstore::store::util::{self, LeaseState, RemoteLease};
use raftstore::store::{
    cmd_resp, Msg as StoreMsg, Peer, ReadExecutor, ReadResponse, RequestInspector, RequestPolicy,
};
use raftstore::Result;
use util::collections::HashMap;
use util::transport::{NotifyError, Sender};
use util::worker::Runnable;

use super::super::metrics::BATCH_SNAPSHOT_COMMANDS;
use super::metrics::*;

/// A read only delegate of `Peer`.
#[derive(Debug)]
pub struct ReadDelegate {
    region: metapb::Region,
    peer: metapb::Peer,
    term: u64,
    applied_index_term: u64,
    leader_lease: Option<RemoteLease>,

    tag: String,
}

impl ReadDelegate {
    fn from_peer(peer: &Peer) -> ReadDelegate {
        let region = peer.region().clone();
        let region_id = region.get_id();
        let peer_id = peer.peer.get_id();
        ReadDelegate {
            region,
            peer: peer.peer.clone(),
            term: peer.term(),
            applied_index_term: peer.get_store().applied_index_term,
            leader_lease: None,
            tag: format!("[region {}] {}", region_id, peer_id),
        }
    }

    fn update(&mut self, progress: Progress) {
        if let Some(region) = progress.region {
            self.region = region;
        }
        if let Some(term) = progress.term {
            if self.term <= term {
                self.term = term;
            } else {
                warn!(
                    "stale progress, registered term {}, update term {}",
                    self.term, term
                );
            }
        }
        if let Some(applied_index_term) = progress.applied_index_term {
            if self.applied_index_term <= applied_index_term {
                self.applied_index_term = applied_index_term;
            } else {
                warn!(
                    "stale progress, registered applied_index_term {}, update applied_index_term {}",
                    self.applied_index_term, applied_index_term
                );
            }
        }
        if let Some(lease) = progress.leader_lease {
            self.leader_lease = Some(lease);
        }
    }

    fn handle_read(&self, req: &RaftCmdRequest, kv_engine: &Arc<DB>) -> Option<ReadResponse> {
        if let Ok(mut resp) =
            ReadExecutor::new(&self.region, kv_engine, &self.tag).execute(req, false)
        {
            if let Some(ref lease) = self.leader_lease {
                if lease.inspect(None) == LeaseState::Valid {
                    // Leader can read local if and only if it is in lease.
                    cmd_resp::bind_term(&mut resp.response, self.term);
                    return Some(resp);
                }
            }
        }
        None
    }
}

impl Display for ReadDelegate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "ReadDelegate for region {}, \
             leader {} at term {}, applied_index_term {}, has lease {}",
            self.region.get_id(),
            self.peer.get_id(),
            self.term,
            self.applied_index_term,
            self.leader_lease.is_some(),
        )
    }
}

#[derive(Debug)]
pub struct Progress {
    region: Option<metapb::Region>,
    term: Option<u64>,
    applied_index_term: Option<u64>,
    leader_lease: Option<RemoteLease>,
}

impl Progress {
    pub fn new() -> Self {
        Progress {
            region: None,
            term: None,
            applied_index_term: None,
            leader_lease: None,
        }
    }

    pub fn set_region(&mut self, region: metapb::Region) {
        self.region = Some(region);
    }

    pub fn set_term(&mut self, term: u64) {
        self.term = Some(term);
    }

    pub fn set_applied_index_term(&mut self, applied_index_term: u64) {
        self.applied_index_term = Some(applied_index_term);
    }

    pub fn set_leader_lease(&mut self, lease: RemoteLease) {
        self.leader_lease = Some(lease);
    }
}

pub enum Task {
    Register(ReadDelegate),
    Update((u64, Progress)),
    Read(StoreMsg),
    Destroy(u64),
}

impl Task {
    pub fn register(peer: &Peer) -> Task {
        let delegate = ReadDelegate::from_peer(peer);
        Task::Register(delegate)
    }

    pub fn update(region_id: u64, progress: Progress) -> Task {
        Task::Update((region_id, progress))
    }

    pub fn destroy(region_id: u64) -> Task {
        Task::Destroy(region_id)
    }

    #[inline]
    pub fn acceptable(msg: &StoreMsg) -> bool {
        match *msg {
            StoreMsg::RaftCmd { ref request, .. } => {
                if request.has_admin_request() || request.has_status_request() {
                    false
                } else {
                    for r in request.get_requests() {
                        match r.get_cmd_type() {
                            CmdType::Get | CmdType::Snap => (),
                            CmdType::Delete
                            | CmdType::Put
                            | CmdType::DeleteRange
                            | CmdType::Prewrite
                            | CmdType::IngestSST
                            | CmdType::Invalid => return false,
                        }
                    }
                    true
                }
            }
            StoreMsg::BatchRaftSnapCmds { .. } => true,
            _ => false,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Register(ref delegate) => write!(f, "localreader Task::Register {:?}", delegate),
            Task::Read(ref msg) => write!(f, "localreader Task::Msg {:?}", msg),
            Task::Update(ref progress) => write!(f, "localreader Task::Update {:?}", progress),
            Task::Destroy(region_id) => write!(f, "localreader Task::Destroy region {}", region_id),
        }
    }
}

fn server_busy(cmd: StoreMsg) {
    let mut err = errorpb::Error::new();
    err.set_message(RAFTSTORE_IS_BUSY.to_owned());
    let mut server_is_busy = errorpb::ServerIsBusy::new();
    server_is_busy.set_reason(RAFTSTORE_IS_BUSY.to_owned());
    err.set_server_is_busy(server_is_busy);
    let mut resp = RaftCmdResponse::new();
    resp.mut_header().set_error(err);

    let read_resp = ReadResponse {
        response: resp,
        snapshot: None,
    };

    match cmd {
        StoreMsg::RaftCmd { callback, .. } => callback.invoke_read(read_resp),
        StoreMsg::BatchRaftSnapCmds {
            batch, on_finished, ..
        } => on_finished.invoke_batch_read(vec![Some(read_resp.clone()); batch.len()]),
        other => panic!("unexpected cmd {:?}", other),
    }
}

pub struct LocalReader<C: Sender<StoreMsg>> {
    store_id: u64,
    kv_engine: Arc<DB>,
    metrics: ReadMetrics,
    // region id -> ReadDelegate
    delegates: HashMap<u64, ReadDelegate>,
    // A channel to raftstore.
    ch: C,
    tag: String,
}

impl LocalReader<mio::Sender<StoreMsg>> {
    pub fn new<T, P>(store: &Store<T, P>) -> Self {
        let mut delegates =
            HashMap::with_capacity_and_hasher(store.get_peers().len(), Default::default());
        for (&region_id, p) in store.get_peers() {
            let delegate = ReadDelegate::from_peer(p);
            debug!(
                "{} init ReadDelegate for peer {:?}",
                delegate.tag, delegate.peer
            );
            delegates.insert(region_id, delegate);
        }
        let store_id = store.store_id();
        LocalReader {
            delegates,
            store_id,
            kv_engine: store.kv_engine(),
            ch: store.get_sendch().into_inner(),
            metrics: ReadMetrics::default(),
            tag: format!("[store {}]", store_id),
        }
    }
}

impl<C: Sender<StoreMsg>> LocalReader<C> {
    fn redirect(&mut self, cmd: StoreMsg) {
        debug!("{} localreader redirect {:?}", self.tag, cmd);
        self.metrics.raft_cmd_rejected += 1;
        match self.ch.send(cmd) {
            Ok(()) => (),
            Err(NotifyError::Full(cmd)) => {
                self.metrics.rejected_by_channel_full += 1;
                server_busy(cmd)
            }
            Err(err) => {
                error!("localreader redirect failed: {:?}", err);
            }
        }
    }

    fn pre_propose_raft_command(&mut self, req: &RaftCmdRequest) -> Result<bool> {
        // Check store id.
        if let Err(e) = util::check_store_id(req, self.store_id) {
            self.metrics.rejected_by_store_id_mismatch += 1;
            return Err(e);
        }

        // Check region id.
        let region_id = req.get_header().get_region_id();
        let delegate = match self.delegates.get(&region_id) {
            Some(status) => status,
            None => {
                self.metrics.rejected_by_no_region += 1;
                return Ok(false);
            }
        };

        // Check peer id.
        if let Err(e) = util::check_peer_id(req, delegate.peer.get_id()) {
            self.metrics.rejected_by_peer_id_mismatch += 1;
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, delegate.term) {
            debug!(
                "delegate.term {}, header.term {}",
                delegate.term,
                req.get_header().get_term()
            );
            self.metrics.rejected_by_term_mismatch += 1;
            return Err(e);
        }

        // Check region epoch.
        if util::check_region_epoch(req, &delegate.region, false).is_err() {
            self.metrics.rejected_by_epoch += 1;
            // Stale epoch, redirect it to raftstore to get the latest region.
            return Ok(false);
        }

        let region_id = req.get_header().get_region_id();
        let delegate = &self.delegates[&region_id];
        let metrics = &mut self.metrics;
        let mut inspector = Inspector { delegate, metrics };
        match inspector.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(true),
            // It can not handle other policies.
            Ok(_) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // It can only handle read command.
    fn propose_raft_command(
        &mut self,
        req: RaftCmdRequest,
        callback: Callback,
        send_time: Instant,
    ) {
        let region_id = req.get_header().get_region_id();
        match self.pre_propose_raft_command(&req) {
            Ok(true) => (),
            // It can not handle the rquest, forwards to raftstore.
            Ok(false) => {
                self.redirect(StoreMsg::RaftCmd {
                    send_time,
                    request: req,
                    callback,
                });
                return;
            }
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self.delegates.get(&region_id) {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                callback.invoke_read(ReadResponse {
                    response,
                    snapshot: None,
                });
                return;
            }
        }

        if let Some(resp) = self.delegates[&region_id].handle_read(&req, &self.kv_engine) {
            self.metrics.raft_cmd_handled += 1;
            callback.invoke_read(resp)
        } else {
            self.metrics.rejected_by_lease_expire += 1;
            self.redirect(StoreMsg::RaftCmd {
                send_time,
                request: req,
                callback,
            })
        }
    }

    fn propose_batch_raft_snapshot_command(
        &mut self,
        batch: Vec<RaftCmdRequest>,
        on_finished: Callback,
    ) {
        let size = batch.len();
        let mut ret = Vec::with_capacity(size);
        for req in batch {
            let region_id = req.get_header().get_region_id();
            match self.pre_propose_raft_command(&req) {
                Ok(true) => {
                    let delegate = &self.delegates[&region_id];
                    let resp = delegate.handle_read(&req, &self.kv_engine);
                    ret.push(resp);
                }
                // It can not handle the rquest, instead of forwarding to raftstore,
                // it returns a `None` which means users need to retry the requsets
                // via `async_snapshot`.
                Ok(false) => {
                    ret.push(None);
                }
                Err(e) => {
                    let mut response = cmd_resp::new_error(e);
                    if let Some(delegate) = self.delegates.get(&region_id) {
                        cmd_resp::bind_term(&mut response, delegate.term);
                    }
                    ret.push(Some(ReadResponse {
                        response,
                        snapshot: None,
                    }));
                }
            }
        }

        self.metrics.batch_snapshot_size.observe(size as _);
        self.metrics.raft_cmd_handled += 1;
        on_finished.invoke_batch_read(ret);
    }
}

struct Inspector<'r, 'm> {
    delegate: &'r ReadDelegate,
    metrics: &'m mut ReadMetrics,
}

impl<'r, 'm> RequestInspector for Inspector<'r, 'm> {
    fn has_applied_to_current_term(&mut self) -> bool {
        if self.delegate.applied_index_term == self.delegate.term {
            true
        } else {
            debug!(
                "{} deny, applied_index_term {} != term {} ",
                self.delegate.tag, self.delegate.applied_index_term, self.delegate.term
            );
            self.metrics.rejected_by_appiled_term += 1;
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable raft's check_quorum.
        if let Some(ref lease) = self.delegate.leader_lease {
            // None means now.
            if LeaseState::Valid == lease.inspect(None) {
                return LeaseState::Valid;
            }
            self.metrics.rejected_by_lease_expire += 1;
            debug!(
                "{} leader lease is expired: {:?}",
                self.delegate.tag, self.delegate.leader_lease
            );
        } else {
            debug!("{} leader lease is None", self.delegate.tag,);
            self.metrics.rejected_by_no_lease += 1;
        }
        LeaseState::Expired
    }
}

impl<C: Sender<StoreMsg>> Runnable<Task> for LocalReader<C> {
    fn run(&mut self, task: Task) {
        fail_point!("local_reader_on_run");
        match task {
            Task::Register(delegate) => {
                debug!(
                    "{} register ReadDelegate for {:?}",
                    delegate.tag, delegate.peer
                );
                self.delegates.insert(delegate.region.get_id(), delegate);
            }
            Task::Read(StoreMsg::RaftCmd {
                send_time,
                request,
                callback,
            }) => self.propose_raft_command(request, callback, send_time),
            Task::Read(StoreMsg::BatchRaftSnapCmds {
                batch, on_finished, ..
            }) => self.propose_batch_raft_snapshot_command(batch, on_finished),
            Task::Read(other) => {
                unimplemented!("unsupported Msg {:?}", other);
            }
            Task::Update((region_id, progress)) => {
                if let Some(delegate) = self.delegates.get_mut(&region_id) {
                    delegate.update(progress);
                } else {
                    debug!(
                        "unregistered ReadDelegate, region_id: {}, {:?}",
                        region_id, progress
                    );
                }
            }
            Task::Destroy(region_id) => {
                self.delegates.remove(&region_id);
            }
        }
    }

    fn on_tick(&mut self) {
        self.metrics.flush();
        let count = self.delegates.values().fold(0i64, |mut acc, delegate| {
            if let Some(ref lease) = delegate.leader_lease {
                if lease.term() == delegate.term {
                    acc += 1;
                }
            }
            acc
        });
        LOCAL_READ_LEADER.set(count);
    }
}

struct ReadMetrics {
    raft_cmd_handled: i64,
    raft_cmd_rejected: i64,

    // TODO: record rejected_by_read_quorum.
    rejected_by_store_id_mismatch: i64,
    rejected_by_peer_id_mismatch: i64,
    rejected_by_term_mismatch: i64,
    rejected_by_lease_expire: i64,
    rejected_by_no_region: i64,
    rejected_by_no_lease: i64,
    rejected_by_epoch: i64,
    rejected_by_appiled_term: i64,
    rejected_by_channel_full: i64,

    batch_snapshot_size: LocalHistogram,
}

impl Default for ReadMetrics {
    fn default() -> ReadMetrics {
        ReadMetrics {
            raft_cmd_handled: 0,
            raft_cmd_rejected: 0,
            rejected_by_store_id_mismatch: 0,
            rejected_by_peer_id_mismatch: 0,
            rejected_by_term_mismatch: 0,
            rejected_by_lease_expire: 0,
            rejected_by_no_region: 0,
            rejected_by_no_lease: 0,
            rejected_by_epoch: 0,
            rejected_by_appiled_term: 0,
            rejected_by_channel_full: 0,
            batch_snapshot_size: BATCH_SNAPSHOT_COMMANDS.local(),
        }
    }
}

impl ReadMetrics {
    fn flush(&mut self) {
        if self.raft_cmd_handled > 0 {
            LOCAL_READ
                .with_label_values(&["handled"])
                .inc_by(self.raft_cmd_handled);
            self.raft_cmd_handled = 0;
        }
        if self.raft_cmd_rejected > 0 {
            LOCAL_READ
                .with_label_values(&["rejected"])
                .inc_by(self.raft_cmd_rejected);
            self.raft_cmd_rejected = 0;
        }
        if self.rejected_by_store_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["store_id_mismatch"])
                .inc_by(self.rejected_by_store_id_mismatch);
            self.rejected_by_store_id_mismatch = 0;
        }
        if self.rejected_by_peer_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["peer_id_mismatch"])
                .inc_by(self.rejected_by_peer_id_mismatch);
            self.rejected_by_peer_id_mismatch = 0;
        }
        if self.rejected_by_term_mismatch > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["term_mismatch"])
                .inc_by(self.rejected_by_term_mismatch);
            self.rejected_by_term_mismatch = 0;
        }
        if self.rejected_by_lease_expire > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["lease_expire"])
                .inc_by(self.rejected_by_lease_expire);
            self.rejected_by_lease_expire = 0;
        }
        if self.rejected_by_no_region > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["no_region"])
                .inc_by(self.rejected_by_no_region);
            self.rejected_by_no_region = 0;
        }
        if self.rejected_by_no_lease > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["no_lease"])
                .inc_by(self.rejected_by_no_lease);
            self.rejected_by_no_lease = 0;
        }
        if self.rejected_by_epoch > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["epoch"])
                .inc_by(self.rejected_by_epoch);
            self.rejected_by_epoch = 0;
        }
        if self.rejected_by_appiled_term > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["appiled_term"])
                .inc_by(self.rejected_by_appiled_term);
            self.rejected_by_appiled_term = 0;
        }
        if self.rejected_by_channel_full > 0 {
            LOCAL_READ_REJECT
                .with_label_values(&["channel_full"])
                .inc_by(self.rejected_by_channel_full);
            self.rejected_by_channel_full = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::*;

    use kvproto::raft_cmdpb::*;
    use tempdir::TempDir;
    use time::Duration;

    use raftstore::store::util::Lease;
    use raftstore::store::Callback;
    use storage::ALL_CFS;
    use util::rocksdb;
    use util::time::monotonic_raw_now;

    use super::*;

    fn new_reader(
        path: &str,
        store_id: u64,
    ) -> (
        TempDir,
        LocalReader<SyncSender<StoreMsg>>,
        Receiver<StoreMsg>,
    ) {
        let path = TempDir::new(path).unwrap();
        let db = rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS, None).unwrap();
        let (ch, rx) = sync_channel(1);
        let reader = LocalReader {
            store_id,
            ch,
            kv_engine: Arc::new(db),
            delegates: HashMap::default(),
            metrics: ReadMetrics::default(),
            tag: "foo".to_owned(),
        };
        (path, reader, rx)
    }

    fn new_peers(store_id: u64, pr_ids: Vec<u64>) -> Vec<metapb::Peer> {
        pr_ids
            .into_iter()
            .map(|id| {
                let mut pr = metapb::Peer::new();
                pr.set_store_id(store_id);
                pr.set_id(id);
                pr
            })
            .collect()
    }

    fn must_extract_cmds(msg: StoreMsg) -> Vec<RaftCmdRequest> {
        match msg {
            StoreMsg::RaftCmd { request, .. } => vec![request],
            StoreMsg::BatchRaftSnapCmds { batch, .. } => batch,
            other => panic!("unexpected msg: {:?}", other),
        }
    }

    fn must_redirect(
        reader: &mut LocalReader<SyncSender<StoreMsg>>,
        rx: &Receiver<StoreMsg>,
        cmd: RaftCmdRequest,
    ) {
        let task = Task::Read(StoreMsg::new_raft_cmd(
            cmd.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        ));
        reader.run(task);
        assert_eq!(
            must_extract_cmds(
                rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                    .unwrap()
            ),
            vec![cmd]
        );
    }

    #[test]
    fn test_read() {
        let store_id = 2;
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id);

        // region: 1,
        // peers: 2, 3, 4,
        // leader:2,
        // from "" to "",
        // epoch 1, 1,
        // term 6.
        let mut region1 = metapb::Region::new();
        region1.set_id(1);
        let prs = new_peers(store_id, vec![2, 3, 4]);
        region1.set_peers(prs.clone().into());
        let epoch13 = {
            let mut ep = metapb::RegionEpoch::new();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let leader2 = prs[0].clone();
        region1.set_region_epoch(epoch13.clone());
        let term6 = 6;
        let mut lease = Lease::new(Duration::seconds(10)); // 10s is long enough.

        let mut cmd = RaftCmdRequest::new();
        let mut header = RaftRequestHeader::new();
        header.set_region_id(1);
        header.set_peer(leader2.clone());
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // The region is not register yet.
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_no_region, 1);

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.remote(1).unwrap();
        // But the applied_index_term is stale.
        let register_region1 = Task::Register(ReadDelegate {
            tag: String::new(),
            region: region1.clone(),
            peer: leader2.clone(),
            term: term6,
            applied_index_term: term6 - 1,
            leader_lease: Some(remote),
        });
        reader.run(register_region1);
        assert!(reader.delegates.get(&1).is_some());
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // The applied_index_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_appiled_term, 1);

        // Make the applied_index_term matches current term.
        let mut pg = Progress::new();
        pg.set_applied_index_term(term6);
        let update_region1 = Task::update(1, pg);
        reader.run(update_region1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // Let's read.
        let region = region1.clone();
        let task = Task::Read(StoreMsg::new_raft_cmd(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region);
            })),
        ));
        reader.run(task);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // Batch snapshot.
        let region = region1.clone();
        let batch_task = Task::Read(StoreMsg::new_batch_raft_snapshot_cmd(
            vec![cmd.clone()],
            Box::new(move |mut resps: Vec<Option<ReadResponse>>| {
                assert_eq!(resps.len(), 1);
                let snap = resps.remove(0).unwrap().snapshot.unwrap();
                assert_eq!(snap.get_region(), &region);
            }),
        ));
        reader.run(batch_task);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // Store id mismatch.
        let mut cmd_store_id = cmd.clone();
        cmd_store_id
            .mut_header()
            .mut_peer()
            .set_store_id(store_id + 1);
        let task = Task::Read(StoreMsg::new_raft_cmd(
            cmd_store_id,
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_store_not_match());
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run(task);
        assert_eq!(reader.metrics.rejected_by_store_id_mismatch, 1);

        // metapb::Peer id mismatch.
        let mut cmd_peer_id = cmd.clone();
        cmd_peer_id
            .mut_header()
            .mut_peer()
            .set_id(leader2.get_id() + 1);
        let task = Task::Read(StoreMsg::new_raft_cmd(
            cmd_peer_id,
            Callback::Read(Box::new(move |resp: ReadResponse| {
                assert!(
                    resp.response.get_header().has_error(),
                    "{:?}",
                    resp.response
                );
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run(task);
        assert_eq!(reader.metrics.rejected_by_peer_id_mismatch, 1);

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        must_redirect(&mut reader, &rx, cmd_read_quorum);

        // Term mismatch.
        let mut cmd_term = cmd.clone();
        cmd_term.mut_header().set_term(term6 - 2);
        let task = Task::Read(StoreMsg::new_raft_cmd(
            cmd_term,
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_stale_command(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run(task);
        assert_eq!(reader.metrics.rejected_by_term_mismatch, 1);

        // Stale epoch.
        let mut epoch12 = epoch13.clone();
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_region_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(reader.metrics.rejected_by_epoch, 1);

        // Lease expired.
        lease.expire();
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_lease_expire, 1);

        // Channel full.
        let task1 = Task::Read(StoreMsg::new_raft_cmd(cmd.clone(), Callback::None));
        let task_full = Task::Read(StoreMsg::new_raft_cmd(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_server_is_busy(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run(task1);
        reader.run(task_full);
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(reader.metrics.rejected_by_channel_full, 1);

        // Destroy region 1.
        let destroy_region1 = Task::destroy(1);
        reader.run(destroy_region1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert!(reader.delegates.get(&1).is_none());
    }
}
