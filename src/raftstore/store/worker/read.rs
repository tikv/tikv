// Copyright 2018 TiKV Project Authors.
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::TrySendError;
use kvproto::errorpb;
use kvproto::metapb;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse};
use prometheus::local::LocalHistogram;
use time::Timespec;

use crate::raftstore::errors::RAFTSTORE_IS_BUSY;
use crate::raftstore::store::fsm::{RaftPollerBuilder, RaftRouter};
use crate::raftstore::store::util::{self, LeaseState, RemoteLease};
use crate::raftstore::store::{
    cmd_resp, Peer, ProposalRouter, RaftCommand, ReadExecutor, ReadResponse, RequestInspector,
    RequestPolicy,
};
use crate::raftstore::Result;
use crate::storage::engine::DB;
use crate::util::collections::HashMap;
use crate::util::time::duration_to_sec;
use crate::util::timer::Timer;
use crate::util::worker::{Runnable, RunnableWithTimer};

use super::metrics::*;

/// A read only delegate of `Peer`.
#[derive(Debug)]
pub struct ReadDelegate {
    region: metapb::Region,
    peer_id: u64,
    term: u64,
    applied_index_term: u64,
    leader_lease: Option<RemoteLease>,
    last_valid_ts: RefCell<Timespec>,

    tag: String,
}

impl ReadDelegate {
    fn from_peer(peer: &Peer) -> ReadDelegate {
        let region = peer.region().clone();
        let region_id = region.get_id();
        let peer_id = peer.peer.get_id();
        ReadDelegate {
            region,
            peer_id,
            term: peer.term(),
            applied_index_term: peer.get_store().applied_index_term(),
            leader_lease: None,
            last_valid_ts: RefCell::new(Timespec::new(0, 0)),
            tag: format!("[region {}] {}", region_id, peer_id),
        }
    }

    fn update(&mut self, progress: Progress) {
        match progress {
            Progress::Region(region) => {
                self.region = region;
            }
            Progress::Term(term) => {
                self.term = term;
            }
            Progress::AppliedIndexTerm(applied_index_term) => {
                self.applied_index_term = applied_index_term;
            }
            Progress::LeaderLease(leader_lease) => {
                self.leader_lease = Some(leader_lease);
            }
        }
    }

    // TODO: return ReadResponse once we remove batch snapshot.
    fn handle_read(
        &self,
        req: &RaftCmdRequest,
        executor: &mut ReadExecutor,
        metrics: &mut ReadMetrics,
    ) -> Option<ReadResponse> {
        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                let snapshot_time = executor.snapshot_time().unwrap();
                let mut last_valid_ts = self.last_valid_ts.borrow_mut();
                if *last_valid_ts == snapshot_time /* quick path for lease checking. */
                    || lease.inspect(Some(snapshot_time)) == LeaseState::Valid
                {
                    // Cache snapshot_time for remaining requests in the same batch.
                    *last_valid_ts = snapshot_time;
                    let mut resp = executor.execute(req, &self.region);
                    // Leader can read local if and only if it is in lease.
                    cmd_resp::bind_term(&mut resp.response, term);
                    return Some(resp);
                } else {
                    metrics.rejected_by_lease_expire += 1;
                    debug!("rejected by lease expire"; "tag" => &self.tag);
                }
            } else {
                metrics.rejected_by_term_mismatch += 1;
                debug!("rejected by term mismatch"; "tag" => &self.tag);
            }
        }

        None
    }
}

impl Display for ReadDelegate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadDelegate for region {}, \
             leader {} at term {}, applied_index_term {}, has lease {}",
            self.region.get_id(),
            self.peer_id,
            self.term,
            self.applied_index_term,
            self.leader_lease.is_some(),
        )
    }
}

#[derive(Debug)]
pub enum Progress {
    Region(metapb::Region),
    Term(u64),
    AppliedIndexTerm(u64),
    LeaderLease(RemoteLease),
}

impl Progress {
    pub fn region(region: metapb::Region) -> Progress {
        Progress::Region(region)
    }

    pub fn term(term: u64) -> Progress {
        Progress::Term(term)
    }

    pub fn applied_index_term(applied_index_term: u64) -> Progress {
        Progress::AppliedIndexTerm(applied_index_term)
    }

    pub fn leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(lease)
    }
}

pub enum Task {
    Register(ReadDelegate),
    Update((u64, Progress)),
    Read(RaftCommand),
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
    pub fn read(cmd: RaftCommand) -> Task {
        Task::Read(cmd)
    }

    /// Task accepts `RaftCmdRequest`s that contain Get/Snap requests.
    /// Returns `true`, it can be saftly sent to localreader,
    /// Returns `false`, it must not be sent to localreader.
    #[inline]
    pub fn acceptable(request: &RaftCmdRequest) -> bool {
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
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Register(ref delegate) => write!(f, "localreader Task::Register {:?}", delegate),
            Task::Read(ref cmd) => write!(f, "localreader Task::Read {:?}", cmd.request),
            Task::Update(ref progress) => write!(f, "localreader Task::Update {:?}", progress),
            Task::Destroy(region_id) => write!(f, "localreader Task::Destroy region {}", region_id),
        }
    }
}

pub struct LocalReader<C: ProposalRouter> {
    store_id: u64,
    kv_engine: Arc<DB>,
    metrics: RefCell<ReadMetrics>,
    // region id -> ReadDelegate
    delegates: HashMap<u64, ReadDelegate>,
    // A channel to raftstore.
    router: C,
    tag: String,
}

impl LocalReader<RaftRouter> {
    pub fn new<'a, T, P>(
        builder: &RaftPollerBuilder<T, P>,
        peers: impl Iterator<Item = &'a Peer>,
    ) -> Self {
        let mut delegates =
            HashMap::with_capacity_and_hasher(peers.size_hint().0, Default::default());
        for p in peers {
            let delegate = ReadDelegate::from_peer(p);
            info!(
                "create ReadDelegate";
                "tag" => &delegate.tag,
                "peer" => delegate.peer_id,
            );
            delegates.insert(p.region().get_id(), delegate);
        }
        let store_id = builder.store.get_id();
        LocalReader {
            delegates,
            store_id,
            kv_engine: builder.engines.kv.clone(),
            router: builder.router.clone(),
            metrics: Default::default(),
            tag: format!("[store {}]", store_id),
        }
    }

    pub fn new_timer() -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
        timer
    }
}

impl<C: ProposalRouter> LocalReader<C> {
    fn redirect(&self, mut cmd: RaftCommand) {
        debug!("localreader redirects command"; "tag" => &self.tag, "command" => ?cmd);
        let region_id = cmd.request.get_header().get_region_id();
        let mut err = errorpb::Error::new();
        match self.router.send(cmd) {
            Ok(()) => return,
            Err(TrySendError::Full(c)) => {
                self.metrics.borrow_mut().rejected_by_channel_full += 1;
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd = c;
            }
            Err(TrySendError::Disconnected(c)) => {
                self.metrics.borrow_mut().rejected_by_no_region += 1;
                err.set_message(format!("region {} is missing", region_id));
                err.mut_region_not_found().set_region_id(region_id);
                cmd = c;
            }
        }

        let mut resp = RaftCmdResponse::new();
        resp.mut_header().set_error(err);
        let read_resp = ReadResponse {
            response: resp,
            snapshot: None,
        };

        cmd.callback.invoke_read(read_resp);
    }

    fn pre_propose_raft_command<'a>(
        &'a self,
        req: &RaftCmdRequest,
    ) -> Result<Option<&'a ReadDelegate>> {
        // Check store id.
        if let Err(e) = util::check_store_id(req, self.store_id) {
            self.metrics.borrow_mut().rejected_by_store_id_mismatch += 1;
            debug!("rejected by store id not match"; "err" => %e);
            return Err(e);
        }

        // Check region id.
        let region_id = req.get_header().get_region_id();
        let delegate = match self.delegates.get(&region_id) {
            Some(delegate) => {
                fail_point!("localreader_on_find_delegate");
                delegate
            }
            None => {
                self.metrics.borrow_mut().rejected_by_no_region += 1;
                debug!("rejected by no region"; "region_id" => region_id);
                return Ok(None);
            }
        };
        // Check peer id.
        if let Err(e) = util::check_peer_id(req, delegate.peer_id) {
            self.metrics.borrow_mut().rejected_by_peer_id_mismatch += 1;
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, delegate.term) {
            debug!(
                "check term";
                "delegate_term" => delegate.term,
                "header_term" => req.get_header().get_term(),
            );
            self.metrics.borrow_mut().rejected_by_term_mismatch += 1;
            return Err(e);
        }

        // Check region epoch.
        if util::check_region_epoch(req, &delegate.region, false).is_err() {
            self.metrics.borrow_mut().rejected_by_epoch += 1;
            // Stale epoch, redirect it to raftstore to get the latest region.
            debug!("rejected by epoch not match"; "tag" => &delegate.tag);
            return Ok(None);
        }

        let mut inspector = Inspector {
            delegate,
            metrics: &mut *self.metrics.borrow_mut(),
        };
        match inspector.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(Some(delegate)),
            // It can not handle other policies.
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // It can only handle read command.
    fn propose_raft_command(&mut self, cmd: RaftCommand, executor: &mut ReadExecutor) {
        let region_id = cmd.request.get_header().get_region_id();
        match self.pre_propose_raft_command(&cmd.request) {
            Ok(Some(delegate)) => {
                let mut metrics = self.metrics.borrow_mut();
                if let Some(resp) = delegate.handle_read(&cmd.request, executor, &mut *metrics) {
                    cmd.callback.invoke_read(resp);
                    return;
                }
            }
            // It can not handle the rquest, forwards to raftstore.
            Ok(None) => {}
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self.delegates.get(&region_id) {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                cmd.callback.invoke_read(ReadResponse {
                    response,
                    snapshot: None,
                });
                return;
            }
        }

        self.redirect(cmd);
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
                "rejected by term check";
                "tag" => &self.delegate.tag,
                "applied_index_term" => self.delegate.applied_index_term,
                "delegate_term" => ?self.delegate.term,
            );
            self.metrics.rejected_by_appiled_term += 1;
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable raft's check_quorum.
        if self.delegate.leader_lease.is_some() {
            // We skip lease check, because it is postponed until `handle_read`.
            LeaseState::Valid
        } else {
            debug!("rejected by leader lease"; "tag" => &self.delegate.tag);
            self.metrics.rejected_by_no_lease += 1;
            LeaseState::Expired
        }
    }
}

impl<C: ProposalRouter> Runnable<Task> for LocalReader<C> {
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        self.metrics
            .borrow()
            .batch_requests_size
            .observe(tasks.len() as _);

        let mut sent = None;
        let mut executor = ReadExecutor::new(
            self.kv_engine.clone(),
            false, /* dont check region epoch */
            true,  /* we need snapshot time */
        );

        for task in tasks.drain(..) {
            match task {
                Task::Register(delegate) => {
                    info!("register ReadDelegate"; "tag" => &delegate.tag);
                    self.delegates.insert(delegate.region.get_id(), delegate);
                }
                Task::Read(cmd) => {
                    if sent.is_none() {
                        sent = Some(cmd.send_time);
                    }
                    self.propose_raft_command(cmd, &mut executor);
                }
                Task::Update((region_id, progress)) => {
                    if let Some(delegate) = self.delegates.get_mut(&region_id) {
                        delegate.update(progress);
                    } else {
                        warn!(
                            "update unregistered ReadDelegate";
                            "region_id" => region_id,
                            "progress" => ?progress,
                        );
                    }
                }
                Task::Destroy(region_id) => {
                    if let Some(delegate) = self.delegates.remove(&region_id) {
                        info!("destroy ReadDelegate"; "tag" => &delegate.tag);
                    }
                }
            }
        }

        if let Some(send_time) = sent {
            self.metrics
                .borrow_mut()
                .requests_wait_duration
                .observe(duration_to_sec(send_time.elapsed()));
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 15_000; // 15s

impl<C: ProposalRouter> RunnableWithTimer<Task, ()> for LocalReader<C> {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        self.metrics.borrow_mut().flush();
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
    }
}

struct ReadMetrics {
    requests_wait_duration: LocalHistogram,
    batch_requests_size: LocalHistogram,

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
}

impl Default for ReadMetrics {
    fn default() -> ReadMetrics {
        ReadMetrics {
            requests_wait_duration: LOCAL_READ_WAIT_DURATION.local(),
            batch_requests_size: LOCAL_READ_BATCH_REQUESTS.local(),
            rejected_by_store_id_mismatch: 0,
            rejected_by_peer_id_mismatch: 0,
            rejected_by_term_mismatch: 0,
            rejected_by_lease_expire: 0,
            rejected_by_no_region: 0,
            rejected_by_no_lease: 0,
            rejected_by_epoch: 0,
            rejected_by_appiled_term: 0,
            rejected_by_channel_full: 0,
        }
    }
}

impl ReadMetrics {
    fn flush(&mut self) {
        self.requests_wait_duration.flush();
        self.batch_requests_size.flush();
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
    use std::thread;

    use kvproto::raft_cmdpb::*;
    use tempdir::TempDir;
    use time::Duration;

    use crate::raftstore::store::util::Lease;
    use crate::raftstore::store::Callback;
    use crate::storage::ALL_CFS;
    use crate::util::rocksdb_util;
    use crate::util::time::monotonic_raw_now;

    use super::*;

    fn new_reader(
        path: &str,
        store_id: u64,
    ) -> (
        TempDir,
        LocalReader<SyncSender<RaftCommand>>,
        Receiver<RaftCommand>,
    ) {
        let path = TempDir::new(path).unwrap();
        let db =
            rocksdb_util::new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
        let (ch, rx) = sync_channel(1);
        let reader = LocalReader {
            store_id,
            router: ch,
            kv_engine: Arc::new(db),
            delegates: HashMap::default(),
            metrics: Default::default(),
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

    fn must_redirect(
        reader: &mut LocalReader<SyncSender<RaftCommand>>,
        rx: &Receiver<RaftCommand>,
        cmd: RaftCmdRequest,
    ) {
        let task = Task::read(RaftCommand::new(
            cmd.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        ));
        reader.run_batch(&mut vec![task]);
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd
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
        let mut lease = Lease::new(Duration::seconds(1)); // 1s is long enough.

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
        assert_eq!(reader.metrics.borrow().rejected_by_no_region, 1);

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        // But the applied_index_term is stale.
        let register_region1 = Task::Register(ReadDelegate {
            tag: String::new(),
            region: region1.clone(),
            peer_id: leader2.get_id(),
            term: term6,
            applied_index_term: term6 - 1,
            leader_lease: Some(remote),
            last_valid_ts: RefCell::new(Timespec::new(0, 0)),
        });
        reader.run_batch(&mut vec![register_region1]);
        assert!(reader.delegates.get(&1).is_some());
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // The applied_index_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.borrow().rejected_by_appiled_term, 1);

        // Make the applied_index_term matches current term.
        let pg = Progress::applied_index_term(term6);
        let update_region1 = Task::update(1, pg);
        reader.run_batch(&mut vec![update_region1]);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // Let's read.
        let region = region1.clone();
        let task = Task::read(RaftCommand::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region);
            })),
        ));
        reader.run_batch(&mut vec![task]);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

        // Wait for expiration.
        thread::sleep(Duration::seconds(1).to_std().unwrap());
        must_redirect(&mut reader, &rx, cmd.clone());

        // Renew lease.
        lease.renew(monotonic_raw_now());

        // Store id mismatch.
        let mut cmd_store_id = cmd.clone();
        cmd_store_id
            .mut_header()
            .mut_peer()
            .set_store_id(store_id + 1);
        let task = Task::read(RaftCommand::new(
            cmd_store_id,
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_store_not_match());
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run_batch(&mut vec![task]);
        assert_eq!(reader.metrics.borrow().rejected_by_store_id_mismatch, 1);

        // metapb::Peer id mismatch.
        let mut cmd_peer_id = cmd.clone();
        cmd_peer_id
            .mut_header()
            .mut_peer()
            .set_id(leader2.get_id() + 1);
        let task = Task::read(RaftCommand::new(
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
        reader.run_batch(&mut vec![task]);
        assert_eq!(reader.metrics.borrow().rejected_by_peer_id_mismatch, 1);

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        must_redirect(&mut reader, &rx, cmd_read_quorum);

        // Term mismatch.
        let mut cmd_term = cmd.clone();
        cmd_term.mut_header().set_term(term6 - 2);
        let task = Task::read(RaftCommand::new(
            cmd_term,
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_stale_command(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run_batch(&mut vec![task]);
        assert_eq!(reader.metrics.borrow().rejected_by_term_mismatch, 1);

        // Stale epoch.
        let mut epoch12 = epoch13.clone();
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_region_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(reader.metrics.borrow().rejected_by_epoch, 1);

        // Expire lease manually, and it can not be renewed.
        let previous_lease_rejection = reader.metrics.borrow().rejected_by_lease_expire;
        lease.expire();
        lease.renew(monotonic_raw_now());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            reader.metrics.borrow().rejected_by_lease_expire,
            previous_lease_rejection + 1
        );

        // Channel full.
        let task1 = Task::read(RaftCommand::new(cmd.clone(), Callback::None));
        let task_full = Task::read(RaftCommand::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_server_is_busy(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        ));
        reader.run_batch(&mut vec![task1]);
        reader.run_batch(&mut vec![task_full]);
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(reader.metrics.borrow().rejected_by_channel_full, 1);

        // Reject by term mismatch in lease.
        let previous_term_rejection = reader.metrics.borrow().rejected_by_term_mismatch;
        let mut cmd9 = cmd.clone();
        cmd9.mut_header().set_term(term6 + 3);
        let msg = RaftCommand::new(
            cmd9.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        let mut batch = vec![
            Task::update(1, Progress::term(term6 + 3)),
            Task::update(1, Progress::applied_index_term(term6 + 3)),
            Task::read(msg),
        ];
        reader.run_batch(&mut batch);
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd9
        );
        assert_eq!(
            reader.metrics.borrow().rejected_by_term_mismatch,
            previous_term_rejection + 1,
        );

        // Destroy region 1.
        let destroy_region1 = Task::destroy(1);
        reader.run_batch(&mut vec![destroy_region1]);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert!(reader.delegates.get(&1).is_none());
    }
}
