use std::sync::{Arc, RwLock};

use rocksdb::{DB, WriteBatch};

use proto::metapb;
use proto::raftpb;
use raft::{self, Ready, RawNode};
use raftserver::{Result, other};
use super::store::Store;
use super::peer_storage::{self, PeerStorage, RaftStorage, ApplySnapResult};
use super::util;

pub struct Peer {
    engine: Arc<DB>,
    store_id: u64,
    peer_id: u64,
    region_id: u64,
    pub raft_group: RawNode<RaftStorage>,
    pub storage: Arc<RwLock<PeerStorage>>,
}

impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create(store: &mut Store, region: metapb::Region) -> Result<Peer> {
        let store_id = store.get_store_id();
        let peer_id = match util::find_peer(&region, store_id) {
            None => return Err(other(format!("find no peer for store {}", store_id))),
            Some(peer) => peer.get_peer_id(),
        };

        Peer::new(store, region, peer_id)
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after appling snapshot.
    pub fn replicate(store: &mut Store, region_id: u64, peer_id: u64) -> Result<Peer> {
        let mut region = metapb::Region::new();
        region.set_region_id(region_id);
        Peer::new(store, region, peer_id)
    }

    fn new(store: &mut Store, region: metapb::Region, peer_id: u64) -> Result<Peer> {
        let store_id = store.get_store_id();

        let s = try!(PeerStorage::new(store.get_engine(), &region));

        let applied_index = s.applied_index();
        let storage = Arc::new(RwLock::new(s));

        let cfg = store.get_config();
        let raft_cfg = raft::Config {
            id: peer_id,
            peers: vec![],
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: false,
            storage: Arc::new(RaftStorage::new(storage.clone())),
        };

        let raft_group = try!(RawNode::new(&raft_cfg, &[]));

        let mut peer = Peer {
            engine: store.get_engine(),
            store_id: store_id,
            peer_id: peer_id,
            region_id: region.get_region_id(),
            storage: storage,
            raft_group: raft_group,
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    pub fn update_region(&mut self, region: &metapb::Region) -> Result<()> {
        if self.region_id != region.get_region_id() {
            return Err(other(format!("invalid region id {} != {}",
                                     region.get_region_id(),
                                     self.region_id)));
        }

        let mut store = self.storage.write().unwrap();
        store.set_region(region);
        Ok(())
    }

    pub fn get_region(&self) -> metapb::Region {
        let store = self.storage.read().unwrap();
        store.get_region().clone()
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
    }

    pub fn handle_raft_ready(&mut self) -> Result<()> {
        if !self.raft_group.has_ready() {
            debug!("raft group is not ready for {}", self.peer_id);
            return Ok(());
        }

        debug!("handle raft ready for peer {} at region {}",
               self.peer_id,
               self.region_id);

        let ready = self.raft_group.ready();

        try!(self.handle_raft_ready_in_storage(&ready));

        for msg in &ready.messages {
            try!(self.send_raft_message(&msg));
        }

        try!(self.handle_raft_commit_entries(&ready.committed_entries));

        self.raft_group.advance(ready);
        Ok(())
    }

    fn handle_raft_ready_in_storage(&mut self, ready: &Ready) -> Result<()> {
        let batch = WriteBatch::new();
        let mut storage = self.storage.write().unwrap();
        let mut last_index = storage.last_index();
        let mut apply_snap_res: Option<ApplySnapResult> = None;
        if !raft::is_empty_snap(&ready.snapshot) {
            apply_snap_res = try!(storage.apply_snapshot(&batch, &ready.snapshot).map(|res| {
                last_index = res.last_index;
                Some(res)
            }));
        }

        if ready.entries.len() > 0 {
            last_index = try!(storage.append(&batch, last_index, &ready.entries));
        }

        if let Some(ref hs) = ready.hs {
            try!(peer_storage::save_hard_state(&batch, self.region_id, hs));
        }

        try!(self.engine.write(batch));

        storage.set_last_index(last_index);
        // If we apply snapshot ok, we should update some infos like applied index too.
        if let Some(res) = apply_snap_res {
            storage.set_applied_index(res.applied_index);
            storage.set_region(&res.region);
        }

        Ok(())
    }

    fn send_raft_message(&mut self, msg: &raftpb::Message) -> Result<()> {
        // TODO: implement it later.
        Ok(())
    }

    fn handle_raft_commit_entries(&mut self, committed_entries: &[raftpb::Entry]) -> Result<()> {
        // TODO: implement it later.
        Ok(())
    }
}
