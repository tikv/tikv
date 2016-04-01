use std::sync::{Arc, RwLock, mpsc};
use std::thread;
use std::time::Duration;

use kvproto::raft_cmdpb::AdminCmdType;
use kvproto::pdpb::{self, CommandType};
use kvproto::raftpb::ConfChangeType;

use tikv::pd::PdClient;
use tikv::util::HandyRwLock;

use super::pd::TestPdClient;
use super::cluster::Simulator;
use super::util::*;

pub fn run_ask_loop<T>(pd_client: Arc<RwLock<TestPdClient>>,
                       sim: Arc<RwLock<T>>,
                       rx: mpsc::Receiver<pdpb::Request>)
    where T: Simulator + Send + Sync + 'static
{
    let mut h = AskHandler {
        pd_client: pd_client,
        sim: sim,
    };

    thread::spawn(move || {
        loop {
            let req = rx.recv();
            if req.is_err() {
                return;
            }

            let req = req.unwrap();
            match req.get_cmd_type() {
                CommandType::AskChangePeer => h.handle_change_peer(req),
                CommandType::AskSplit => h.handle_split(req),
                _ => {
                    error!("invalid request {:?}, skip it", req);
                }
            }
        }
    });
}

struct AskHandler<T: Simulator> {
    pd_client: Arc<RwLock<TestPdClient>>,
    sim: Arc<RwLock<T>>,
}

impl<T: Simulator> AskHandler<T> {
    fn handle_change_peer(&mut self, req: pdpb::Request) {
        let cluster_id = req.get_header().get_cluster_id();
        let region = req.get_ask_change_peer().get_region();
        let leader = req.get_ask_change_peer().get_leader();
        // because region may change at this point, we should use
        // latest region info instead. TODO: update leader too.
        let region = self.pd_client
                         .rl()
                         .get_region_by_id(cluster_id, region.get_id())
                         .unwrap();

        let meta = self.pd_client.rl().get_cluster_meta(cluster_id).unwrap();
        let max_peer_number = meta.get_max_peer_number() as usize;
        let peer_number = region.get_peers().len();
        if max_peer_number == peer_number {
            return;
        }

        let (conf_change_type, peer) = if max_peer_number < peer_number {
            // Find first follower.
            let pos = region.get_peers()
                            .iter()
                            .position(|x| x.get_id() != leader.get_id())
                            .unwrap();
            (ConfChangeType::RemoveNode, region.get_peers()[pos].clone())
        } else {
            // Choose first store which all peers are not in.
            let stores = self.pd_client.rl().get_stores(cluster_id).unwrap();
            let pos = stores.iter().position(|store| {
                let store_id = store.get_id();
                region.get_peers().iter().all(|x| x.get_store_id() != store_id)
            });

            if pos.is_none() {
                // find nothing
                return;
            }

            let store = &stores[pos.unwrap()];
            let peer_id = self.pd_client.wl().alloc_id().unwrap();
            let peer = new_peer(store.get_id(), peer_id);
            (ConfChangeType::AddNode, peer)
        };

        let mut change_peer = new_admin_request(region.get_id(),
                                                region.get_region_epoch(),
                                                new_change_peer_cmd(conf_change_type, peer));
        change_peer.mut_header().set_peer(leader.clone());
        let resp = self.sim.wl().call_command(change_peer, Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), format!("{:?}", resp));
        assert_eq!(resp.get_admin_response().get_cmd_type(),
                   AdminCmdType::ChangePeer);

        let region = resp.get_admin_response().get_change_peer().get_region();
        self.pd_client.wl().change_peer(cluster_id, region.clone()).unwrap();
    }

    fn handle_split(&mut self, req: pdpb::Request) {
        let cluster_id = req.get_header().get_cluster_id();
        let region = req.get_ask_split().get_region();
        let leader = req.get_ask_split().get_leader();
        let split_key = req.get_ask_split().get_split_key().to_vec();
        let region = self.pd_client
                         .rl()
                         .get_region_by_id(cluster_id, region.get_id())
                         .unwrap();
        if &*split_key <= region.get_start_key() ||
           (!region.get_end_key().is_empty() && &*split_key >= region.get_end_key()) {
            error!("invalid split key {:?} for region {:?}", split_key, region);
            return;
        }

        let new_region_id = self.pd_client.wl().alloc_id().unwrap();
        let mut peer_ids: Vec<u64> = vec![];
        for _ in 0..region.get_peers().len() {
            let peer_id = self.pd_client.wl().alloc_id().unwrap();
            peer_ids.push(peer_id);
        }

        let mut split = new_admin_request(region.get_id(),
                                          region.get_region_epoch(),
                                          new_split_region_cmd(Some(split_key),
                                                               new_region_id,
                                                               peer_ids));
        split.mut_header().set_peer(leader.clone());
        let resp = self.sim.wl().call_command(split, Duration::from_secs(3)).unwrap();

        assert!(!resp.get_header().has_error(), format!("{:?}", resp));
        assert_eq!(resp.get_admin_response().get_cmd_type(),
                   AdminCmdType::Split);

        let left = resp.get_admin_response().get_split().get_left();
        let right = resp.get_admin_response().get_split().get_right();

        self.pd_client.wl().split_region(cluster_id, left.clone(), right.clone()).unwrap();
    }
}
