use std::collections::{HashMap, HashSet};
use std::thread;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{Simulator, Cluster};
use tikv::raftserver::server::*;
use tikv::util::codec::{self, rpc};
use kvproto::raft_serverpb::{Message, MessageType};
use kvproto::raft_cmdpb::*;
use tikv::pd::PdClient;
use super::util;
use super::pd::TestPdClient;
use super::pd_ask::run_ask_loop;

pub struct ServerCluster {
    cluster_id: u64,
    senders: HashMap<u64, SendCh>,
    handles: HashMap<u64, thread::JoinHandle<()>>,
    addrs: HashMap<u64, SocketAddr>,

    msg_id: Mutex<u64>,
    pd_client: Arc<RwLock<TestPdClient>>,
}

impl ServerCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> ServerCluster {
        ServerCluster {
            cluster_id: cluster_id,
            senders: HashMap::new(),
            handles: HashMap::new(),
            addrs: HashMap::new(),
            msg_id: Mutex::new(0),
            pd_client: pd_client,
        }
    }
}

impl Simulator for ServerCluster {
    #[allow(useless_format)]
    fn run_node(&mut self, node_id: u64, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.handles.contains_key(&node_id));
        assert!(node_id == 0 || !self.senders.contains_key(&node_id));

        let cfg = util::new_server_config(self.cluster_id);
        let mut event_loop = create_event_loop().unwrap();
        let mut server = Server::new(&mut event_loop, cfg, vec![engine], self.pd_client.clone())
                             .unwrap();
        let addr = server.listening_addr().unwrap();

        assert!(node_id == 0 || node_id == server.get_node_id());
        let node_id = server.get_node_id();

        // We must get real listening address and re-update pd again.
        self.pd_client
            .write()
            .unwrap()
            .put_node(self.cluster_id, util::new_node(node_id, addr.to_string()))
            .unwrap();

        let sender = server.get_sendch();

        let t = thread::spawn(move || {
            server.run(&mut event_loop).unwrap();
        });

        self.handles.insert(node_id, t);
        self.senders.insert(node_id, sender);
        self.addrs.insert(node_id, addr);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let h = self.handles.remove(&node_id).unwrap();
        let sender = self.senders.remove(&node_id).unwrap();
        self.addrs.remove(&node_id).unwrap();

        sender.kill().unwrap();
        h.join().unwrap();
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.senders.keys().cloned().collect()
    }

    fn call_command(&self,
                    request: RaftCommandRequest,
                    timeout: Duration)
                    -> Option<RaftCommandResponse> {
        let node_id = request.get_header().get_peer().get_node_id();
        let addr = self.addrs.get(&node_id).unwrap();
        let mut conn = TcpStream::connect(addr).unwrap();

        conn.set_write_timeout(Some(timeout)).unwrap();

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Command);
        msg.set_cmd_req(request);

        let mut msg_id = self.msg_id.lock().unwrap();
        *msg_id += 1;
        let res = rpc::encode_msg(&mut conn, *msg_id, &msg);
        if let Err(codec::Error::Io(ref e)) = res {
            if e.kind() == ErrorKind::TimedOut {
                return None;
            }
        }
        res.unwrap();

        conn.set_read_timeout(Some(timeout)).unwrap();

        let mut resp_msg = Message::new();
        let res = rpc::decode_msg(&mut conn, &mut resp_msg);
        if let Err(codec::Error::Io(ref e)) = res {
            if e.kind() == ErrorKind::TimedOut {
                return None;
            }
        }

        let get_msg_id = res.unwrap();

        assert_eq!(resp_msg.get_msg_type(), MessageType::CommandResp);
        assert_eq!(*msg_id, get_msg_id);

        Some(resp_msg.take_cmd_resp())
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));
    let sim = Arc::new(RwLock::new(ServerCluster::new(id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);
    Cluster::new(id, count, sim, pd_client)
}
