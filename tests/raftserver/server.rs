use std::collections::HashMap;
use std::thread;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{ClusterSimulator, Cluster};
use tikv::raftserver::server::*;
use tikv::util::codec::{self, rpc};
use tikv::proto::raft_serverpb::{Message, MessageType};
use tikv::proto::raft_cmdpb::*;


pub struct ServerCluster {
    cluster_id: u64,
    senders: HashMap<u64, SendCh>,
    handles: HashMap<u64, thread::JoinHandle<()>>,
    addrs: HashMap<u64, SocketAddr>,

    msg_id: Mutex<u64>,
}

impl ServerCluster {
    pub fn new(cluster_id: u64) -> ServerCluster {
        ServerCluster {
            cluster_id: cluster_id,
            senders: HashMap::new(),
            handles: HashMap::new(),
            addrs: HashMap::new(),
            msg_id: Mutex::new(0),
        }
    }

    fn new_config(&self) -> Config {
        let store_cfg = StoreConfig {
            raft_base_tick_interval: 10,
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 20,
            raft_log_gc_tick_interval: 100,
            ..StoreConfig::default()
        };

        Config {
            cluster_id: self.cluster_id,
            addr: "127.0.0.1:0".to_owned(),
            store_cfg: store_cfg,
            ..Config::default()
        }
    }
}

impl ClusterSimulator for ServerCluster {
    fn run_store(&mut self, store_id: u64, engine: Arc<DB>) {
        assert!(!self.handles.contains_key(&store_id));
        assert!(!self.senders.contains_key(&store_id));

        let cfg = self.new_config();
        let mut event_loop = create_event_loop().unwrap();
        let mut server = Server::new(&mut event_loop, cfg, vec![engine]).unwrap();
        let addr = server.get_listen_addr().unwrap();

        let sender = server.get_sendch();
        let t = thread::spawn(move || {
            server.run(&mut event_loop).unwrap();
        });

        self.handles.insert(store_id, t);
        self.senders.insert(store_id, sender);
        self.addrs.insert(store_id, addr);
    }

    fn stop_store(&mut self, store_id: u64) {
        let h = self.handles.remove(&store_id).unwrap();
        let sender = self.senders.remove(&store_id).unwrap();
        self.addrs.remove(&store_id).unwrap();

        sender.kill().unwrap();
        h.join().unwrap();
    }

    fn get_store_ids(&self) -> Vec<u64> {
        self.senders.keys().cloned().collect()
    }

    fn call_command(&self,
                    request: RaftCommandRequest,
                    timeout: Duration)
                    -> Option<RaftCommandResponse> {
        let store_id = request.get_header().get_peer().get_store_id();
        let addr = self.addrs.get(&store_id).unwrap();
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
    Cluster::new(id, count, ServerCluster::new(id))
}
