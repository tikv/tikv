use std::thread;
use env_logger;

use tikv::raftserver::store::*;
use tikv::proto::raft_cmdpb::*;

use super::util::*;

#[test]
fn test_single_store() {
    env_logger::init().expect("");

    let trans = StoreTransport::new();
    let path = TempDir::new("test_single_store").unwrap();
    let engine = new_engine(&path);

    // we use cluster id 0.
    bootstrap_cluster(engine.clone(), 0).unwrap();

    let mut store = new_store(engine, trans);

    let sender = store.get_sender();

    let t = thread::spawn(move || {
        store.run().unwrap();
    });

    // wait to let raft run. 
    sleep_ms(500);

    // TODO: send RaftCommandRequest, wait the response.
    let peer = new_peer(1, 1, 1);
    let put = new_request(1,
                          peer.clone(),
                          vec![new_put_cmd(&keys::data_key(b"a1"), b"v1")]);
    let resp = call_timeout(&sender, put, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Put);

    let get = new_request(1, peer.clone(), vec![new_get_cmd(&keys::data_key(b"a1"))]);
    let resp = call_timeout(&sender, get, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Get);

    // TODO: add more tests 

    sender.send_quit().unwrap();

    t.join().unwrap();
}
