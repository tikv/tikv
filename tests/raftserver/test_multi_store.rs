use std::time::Duration;

use tikv::raftserver::store::*;
use tikv::proto::raft_cmdpb::CommandType;

use super::util::*;
use super::cluster::Cluster;

#[test]
fn test_multi_store() {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    // Let raft run.
    sleep_ms(500);

    let put = new_request(1,
                          vec![new_put_cmd(&keys::data_key(b"a1"), b"v1")]);
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Put);

    let get = new_request(1, vec![new_get_cmd(&keys::data_key(b"a1"))]);
    let resp = cluster.call_command_on_leader(get, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Get);
}

#[test]
fn test_multi_store_snapshot() {
    
}
