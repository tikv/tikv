use std::time::Duration;

use tikv::raftserver::store::*;
use tikv::proto::raft_cmdpb::*;
use super::cluster::Cluster;

use super::util::*;

#[test]
fn test_single_store() {
    // init_env_log();

    let mut cluster = Cluster::new(0, 1);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    // wait to let raft run.
    sleep_ms(500);

    let put = new_request(1, vec![new_put_cmd(&keys::data_key(b"a1"), b"v1")]);
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Put);

    let get = new_request(1, vec![new_get_cmd(&keys::data_key(b"a1"))]);
    let resp = cluster.call_command_on_leader(get, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Get);

    // TODO: add more tests
}
