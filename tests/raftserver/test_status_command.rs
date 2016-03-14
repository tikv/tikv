use super::server::*;

#[test]
fn test_region_detail() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    cluster.bootstrap_region().expect("");
    cluster.start();

    let leader = cluster.leader_of_region(1).unwrap();
    let region_detail = cluster.region_detail(1, 1);
    assert!(region_detail.has_region());
    let region = region_detail.get_region();
    assert_eq!(region.get_region_id(), 1);
    assert!(region.has_start_key());
    assert!(region.get_start_key().is_empty());
    assert!(region.has_end_key());
    assert!(region.get_end_key().is_empty());
    assert_eq!(region.get_peers().len(), 5);
    assert!(region.get_max_peer_id() >= 5);

    assert!(region_detail.has_leader());
    assert_eq!(region_detail.get_leader(), &leader);
}
