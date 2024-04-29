// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// FIXME: Now, for making sure there isn't a regression after the advanced
// prepare patch (anyway it is just a patch...), we won't reject the
// `CommitMerge` command, or the client may fall into an eternal wait over it
// while waiting pending admin command finish.
//
// Omitting rejecting the command won't break the consistency (at least won't
// make things worse), but will break the case: this case itself wants to prove
// that the `CommitMerge` won't be proposed.
#[test]
#[ignore = "See the comment of `test_merge`"]
fn test_merge() {
    use std::time::Duration;

    use test_backup::disk_snap::{assert_success, Suite};

    let mut suite = Suite::new(1);
    suite.split(b"k");
    let mut source = suite.cluster.get_region(b"a");
    let target = suite.cluster.get_region(b"z");
    assert_ne!(source.id, target.id);
    fail::cfg("on_schedule_merge", "pause").unwrap();
    let resp = suite.cluster.try_merge(source.id, target.id);
    assert_success(&resp);
    let mut call = suite.prepare_backup(1);
    call.prepare(60);
    fail::remove("on_schedule_merge");
    // Manually "apply" the prepare merge on region epoch.
    source.mut_region_epoch().set_conf_ver(2);
    source.mut_region_epoch().set_version(3);
    call.wait_apply([&source, &target].into_iter().cloned());
    let source = suite.cluster.get_region(b"a");
    let target = suite.cluster.get_region(b"z");
    assert_ne!(source.id, target.id);
    suite.nodes[&1].rejector.reset();
    test_util::eventually(Duration::from_secs(1), Duration::from_secs(10), || {
        let source = suite.cluster.get_region(b"a");
        let target = suite.cluster.get_region(b"z");
        source.id == target.id
    })
}
