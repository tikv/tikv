// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use fail;
use futures::Future;
use test_raftstore::*;
use tikv::pd::PdClient;

#[test]
fn test_destory_local_reader() {
    let _guard = ::setup();

    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    // Set election timeout and max leader lease to 1s.
    configure_for_lease_read(&mut cluster, Some(100), Some(10));

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // Now region 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let epoch = pd_client.get_region_epoch(r1);

    // Conf version must change.
    assert!(epoch.get_conf_ver() > 2);

    // Transfer leader to peer (2, 2).
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Remove peer (1, 1) from region 1.
    pd_client.must_remove_peer(r1, new_peer(1, 1));

    // Make sure region 1 is removed from store 1.
    cluster.must_region_not_exist(r1, 1);

    let region = pd_client.get_region_by_id(r1).wait().unwrap().unwrap();

    // Local reader panics if it finds a delegate.
    let reader_has_delegate = "localreader_on_find_delegate";
    fail::cfg(reader_has_delegate, "panic").unwrap();

    let resp = read_on_peer(
        &mut cluster,
        new_peer(1, 1),
        region,
        key,
        false,
        Duration::from_secs(5),
    );
    debug!("resp: {:?}", resp);
    assert!(resp.unwrap().get_header().has_error());

    fail::remove(reader_has_delegate);
}
