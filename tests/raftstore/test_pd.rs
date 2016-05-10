// Copyright 2016 PingCAP, Inc.
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

use tikv::pd::PdClient;
use tikv::util::HandyRwLock;
use super::server::new_server_cluster;
use super::util;

#[test]
fn test_pd_heartbeat() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.cfg.store_cfg.pd_heartbeat_tick_interval = 50;

    cluster.bootstrap_region().unwrap();
    cluster.start();

    util::sleep_ms(100);

    let pd_client = cluster.pd_client.clone();
    let store = pd_client.rl().get_store(0, 1).unwrap();
    assert!(store.get_address().len() > 0);

    // force update a wrong store meta.
    pd_client.wl().put_store(0, util::new_store(1, "".to_owned())).unwrap();

    util::sleep_ms(500);
    let store1 = pd_client.rl().get_store(0, 1).unwrap();
    assert_eq!(store1.get_address(), store.get_address());
}
