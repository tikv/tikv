// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use super::utils::*;

#[test]
fn test_mock_infra() {
    let (mut cluster, _) = new_mock_cluster(0, 3);
    let prev_state = collect_all_states(&cluster.cluster_ext, 1);
    assert_eq!(prev_state.len(), cluster.get_all_store_ids().len());
    assert_eq!(prev_state.len(), cluster.cluster_ext.get_cluster_size());
    cluster.shutdown();
}
