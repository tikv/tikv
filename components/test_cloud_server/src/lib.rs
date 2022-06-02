// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub mod cluster;
pub use cluster::*;

#[cfg(test)]
mod tests {
    use crate::{put_mut, ServerCluster};

    #[test]
    fn it_works() {
        test_util::init_log_for_test();
        let mut cluster = ServerCluster::new(vec![1, 2, 3], |_, _| {});
        let stores = cluster.get_stores();
        assert_eq!(stores.len(), 3);
        let ctx = cluster.new_rpc_context("abc".as_bytes());
        let client = cluster.get_kv_client(ctx.get_peer().get_store_id());
        test_raftstore::must_kv_prewrite(
            &client,
            ctx.clone(),
            vec![put_mut("abc", "v")],
            "abc".into(),
            1,
        );
        test_raftstore::must_kv_commit(&client, ctx.clone(), vec!["abc".into()], 1, 2, 2);
        test_raftstore::must_kv_read_equal(&client, ctx, "abc".into(), "v".into(), 3);
        cluster.stop();
    }
}
