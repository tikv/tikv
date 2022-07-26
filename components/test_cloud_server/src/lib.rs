// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

extern crate core;

pub mod client;
pub mod cluster;
pub mod scheduler;

pub use cluster::*;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::ServerCluster;

    #[test]
    fn it_works() {
        test_util::init_log_for_test();
        let mut cluster = ServerCluster::new(vec![1, 2, 3], |_, _| {});
        let stores = cluster.get_stores();
        assert_eq!(stores.len(), 3);
        let mut client = cluster.new_client();
        client.put_kv(0..100, gen_key, gen_val);
        client.put_kv(100..200, gen_key, gen_val);
        client.put_kv(200..300, gen_key, gen_val);
        let split_keys = vec![gen_key(50), gen_key(150), gen_key(200)];
        for split_key in &split_keys {
            client.split(split_key);
        }
        cluster.wait_pd_region_count(4);
        cluster.get_pd_client().disable_default_operator();
        cluster.remove_node_peers(1);
        cluster.stop_node(1);
        std::thread::sleep(Duration::from_millis(100));
        cluster.start_node(1, |_, _| {});
        cluster.get_pd_client().enable_default_operator();
        cluster.wait_region_replicated(&[], 3);
        for split_key in &split_keys {
            cluster.wait_region_replicated(split_key, 3);
        }
        cluster.stop();
    }

    fn gen_key(i: usize) -> Vec<u8> {
        format!("key{:04}", i).into_bytes()
    }

    fn gen_val(i: usize) -> Vec<u8> {
        format!("val{:04}", i).repeat(i % 32).into_bytes()
    }
}
