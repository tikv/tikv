use super::cluster::{Cluster, Simulator};
use tikv::raftserver::store::*;
use super::node::new_node_cluster;
use super::server::new_server_cluster;

fn test_put<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        cluster.put(putk, putv);
        let v = cluster.get(putk);
        assert_eq!(v, Some(putv.to_vec()));
    }
    // value should be overwrited.
    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i + 1));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        cluster.put(putk, putv);
        let v = cluster.get(putk);
        assert_eq!(v, Some(putv.to_vec()));
    }
}

fn test_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        cluster.put(putk, putv);
        let v = cluster.get(putk);
        assert_eq!(v, Some(putv.to_vec()));
    }

    for i in 1..1000 {
        let k = format!("key{}", i);
        let putk = k.as_bytes();
        cluster.delete(putk);
        assert!(cluster.get(putk).is_none());
    }
}

fn test_seek<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    for i in 100..200 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        cluster.put(putk, putv);
    }

    for i in 0..100 {
        let k = format!("key{:03}", i);
        let putk = k.as_bytes();
        let (k, v) = cluster.seek(putk).unwrap();
        assert_eq!(k, b"key100");
        assert_eq!(v, b"value100");
    }

    for i in 100..200 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        let (sk, sv) = cluster.seek(putk).unwrap();
        assert_eq!(sk, putk);
        assert_eq!(sv, putv);
    }

    for i in 200..300 {
        let k = format!("key{}", i);
        let putk = k.as_bytes();
        assert!(cluster.seek(putk).is_none());
    }

    assert!(cluster.seek(b"key2").is_none(),
            "seek should follow binary order");
}

#[test]
fn test_node_put() {
    let mut cluster = new_node_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_node_delete() {
    let mut cluster = new_node_cluster(0, 1);
    test_delete(&mut cluster);
}

#[test]
fn test_node_seek() {
    let mut cluster = new_node_cluster(0, 1);
    test_seek(&mut cluster);
}

#[test]
fn test_server_put() {
    let mut cluster = new_server_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_server_delete() {
    let mut cluster = new_server_cluster(0, 1);
    test_delete(&mut cluster);
}

#[test]
fn test_server_seek() {
    let mut cluster = new_server_cluster(0, 1);
    test_seek(&mut cluster);
}
