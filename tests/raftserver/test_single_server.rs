use std::time::Duration;

use super::server::new_server_cluster;
use tikv::raftserver::store::{keys, Retriever};

use super::util::*;

// TODO: unify store/server test later.

#[test]
fn test_put() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    sleep_ms(300);

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

    let put = new_request(1, vec![new_put_cmd(b"a2", b"v2")]);
    let resp = cluster.call_command_on_leader(1, put, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "invalid key should be rejected.");
    assert!(cluster.engines[&1].get_value(b"a2").unwrap().is_none());
}

#[test]
fn test_delete() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    sleep_ms(300);

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

#[test]
fn test_seek() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    sleep_ms(300);

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
        assert_eq!(k, keys::data_key(b"key100"));
        assert_eq!(v, b"value100");
    }

    for i in 100..200 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        let (sk, sv) = cluster.seek(putk).unwrap();
        assert_eq!(sk, keys::data_key(putk));
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
